#[cfg(test)]
mod test {
    use crate::candles::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::Trade};
    use crate::markets::fetch_markets;
    use chrono::{Duration, DurationRound};
    use sqlx::PgPool;

    #[tokio::test]
    async fn fetch_trades_and_write_to_csv() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");

        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.exchange_name == configuration.application.exchange)
            .unwrap();

        // Get REST client for exchange
        let client = match exchange.exchange_name.as_str() {
            "ftxus" => RestClient::new_us(),
            "ftx" => RestClient::new_intl(),
            _ => panic!("No client exists for {}.", exchange.exchange_name),
        };

        // Get input from config for market to archive
        let market_ids = fetch_markets(&pool, &exchange)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == configuration.application.market)
            .unwrap();

        // Gets 15t candles for market newer than last 01d candle
        let candles = match select_last_01d_candle(&pool, &market).await {
            Ok(c) => select_candles_gte_datetime(
                &pool,
                &exchange,
                &market,
                c.datetime + Duration::days(1),
            )
            .await
            .expect("Could not fetch candles."),
            Err(sqlx::Error::RowNotFound) => select_candles(&pool, &exchange, &market)
                .await
                .expect("Could not fetch candles."),
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };

        // If there are no candles, then return, nothing to archive
        if candles.len() == 0 {
            return;
        };

        // Filter candles for last full day
        let next_candle = candles.last().unwrap().datetime + Duration::seconds(900);
        let last_full_day = next_candle.duration_trunc(Duration::days(1)).unwrap();
        let filtered_candles: Vec<Candle> = candles
            .iter()
            .filter(|c| c.datetime < last_full_day)
            .cloned()
            .collect();

        // Resample to 01d candles
        let resampled_candles = resample_candles(&filtered_candles, Duration::days(1));

        // If there are no resampled candles, then return
        if resampled_candles.len() == 0 {
            return;
        };

        // Insert 01D candles
        insert_candles_01d(&pool, &market, &resampled_candles)
            .await
            .expect("Could not insert candles.");

        // Get exchange candles for validation
        let first_candle = resampled_candles.first().unwrap().datetime;
        let last_candle = resampled_candles.last().unwrap().datetime;
        let mut exchange_candles =
            get_ftx_candles(&client, &market, first_candle, last_candle, 86400).await;

        // Validate 01d candles - if all 15T candles are validated (trades archived)
        for candle in resampled_candles.iter() {
            // get 15t candles that make up candle
            let hb_candles: Vec<Candle> = filtered_candles
                .iter()
                .filter(|c| {
                    c.datetime.duration_trunc(Duration::days(1)).unwrap() == candle.datetime
                })
                .cloned()
                .collect();
            println!("Validating {:?} with {:?}", candle, hb_candles);
            // Check if all hb candles are valid
            let hb_is_validated = hb_candles.iter().all(|c| c.is_validated == true);
            // Check if volume matches value
            let vol_is_validated = validate_candle(&candle, &mut exchange_candles);
            // Update candle validation status
            if hb_is_validated && vol_is_validated {
                update_candle_validation(&pool, &exchange, &market, &candle, 86400)
                    .await
                    .expect("Could not update candle validation status.");
            }
        }

        // Get validated but not archived 01d candles
        let candles_to_archive = select_candles_valid_not_archived(&pool, &market)
            .await
            .expect("Could not fetch valid not archived candles.");

        // Archive trades

        // Update 01d candles to is_archived
    }
}
