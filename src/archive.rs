#[cfg(test)]
mod test {
    use crate::configuration::get_configuration;
    use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::Trade};
    use crate::markets::fetch_markets;
    use crate::candles::{select_last_01d_candle, select_candles};
    use chrono::Duration;
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
        let _client = match exchange.exchange_name.as_str() {
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

        // Get last 01d candle for market
        let last_daily_candle = select_last_01d_candle(&pool, &market).await.expect("Could not fetch last candle.");

        // Get 15t candles for market newer than last 01d candle
        let candles = select_candles(&pool, &exchange, &market).await.expect("Could not fetch candles.");

        // Resample to 01d candles
        let resampled_candles = candles.resample(Duration::days(1));

        // Insert 01D candles

        // Get validated but not archived 01d candles

        // Archive trades

        // Update 01d candles to is_archived
    }
}
