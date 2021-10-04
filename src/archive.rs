#[cfg(test)]
mod test {
    use crate::configuration::get_configuration;
    use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::Trade};
    use crate::markets::fetch_markets;
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

        // Get 01d candles for market

        // Get 15t candles for market newer than last 01d candle

        // Resample to 01d candles

        // Insert 01D candles

        // Get validated but not archived candles

        // Archive trades

        // Update candles to is_archived
    }
}
