use crate::inquisidor::Inquisidor;
use crate::markets::{select_market_details_by_status_exchange, MarketStatus};
use crate::exchanges::ExchangeName;
use crate::trades::select_gdax_trades_by_table;

impl Inquisidor {
    pub async fn cleanup_gdax(self) {
        // Cleanup gdax candles. There was a defect where candles selected for a timeframe where not
        // sorted by id so the first and last trades and open and close trades are not 100%
        // accurate.

        // 1) Delete all gdax candle validations
        println!("Deleting gdax candle_validations.");
        let sql = r#"
            DELETE FROM candles_validations
            WHERE exchange_name = 'gdax'
            "#;
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .expect("Failed to delete gdax candle_validations.");
        println!("Gdax candle_validations deleted.");

        // 2) For each GDAX market: 
        let gdax_markets = select_market_details_by_status_exchange(
            &self.pool,
            &ExchangeName::Gdax,
            &MarketStatus::Backfill,
        )
        .await
        .expect("Failed to select gdax markets.");

        for market in gdax_markets.iter() {
            println!("Cleaning up {} candles.", market.market_name);
            // a) Delete 01d candles
            let sql = r#"
                DELETE FROM candles_01d
                WHERE market_id = $1
                "#;
            println!("Deleting 01d candles.");
            sqlx::query(&sql).bind(market.market_id).execute(&self.pool).await.expect("Failed to delete 01d candles.");
            println!("01d candles deleted.");
            // b) Migrate all validated trades to processed
            println!("Loading all validated trades.");
            let table = format!("trades_gdax_{}_validated", market.as_strip());
            let sql = select_gdax_trades_by_table(&self.pool, table).await.expect("Failed to select trades from validated_table.");

            // c) Delete all hb candles

            // d) Select all trades from processed and create candles for date range

            // e) Migrate all trades to processed

            // f) Validate / create 01d / Validate 01d


        }
        println!("GDAX cleanup complete.");
    }
}
