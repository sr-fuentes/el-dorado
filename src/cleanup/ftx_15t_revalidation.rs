use crate::candles::{select_candles_by_daterange, DailyCandle};
use crate::configuration::Settings;
use crate::exchanges::{fetch_exchanges, ftx::RestClient};
use crate::markets::fetch_markets;
use chrono::Duration;
use sqlx::PgPool;

// Pull all 01d candles that are left after the previous cleanups. The 01d that are not validated
// have all 15t validated candles but were validated using a 1bps tolerance instead of the an exact
// match. This cleanup script will grab all current 01d candles that are not validated. Set all
// associated 15t candles to not validated and the trade count to -1. It will also delete all trades
// that are associated with the 15t candles. Finally it will call the
// previous cleanup scripts to clean up the 15t candles and 01d candle validations.

pub async fn cleanup_04(pool: &PgPool, config: Settings) {
    // Get exchanges from database
    let exchanges = fetch_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");
    // Match exchange to config
    let exchange = exchanges
        .iter()
        .find(|e| e.exchange_name == config.application.exchange)
        .unwrap();
    // Get REST client for exchange
    let client = match exchange.exchange_name.as_str() {
        "ftxus" => RestClient::new_us(),
        "ftx" => RestClient::new_intl(),
        _ => panic!("No client exists for {}", exchange.exchange_name),
    };
    // Get all markets and ids for markets
    let market_ids = fetch_markets(pool, &exchange)
        .await
        .expect("Could not fetch markets.");
    // Get all 01d candles that are not validated
    let sql = format!(
        r#"
        SELECT * FROM candles_01d
        WHERE not is_validated
        ORDER BY market_id, datetime
        "#
    );
    let candles = sqlx::query_as::<_, DailyCandle>(&sql)
        .fetch_all(pool)
        .await
        .expect("Could not fetch invalid daily candles.");
    // For each invalid candle - mark the hb candles for revalidation
    for candle in candles.iter() {
        let market = market_ids
            .iter()
            .find(|m| m.market_id == candle.market_id)
            .unwrap();
        // Get hb candles
        let hb_candles = select_candles_by_daterange(
            pool,
            &exchange.exchange_name,
            &market.market_id,
            candle.datetime,
            candle.datetime + Duration::days(1),
        )
        .await
        .expect("Could not fetch hb candles.");
        // For each hb candle
        // - delete trades from each table
        for hb_candle in hb_candles.iter() {}
        // - set validation status = false and trade_count = -1
    }
}
