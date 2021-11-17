use crate::candles::{Candle, qc_unvalidated_candle};
use crate::configuration::Settings;
use crate::exchanges::{fetch_exchanges, ftx::RestClient};
use crate::markets::fetch_markets;
use sqlx::PgPool;

// Clean up FTX candles that are not validated. Initial setup used a limit of 100 trades for each
// trade api call. There are numerous instances where there were more than 100 trades in the
// microsecond resulting in missing trades from the candle cause it to not be validated. This script
// pulls each current unvalidated candle and will re-download the trades and re-calculate the candle.

pub async fn cleanup_02(pool: &PgPool, config: Settings) {
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
    // Get all hb candles that are not validated
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE not is_validated
        ORDER BY market_id, time
        "#,
        exchange.exchange_name
    );
    let candles = sqlx::query_as::<_, Candle>(&sql)
        .fetch_all(pool)
        .await
        .expect("Could not fetch invalid candles.");
    // For each invalid candle - try to revalidate
    for candle in candles.iter() {
        let market = market_ids
            .iter()
            .find(|m| m.market_id == candle.market_id)
            .unwrap();
        println!(
            "Attempting to revalidate: {:?} - {:?}",
            &market.market_name, &candle.datetime
        );
        let is_success = qc_unvalidated_candle(&client, pool, &exchange.exchange_name, &market, &candle)
            .await;
        println!("Revalidation success? {:?}", is_success);
    }
}
