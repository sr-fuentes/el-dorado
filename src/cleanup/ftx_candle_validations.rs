use crate::candles::{qc_unvalidated_candle, Candle};
use crate::configuration::Settings;
use crate::exchanges::{select_exchanges, ftx::RestClient, ExchangeName};
use crate::markets::{select_market_ids_by_exchange, select_market_detail};
use sqlx::PgPool;

// Clean up FTX candles that are not validated. Initial setup used a limit of 100 trades for each
// trade api call. There are numerous instances where there were more than 100 trades in the
// microsecond resulting in missing trades from the candle cause it to not be validated. This script
// pulls each current unvalidated candle and will re-download the trades and re-calculate the candle.

pub async fn cleanup_02(pool: &PgPool, config: &Settings) {
    // Get exchanges from database
    let exchanges = select_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");
    // Match exchange to config
    let exchange = exchanges
        .iter()
        .find(|e| e.name.as_str() == config.application.exchange)
        .unwrap();
    // Get REST client for exchange
    let client = match exchange.name {
        ExchangeName::FtxUs => RestClient::new_us(),
        ExchangeName::Ftx => RestClient::new_intl(),
    };
    // Get all markets and ids for markets
    let market_ids = select_market_ids_by_exchange(pool, &exchange.name)
        .await
        .expect("Could not fetch markets.");
    // Get all hb candles that are not validated
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE not is_validated
        ORDER BY market_id, datetime
        "#,
        exchange.name.as_str()
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
        let market_detail = select_market_detail(pool, market)
            .await
            .expect("Could not fetch market detail.");
        println!(
            "Attempting to revalidate: {:?} - {:?}",
            &market.market_name, &candle.datetime
        );
        let is_success = qc_unvalidated_candle(
            &client,
            pool,
            &exchange.name.as_str(),
            &market_detail,
            candle,
        )
        .await;
        println!("Revalidation success? {:?}", is_success);
    }
}
