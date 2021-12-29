use crate::candles::{
    delete_candle_01d, insert_candles_01d, resample_candles, select_candles_by_daterange,
    validate_01d_candles, DailyCandle,
};
use crate::configuration::Settings;
use crate::exchanges::{ftx::RestClient, select_exchanges, ExchangeName};
use crate::markets::{select_market_detail, select_market_ids_by_exchange, MarketId};
use chrono::Duration;
use sqlx::PgPool;

// Clean up all FTX 01D candles that are not validated. Review each one and rebuild the candle
// from the 15m candles for each day that is not validated. Delete the original candle and insert
// the newly calculated one. Finally re-run the 01D candle validation. Follow up by running the
// archive funtionality after this script is run.

pub async fn cleanup_03(pool: &PgPool, config: &Settings) {
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
    // Get all 01d candles that are not validated
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE not is_validated
        ORDER BY market_id, datetime
        "#;
    let candles = sqlx::query_as::<_, DailyCandle>(sql)
        .fetch_all(pool)
        .await
        .expect("Could not fetch invalid daily candles.");
    // For each invalid candle - rebuild from 15m candles
    for candle in candles.iter() {
        let market = market_ids
            .iter()
            .find(|m| m.market_id == candle.market_id)
            .unwrap();
        // Get hb candles
        let hb_candles = select_candles_by_daterange(
            pool,
            exchange.name.as_str(),
            &market.market_id,
            candle.datetime,
            candle.datetime + Duration::days(1),
        )
        .await
        .expect("Could not fetch hb candles.");
        let resampled_candles = resample_candles(market.market_id, &hb_candles, Duration::days(1));
        // There should only be one resampled candle. If there is not, skip
        if resampled_candles.len() != 1 {
            println!(
                "Number of resampled candles not = 1: {:?} {:?}",
                resampled_candles.len(),
                resampled_candles
            );
            continue;
        };
        // Pop off the first candle
        let resampled_candle = resampled_candles.first().unwrap();
        // If the volume or number of trades is different that original, then delete original and
        // replace with the resampled version.
        if candle.trade_count != resampled_candle.trade_count {
            println!(
                "Updating {} {} candle. Trade count differs from new trade count {} vs {}.",
                market.market_name,
                candle.datetime,
                candle.trade_count,
                resampled_candle.trade_count
            );
            println!("Deleting old 01d candle.");
            delete_candle_01d(pool, &market.market_id, &candle.datetime)
                .await
                .expect("Could not delete candle.");
            println!("Inserting new 01d candle.");
            insert_candles_01d(pool, &market.market_id, &resampled_candles, false)
                .await
                .expect("Could not insert resampled candles.");
        } else {
            println!(
                "{} {} candle matches resampled version.",
                market.market_name, candle.datetime
            );
        };
    }
    // Now that daily candles have been resampled - revalidate them market by market
    println!("Re-validating resampled candles.");
    // Get markets that have candles
    let sql = r#"
        SELECT DISTINCT c.market_id, m.market_name
        FROM candles_01d c
        INNER JOIN markets m
        ON c.market_id = m.market_id
        "#;
    let markets_with_candles = sqlx::query_as::<_, MarketId>(sql)
        .fetch_all(pool)
        .await
        .expect("Failed to fetch markets with candles.");
    println!(
        "Cleaning up the following markets: \n {:?}",
        markets_with_candles
    );
    // For each market, revalidated 01d candles
    for market in markets_with_candles.iter() {
        println!("Validating {:?}", market);
        let market_detail = select_market_detail(pool, market)
            .await
            .expect("Could not fetch market detail.");
        validate_01d_candles(pool, &client, exchange.name.as_str(), &market_detail).await;
    }
}
