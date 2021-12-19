use crate::candles::{select_candles_by_daterange, DailyCandle};
use crate::cleanup::{cleanup_02, cleanup_03};
use crate::configuration::Settings;
use crate::exchanges::select_exchanges;
use crate::markets::select_market_ids_by_exchange;
use crate::trades::delete_ftx_trades_by_time;
use chrono::Duration;
use sqlx::PgPool;

// Pull all 01d candles that are left after the previous cleanups. The 01d that are not validated
// have all 15t validated candles but were validated using a 1bps tolerance instead of the an exact
// match. This cleanup script will grab all current 01d candles that are not validated. Set all
// associated 15t candles to not validated and the trade count to -1. It will also delete all trades
// that are associated with the 15t candles. Finally it will call the
// previous cleanup scripts to clean up the 15t candles and 01d candle validations.

pub async fn cleanup_04(pool: &PgPool, config: &Settings) {
    // Get exchanges from database
    let exchanges = select_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");
    // Match exchange to config
    let exchange = exchanges
        .iter()
        .find(|e| e.name.as_str() == config.application.exchange)
        .unwrap();
    // Get all markets and ids for markets
    let market_ids = select_market_ids_by_exchange(pool, &exchange.name)
        .await
        .expect("Could not fetch markets.");
    // Get all 01d candles that are not validated
    println!("Getting 01d unvalidated candles.");
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE not is_validated
        ORDER BY market_id, datetime
        "#;
    let candles = sqlx::query_as::<_, DailyCandle>(sql)
        .fetch_all(pool)
        .await
        .expect("Could not fetch invalid daily candles.");
    // For each invalid candle - mark the hb candles for revalidation
    for candle in candles.iter() {
        let market = market_ids
            .iter()
            .find(|m| m.market_id == candle.market_id)
            .unwrap();
        println!(
            "Marking 15t candles for re-validation for {} {}",
            market.market_name, candle.datetime
        );
        // Get hb candles
        let hb_candles = select_candles_by_daterange(
            pool,
            &exchange.name.as_str(),
            &market.market_id,
            candle.datetime,
            candle.datetime + Duration::days(1),
        )
        .await
        .expect("Could not fetch hb candles.");
        // For each hb candle
        // - delete trades from each table
        // - set validation status = false and trade_count = -1
        for hb_candle in hb_candles.iter() {
            delete_ftx_trades_by_time(
                pool,
                &exchange.name.as_str(),
                market.strip_name().as_str(),
                "rest",
                hb_candle.datetime,
                hb_candle.datetime + Duration::seconds(900),
            )
            .await
            .expect("could not delete rest and ws trades.");
            delete_ftx_trades_by_time(
                pool,
                &exchange.name.as_str(),
                market.strip_name().as_str(),
                "processed",
                hb_candle.datetime,
                hb_candle.datetime + Duration::seconds(900),
            )
            .await
            .expect("could not delete processed trades.");
            delete_ftx_trades_by_time(
                pool,
                &exchange.name.as_str(),
                market.strip_name().as_str(),
                "validated",
                hb_candle.datetime,
                hb_candle.datetime + Duration::seconds(900),
            )
            .await
            .expect("could not delete validated trades.");
            let sql_update = format!(
                r#"
                UPDATE candles_15t_{}
                SET (is_validated, trade_count) = ($1, $2)
                WHERE market_id = $3
                AND datetime = $4
                "#,
                exchange.name.as_str()
            );
            sqlx::query(&sql_update)
                .bind(false)
                .bind(-1)
                .bind(hb_candle.market_id)
                .bind(hb_candle.datetime)
                .execute(pool)
                .await
                .expect("Could not update candle valdiated and count.");
        }
    }
    // Now that the 15t candles have been re-set. Call the revalidation script
    println!("Calling Cleanup 02 to re-validated marked 15t candles.");
    cleanup_02(pool, config).await;

    // Not re-validted the 01d candles with re-built 15T candles
    println!("Calling Cleanup 03 to re-validated 01d candles.");
    cleanup_03(pool, config).await;
}
