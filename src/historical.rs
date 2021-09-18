use crate::candles::{insert_candle, Candle};
use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;
use crate::markets::{fetch_markets, MarketId};
use chrono::{DateTime, Duration, TimeZone, Utc};
use sqlx::PgPool;
use tokio::time::sleep;
use uuid::Uuid;

pub async fn run(pool: &PgPool) {
    // Get input from user for exchange to run
    let exchange = Exchange {
        exchange_id: Uuid::new_v4(),
        exchange_name: "ftxus".to_string(),
    };

    // Get input from user for market to run
    let market_ids = fetch_markets(pool, &exchange)
        .await
        .expect("Could not fetch exchanges.");
    let market = market_ids
        .iter()
        .find(|m| m.market_name == "SOL/USD")
        .unwrap();

    // Get last state of market, return status, start and finish
    let start = Utc.timestamp(1631664000, 0); // 9/15/2021 00:00
    let end = Utc.timestamp(1631671200, 0); // 9/15/2021 02:00

    // Clear out _rest table for processed

    // Backfill historical
    // Match exchange for backfill routine
    backfill_ftx(pool, &exchange, market, start, end).await;
}

pub async fn backfill_ftx(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) {
    // Get appropriate REST client
    let client = match exchange.exchange_name.as_str() {
        "ftxus" => RestClient::new_us(),
        "ftx" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange.exchange_name),
    };

    // From start to end fill forward trades in 15m buckets and create candle
    // Pagination returns trades in desc order from end timestamp in 100 trade batches
    // Use last trade out of 100 (earliest timestamp) to set as end time for next request for trades
    // Until trades returned is < 100 meaning there are no further trades for interval
    // Then advance interval start by interval length and set new end timestamp and begin
    // To reterive trades.
    let mut interval_start = start.clone();
    while interval_start < end {
        // Set end of bucket to end of interval
        let interval_end = interval_start + Duration::seconds(900);
        let mut interval_end_or_last_trade = interval_start + Duration::seconds(900);
        println!(
            "Filling trades for interval from {} to {}.",
            interval_start, interval_end
        );
        while interval_start < interval_end_or_last_trade {
            // Prevent 429 errors by only requesting 4 per second
            sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = client
                .get_trades(
                    market.market_name.as_str(),
                    Some(100),
                    Some(interval_start),
                    Some(interval_end_or_last_trade),
                )
                .await
                .expect("Failed to get trades.");
            let num_trades = new_trades.len();
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                // unwrap can be used because it will only be called
                // if there is at least one element in new_trades vec
                // Set end of interval to last trade returned for pagination
                interval_end_or_last_trade = new_trades.first().unwrap().time;
                let first_trade = new_trades.last().unwrap().time;
                println!(
                    "{} trades returned. First: {}, Last: {}",
                    num_trades, interval_end_or_last_trade, first_trade
                );
                println!("New last trade ts: {}", interval_end_or_last_trade);
                // save trades to db
                insert_ftxus_trades(pool, market, exchange, new_trades, "rest")
                    .await
                    .expect("Failed to insert ftx trades.");
            };
            // If new trades returns less than 100 trades then there are no more trades
            // for that interval, create the candle and process the trades for that period
            if num_trades < 100 {
                // Move trades from _rest to _processed and create candle
                // Select trades for market between start and end interval
                let mut interval_trades =
                    select_ftx_trades(pool, market, exchange, interval_start, interval_end)
                        .await
                        .expect("Could not fetch trades from db.");
                // Sort and dedup
                interval_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                interval_trades.dedup_by(|t1, t2| t1.id == t2.id);
                // Create Candle
                let new_candle = Candle::new_from_trades(interval_start, &interval_trades);
                // Insert into candles
                insert_candle(pool, market, exchange, new_candle)
                    .await
                    .expect("Could not insert new candle.");
                // Delete trades for market between start and end interval
                delete_ftx_trades(pool, market, exchange, interval_start, interval_end)
                    .await
                    .expect("Could not delete trades from db.");
                // Insert into processed trades
                insert_ftxus_trades(pool, market, exchange, interval_trades, "processed")
                    .await
                    .expect("Could not insert processed trades.");
                // Move to next interval start
                interval_start = interval_start + Duration::seconds(900); // TODO! set to market heartbeat
            };
        }
    }
}

// pub async fn insert_ftx_trades(
//     pool: &PgPool,
//     market: &MarketId,
//     trades: Vec<Trade>,
// ) -> Result<(), sqlx::Error> {
//     for trade in trades.iter() {
//         sqlx::query!(
//             r#"
//                 INSERT INTO ftx_trades (
//                     market_id, trade_id, price, size, side, liquidation, time)
//                 VALUES ($1, $2, $3, $4, $5, $6, $7)
//             "#,
//             market.market_id,
//             trade.id,
//             trade.price,
//             trade.size,
//             trade.side,
//             trade.liquidation,
//             trade.time
//         )
//         .execute(pool)
//         .await?;
//     }
//     Ok(())
// }

pub async fn insert_ftxus_trades(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    trades: Vec<Trade>,
    table: &str,
) -> Result<(), sqlx::Error> {
    for trade in trades.iter() {
        // Cannot user sqlx query! macro because table may not exist at
        // compile time and table name is dynamic to ftx and ftxus.
        let sql = format!(
            r#"
                INSERT INTO trades_{}_{} (
                    market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            exchange.exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market.market_id)
            .bind(trade.id)
            .bind(trade.price)
            .bind(trade.size)
            .bind(&trade.side)
            .bind(trade.liquidation)
            .bind(trade.time)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn select_ftx_trades(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<Trade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let rows = sqlx::query_as::<_, Trade>(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_ftxus_rest
        WHERE market_id = $1 AND time >= $2 and time < $3
        UNION
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_ftxus_ws
        WHERE market_id = $1 AND time >= $2 and time < $3
        "#,
    )
    .bind(market.market_id)
    .bind(interval_start)
    .bind(interval_end)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn delete_ftx_trades(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let tables = ["rest", "ws"];
    for table in tables {
        let sql = format!(
            r#"
                DELETE FROM trades_{}_{}
                WHERE market_id = $1 AND time >= $2 and time <$3
            "#,
            exchange.exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market.market_id)
            .bind(interval_start)
            .bind(interval_end)
            .execute(pool)
            .await?;
    }
    Ok(())
}
