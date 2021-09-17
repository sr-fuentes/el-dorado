use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;
use crate::markets::MarketId;
use chrono::{DateTime, Duration, TimeZone, Utc};
use sqlx::PgPool;

pub async fn run(_pool: &PgPool) {
    // Get input from user for exchange to run
    let _exchange = "ftxus";

    // Get input from user for market to run
    let _market = "SOL/USD";

    // Get last state of market, return status, start and finish
    let (_start, _end) = (Utc.timestamp(1524887322, 0), Utc.timestamp(1524887322, 0));

    // Clear out _rest table for processed

    // Backfill historical
    // Match exchange for backfill routine
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

    // From start to end fill forward trades in 15m buckets
    // Each new day create 15 minutes candles
    let mut interval_start = start.clone();
    while interval_start < end {
        let mut interval_end = interval_start + Duration::seconds(900);
        let mut last_trade_ts = interval_end.clone();
        println!(
            "Filling trades for interval from {} to {}.",
            interval_start, interval_end
        );
        while interval_start < last_trade_ts {
            let mut new_trades = client
                .get_trades(
                    market.market_name.as_str(),
                    Some(100),
                    Some(interval_start),
                    Some(last_trade_ts),
                )
                .await
                .expect("Failed to get trades.");
            let num_trades = new_trades.len();
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                // unwrap can be used because it will only be called
                // if there is at least one element in new_trades vec
                last_trade_ts = new_trades.first().unwrap().time;
                println!("New last trade ts: {}", last_trade_ts);

                // save trades to db
                insert_ftxus_trades(pool, market, exchange, new_trades)
                    .await
                    .expect("Failed to insert ftx trades.");
            };
            // If new trades returns less than 100 trades then there are no more trades
            // for that interval, create the candle and process the trades for that period
            if num_trades < 100 {
                // Move trades from _rest to _processed and create candle

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
) -> Result<(), sqlx::Error> {
    for trade in trades.iter() {
        let sql = format!(
            r#"
                INSERT INTO trades_{}_rest (
                    market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            exchange.exchange_name,
        );
        println!("Query: {}", sql);
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
