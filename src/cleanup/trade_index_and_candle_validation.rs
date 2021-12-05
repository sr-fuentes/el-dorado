use crate::candles::Candle;
use crate::configuration::Settings;
use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::Trade};
use crate::markets::*;
use crate::trades::select_ftx_trades_by_time;
use chrono::{Duration, Utc};
use sqlx::PgPool;

// Clean up FTX and FTX.US trade tables to set trade id as Primary Key and index on time
// Backfill first trade id and ts for all existing candles
// Make the first trade id and ts columns not null

// Run this code after the following migrations have been run:
// alter candles add first trade id and ts

// Run this migration after this code has run
// make candles first trade id and ts not null

pub async fn cleanup_01(pool: &PgPool, config: Settings) {
    // Get exchanges from database
    let exchanges = fetch_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");
    // Match exchange to exchanges in database
    let exchange = exchanges
        .iter()
        .find(|e| e.exchange_name == config.application.exchange)
        .unwrap();

    // Get REST client for exchange
    let _client = match exchange.exchange_name.as_str() {
        "ftxus" => RestClient::new_us(),
        "ftx" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange.exchange_name),
    };

    // Get markets that have candles
    let sql = format!(
        r#"
            SELECT DISTINCT c.market_id,
                m.market_name
            FROM candles_15t_{} c
            INNER JOIN markets m
            ON c.market_id = m.market_id
        "#,
        exchange.exchange_name
    );
    let markets_with_candles = sqlx::query_as::<_, MarketId>(&sql)
        .fetch_all(pool)
        .await
        .expect("Failed to fetch markets with candles.");
    println!(
        "Cleaning up the following markets: \n{:?}",
        markets_with_candles
    );

    // Set timer to see if it is faster with datetime index
    let timer_start = Utc::now();

    // For each market, pull candles and either process, validate update firsts
    // or update firsts
    for market in markets_with_candles {
        println!("Cleaning up market: {:?}", market);
        // Get candles for that market
        let sql = format!(
            r#"
            SELECT *, last_trade_ts as first_trade_ts,
                last_trade_id as first_trade_id 
            FROM candles_15t_{}
            WHERE market_id = $1
            ORDER BY datetime
            "#,
            exchange.exchange_name
        );
        let market_candles = sqlx::query_as::<_, Candle>(&sql)
            .bind(market.market_id)
            .fetch_all(pool)
            .await
            .expect("Could not fetch candles.");
        for candle in market_candles {
            println!("Getting trades for candle {:?}", candle.datetime);
            // Fetch trades for each candle
            let mut trades = match candle.is_validated {
                false => select_ftx_trades_by_time(
                    pool,
                    &exchange.exchange_name,
                    market.strip_name().as_str(),
                    "processed",
                    candle.datetime,
                    candle.datetime + Duration::seconds(900),
                )
                .await
                .expect("Could not select trades for candle."),
                true => {
                    let sql = format!(
                        r#"
                        SELECT trade_id as id, price, size, side, liquidation, time
                        FROM trades_{}_validated
                        WHERE market_id = $1 AND time >=$2 and time < $3
                        "#,
                        exchange.exchange_name
                    );
                    sqlx::query_as::<_, Trade>(&sql)
                        .bind(market.market_id)
                        .bind(candle.datetime)
                        .bind(candle.datetime + Duration::seconds(900))
                        .fetch_all(pool)
                        .await
                        .expect("Could not select trades for candle.")
                }
            };
            // If there are trades, set the first id and ts. If not, get previous candle
            // and forward fill last trade data from previous candle.
            if trades.len() > 0 {
                println!("Setting first trade from trades.");
                trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                let first_trade = trades.first().unwrap();
                // Update candle
                let sql = format!(
                    r#"
                    UPDATE candles_15t_{}
                    SET first_trade_ts = $1, first_trade_id = $2
                    WHERE market_id = $3
                    AND datetime = $4
                    "#,
                    exchange.exchange_name
                );
                sqlx::query(&sql)
                    .bind(first_trade.time)
                    .bind(first_trade.id.to_string())
                    .bind(market.market_id)
                    .bind(candle.datetime)
                    .execute(pool)
                    .await
                    .expect("Could not update candle.");
            } else {
                // No trades for this candle, set first trade id and ts = last
                println!("No Trade. Setting first trade from existing last.");
                println!("Existing candle: {:?}", candle);
                let sql = format!(
                    r#"
                    UPDATE candles_15t_{}
                    SET first_trade_ts = $1, first_trade_id = $2
                    WHERE market_id = $3
                    AND datetime = $4
                    "#,
                    exchange.exchange_name
                );
                sqlx::query(&sql)
                    .bind(candle.last_trade_ts)
                    .bind(candle.last_trade_id)
                    .bind(market.market_id)
                    .bind(candle.datetime)
                    .execute(pool)
                    .await
                    .expect("Could not update candle.");
            }
        }
    }

    // End timer
    let timer_end = Utc::now();
    let duration = timer_end - timer_start;
    println!(
        "Start {} - End {}, Total Time: {}",
        timer_start, timer_end, duration
    );
}
