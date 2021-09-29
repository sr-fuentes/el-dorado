use crate::candles::Candle;
use crate::configuration::Settings;
use crate::exchanges::{fetch_exchanges, ftx::RestClient};
use crate::historical::select_ftx_trades;
use crate::markets::*;
use chrono::{Duration, Utc};
use sqlx::PgPool;

// Clean up FTX and FTX.US trade tables to set trade id as Primary Key
// Re-validate candles by indexing on trades on time and backfilling
// First trade id and ts data

// Run this code after the following migrations have been run:
//

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
            println!("Getting trades for candle {:?}", candle);
            // Fetch trades for each candle
            let _trades = match candle.is_validated {
                true => select_ftx_trades(
                    pool,
                    &market,
                    exchange,
                    candle.datetime,
                    candle.datetime + Duration::seconds(900),
                    true,
                ),
                false => continue,
            };
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
