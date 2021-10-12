use crate::candles::*;
use crate::configuration::*;
use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::RestError, ftx::Trade, Exchange};
use crate::markets::*;
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use uuid::Uuid;

pub async fn run(pool: &PgPool, config: Settings) {
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
    let client = match exchange.exchange_name.as_str() {
        "ftxus" => RestClient::new_us(),
        "ftx" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange.exchange_name),
    };

    // Get input from user for market to run
    let market_ids = fetch_markets(pool, &exchange)
        .await
        .expect("Could not fetch exchanges.");
    let market = market_ids
        .iter()
        .find(|m| m.market_name == config.application.market)
        .unwrap();

    // Validate / clean up current candles / trades for market
    validate_hb_candles(pool, &client, &exchange, &market).await;

    // Delete trades from _rest table for market
    delete_trades_by_market_table(pool, &market, &exchange, "rest")
        .await
        .expect("Could not clear _rest trades.");

    // Get last state of market, return status, start and finish
    let market_detail = select_market_detail(pool, &market)
        .await
        .expect("Failed to get market detail.");
    let start = match market_detail.last_validated_candle {
        Some(lvc) => lvc + Duration::seconds(900),
        None => get_ftx_start(&client, &market).await,
    };
    let end = Utc::now().duration_trunc(Duration::seconds(900)).unwrap(); // 9/15/2021 02:00

    // Backfill historical
    // Match exchange for backfill routine
    println!("Backfilling from {} to {}.", start, end);
    backfill_ftx(pool, &client, &exchange, market, start, end).await;
}

pub async fn backfill_ftx(
    pool: &PgPool,
    client: &RestClient,
    exchange: &Exchange,
    market: &MarketId,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) {
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
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = match client
                .get_trades(
                    market.market_name.as_str(),
                    Some(100),
                    Some(interval_start),
                    Some(interval_end_or_last_trade),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() {
                        println!("Request timed out. Waiting 30 seconds before retrying.");
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else {
                        panic!("Error: {:?}", e)
                    }
                }
                Err(e) => panic!("RestError: {:?}", e),
                Ok(result) => result,
            };
            let num_trades = new_trades.len();
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                // Unwrap can be used because it will only be called
                // if there is at least one element in new_trades vec
                // Set end of interval to last trade returned for pagination
                interval_end_or_last_trade = new_trades.first().unwrap().time;
                let first_trade = new_trades.last().unwrap().time;
                println!(
                    "{} trades returned. First: {}, Last: {}",
                    num_trades, interval_end_or_last_trade, first_trade
                );
                println!("New last trade ts: {}", interval_end_or_last_trade);
                // FTX trades API takes time in microseconds. This could lead to a
                // endless loop if there are more than 100 trades in that microsecond.
                // In that case move the end to last trade minus one microsecond.
                if interval_end_or_last_trade == first_trade {
                    interval_end_or_last_trade =
                        interval_end_or_last_trade - Duration::microseconds(1);
                    println!(
                        "More than 100 trades in microsecond. Resetting to: {}",
                        interval_end_or_last_trade
                    );
                }
                // Save trades to db
                insert_ftxus_trades(pool, market, exchange, new_trades, "rest")
                    .await
                    .expect("Failed to insert ftx trades.");
            };
            // If new trades returns less than 100 trades then there are no more trades
            // for that interval, create the candle and process the trades for that period
            // Do not create candle for last interval (when interval_end is greater than end time)
            // because the interval_end may not be complete (ie interval_end = 9:15 and current time
            // is 9:13, you dont want to create the candle because it is not closed yet).
            if num_trades < 100 {
                if interval_end < end {
                    // Move trades from _rest to _processed and create candle
                    // Select trades for market between start and end interval
                    let mut interval_trades = select_ftx_trades(
                        pool,
                        &market.market_id,
                        &exchange.exchange_name,
                        interval_start,
                        interval_end,
                        false,
                        false,
                    )
                    .await
                    .expect("Could not fetch trades from db.");
                    // Check if there are interval trades, if there are: create candle and archive
                    // trades. If there are not, get previous trade details from previous candle
                    // and build candle from last
                    let new_candle = match interval_trades.len() {
                        0 => {
                            // Get previous candle
                            let previous_candle =
                                select_previous_candle(pool, &exchange, &market, interval_start)
                                    .await
                                    .expect("No previous candle.");
                            // Create Candle
                            Candle::new_from_last(
                                interval_start,
                                previous_candle.close,
                                previous_candle.last_trade_ts,
                                &previous_candle.last_trade_id.to_string(),
                            )
                        }
                        _ => {
                            // Sort and dedup
                            interval_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                            interval_trades.dedup_by(|t1, t2| t1.id == t2.id);
                            // Create Candle
                            Candle::new_from_trades(interval_start, &interval_trades)
                        }
                    };
                    // Insert into candles
                    insert_candle(pool, market, exchange, new_candle)
                        .await
                        .expect("Could not insert new candle.");
                    // Delete trades for market between start and end interval
                    delete_ftx_trades(
                        pool,
                        &market.market_id,
                        &exchange.exchange_name,
                        interval_start,
                        interval_end,
                        false,
                        false,
                    )
                    .await
                    .expect("Could not delete trades from db.");
                    // Insert into processed trades
                    insert_ftxus_trades(pool, market, exchange, interval_trades, "processed")
                        .await
                        .expect("Could not insert processed trades.");
                };
                // Move to next interval start
                interval_start = interval_start + Duration::seconds(900); // TODO! set to market heartbeat
            };
        }
    }
}

pub async fn get_ftx_start(client: &RestClient, market: &MarketId) -> DateTime<Utc> {
    // Set end time to floor of today's date, start time to 90 days prior. Check each day if there
    // are trades and return the start date - 1 that first returns trades
    let end_time = Utc::now().duration_trunc(Duration::days(1)).unwrap();
    let mut start_time = end_time - Duration::days(90);
    while start_time < end_time {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let new_trades = client
            .get_trades(market.market_name.as_str(), Some(1), None, Some(start_time))
            .await
            .expect("Failed to get trades.");
        println!("New Trades: {:?}", new_trades);
        if new_trades.len() > 0 {
            return start_time;
        } else {
            start_time = start_time + Duration::days(1)
        };
    }
    start_time
}

pub async fn validate_hb_candles(
    pool: &PgPool,
    client: &RestClient,
    exchange: &Exchange,
    market: &MarketId,
) {
    let unvalidated_candles = select_unvalidated_candles(pool, &exchange, &market)
        .await
        .expect("Could not fetch unvalidated candles.");
    if unvalidated_candles.len() > 0 {
        let first_candle = unvalidated_candles.first().unwrap().datetime;
        let last_candle = unvalidated_candles.last().unwrap().datetime;
        println!(
            "Getting exchange candles from {:?} to {:?}",
            first_candle, last_candle
        );
        let mut exchange_candles =
            get_ftx_candles(&client, &market, first_candle, last_candle, 900).await;
        println!("Pulled {} candles from exchange.", exchange_candles.len());
        println!(
            "First returned candle is: {:?}",
            exchange_candles.first().unwrap()
        );
        for unvalidated_candle in unvalidated_candles {
            // validate candle - get candle from exchange, comp volume. if volume matches
            // consider it validated - if not - pull trades
            println!(
                "Validating {} candle {}.",
                market.market_name, unvalidated_candle.datetime
            );
            let is_valid = validate_candle(&unvalidated_candle, &mut exchange_candles);
            if is_valid {
                // Update market details and candle with validated data
                update_market_last_validated(pool, &market, &unvalidated_candle)
                    .await
                    .expect("Could not update market details.");
                update_candle_validation(pool, &exchange, &market, &unvalidated_candle, 900)
                    .await
                    .expect("Could not update candle validation status.");
                // If there are trades (volume > 0) then move from processed to validated
                if unvalidated_candle.volume > dec!(0) {
                    // Update validated trades and move from processed to validated
                    let validated_trades = select_ftx_trades(
                        pool,
                        &market.market_id,
                        &exchange.exchange_name,
                        unvalidated_candle.datetime,
                        unvalidated_candle.datetime + Duration::seconds(900),
                        true,
                        false,
                    )
                    .await
                    .expect("Could not fetch validated trades.");
                    // Get first and last trades to get id for delete query
                    let first_trade = validated_trades.first().unwrap().id;
                    let last_trade = validated_trades.last().unwrap().id;
                    insert_ftxus_trades(pool, &market, &exchange, validated_trades, "validated")
                        .await
                        .expect("Could not insert validated trades.");
                    delete_ftx_trades_by_id(
                        pool,
                        &market.market_id,
                        &exchange.exchange_name,
                        first_trade,
                        last_trade,
                        true,
                        false,
                    )
                    .await
                    .expect("Could not delete processed trades.");
                }
            } else {
                // Add to re-validation queue
                println!(
                    "Candle not validated: {} \t {}",
                    market.market_name, unvalidated_candle.datetime
                );
            };
        }
    }
}

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
                ON CONFLICT (trade_id) DO NOTHING
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
    market_id: &Uuid,
    exchange_name: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    is_processed: bool,
    is_validated: bool,
) -> Result<Vec<Trade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = if is_processed {
        format!(
            r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_processed
        WHERE market_id = $1 AND time >= $2 and time < $3
        "#,
            exchange_name
        )
    } else if is_validated {
        format!(
            r#"
          SELECT trade_id as id, price, size, side, liquidation, time
          FROM trades_{}_validated
          WHERE market_id = $1 AND time >= $2 AND time < $3
          "#,
            exchange_name
        )
    } else {
        format!(
            r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{table}_rest
        WHERE market_id = $1 AND time >= $2 and time < $3
        UNION
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{table}_ws
        WHERE market_id = $1 AND time >= $2 and time < $3
        "#,
            table = exchange_name
        )
    };
    let rows = sqlx::query_as::<_, Trade>(&sql)
        .bind(market_id)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn delete_ftx_trades(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    is_processed: bool,
    is_validated: bool,
) -> Result<(), sqlx::Error> {
    let mut tables = Vec::new();
    if is_processed {
        tables.push("processed");
    } else if is_validated {
        tables.push("validated");
    } else {
        tables.push("rest");
        tables.push("ws");
    };
    for table in tables {
        let sql = format!(
            r#"
                DELETE FROM trades_{}_{}
                WHERE market_id = $1 AND time >= $2 and time <$3
            "#,
            exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market_id)
            .bind(interval_start)
            .bind(interval_end)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn delete_ftx_trades_by_id(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    first_trade_id: i64,
    last_trade_id: i64,
    is_processed: bool,
    is_validated: bool,
) -> Result<(), sqlx::Error> {
    let mut tables = Vec::new();
    if is_processed {
        tables.push("processed");
    } else if is_validated {
        tables.push("validated");
    } else {
        tables.push("rest");
        tables.push("ws");
    };
    for table in tables {
        let sql = format!(
            r#"
                DELETE FROM trades_{}_{}
                WHERE market_id = $1 AND trade_id >= $2 and trade_id <= $3
            "#,
            exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market_id)
            .bind(first_trade_id)
            .bind(last_trade_id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn delete_trades_by_market_table(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            DELETE FROM trades_{}_{}
            WHERE market_id = $1
        "#,
        exchange.exchange_name, trade_table
    );
    sqlx::query(&sql)
        .bind(market.market_id)
        .execute(pool)
        .await?;
    Ok(())
}
