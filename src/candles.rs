use crate::configuration::*;
use crate::exchanges::{ftx::Candle as CandleFtx, ftx::RestClient, ftx::RestError, ftx::Trade};
use crate::markets::{update_market_last_validated, MarketId};
use crate::trades::{
    delete_ftx_trades_by_id, delete_ftx_trades_by_time, insert_ftx_trades,
    select_ftx_trades_by_table, select_ftx_trades_by_time,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
pub struct Candle {
    pub datetime: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub volume_net: Decimal,
    pub volume_liquidation: Decimal,
    pub value: Decimal,
    pub trade_count: i64,
    pub liquidation_count: i64,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub first_trade_ts: DateTime<Utc>,
    pub first_trade_id: String,
    pub is_validated: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
pub struct DailyCandle {
    pub is_archived: bool,
    pub datetime: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub volume_net: Decimal,
    pub volume_liquidation: Decimal,
    pub value: Decimal,
    pub trade_count: i64,
    pub liquidation_count: i64,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub first_trade_ts: DateTime<Utc>,
    pub first_trade_id: String,
    pub is_validated: bool,
    pub market_id: Uuid,
    pub is_complete: bool,
}

impl Candle {
    // Takes a Vec of Trade and aggregates into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from trades in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close / Datetime may
    // be incorrect.
    pub fn new_from_trades(datetime: DateTime<Utc>, trades: &Vec<Trade>) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No first trade for candle.").price, // open
                Decimal::MIN,                                              // high
                Decimal::MAX,                                              // low
                dec!(0),                                                   // close
                dec!(0),                                                   // volume
                dec!(0),                                                   // volume_net
                dec!(0),                                                   // volume_liquidation
                dec!(0),                                                   // value
                0,                                                         // count
                0,                                                         // liquidation_count,
                datetime,                                                  // last_trade_ts
                "".to_string(),                                            // last_trade_id
                trades.first().expect("No first trade.").time,             // first_trade_ts
                trades.first().expect("No first trade.").id.to_string(),   // first_trade_id
            ),
            |(o, h, l, _c, v, vn, vl, a, n, ln, _ts, _id, fts, fid), t| {
                (
                    o,
                    h.max(t.price),
                    l.min(t.price),
                    t.price,
                    v + t.size,
                    if t.side == "sell" {
                        vn + (t.size * dec!(-1))
                    } else {
                        vn + t.size
                    },
                    if t.liquidation { vl + t.size } else { vl },
                    a + (t.size * t.price),
                    n + 1,
                    if t.liquidation { ln + 1 } else { ln },
                    t.time,
                    t.id.to_string(),
                    fts,
                    fid,
                )
            },
        );
        Self {
            datetime,
            open: candle_tuple.0,
            high: candle_tuple.1,
            low: candle_tuple.2,
            close: candle_tuple.3,
            volume: candle_tuple.4,
            volume_net: candle_tuple.5,
            volume_liquidation: candle_tuple.6,
            value: candle_tuple.7,
            trade_count: candle_tuple.8,
            liquidation_count: candle_tuple.9,
            last_trade_ts: candle_tuple.10,
            last_trade_id: candle_tuple.11,
            first_trade_ts: candle_tuple.12,
            first_trade_id: candle_tuple.13,
            is_validated: false,
        }
    }

    // Takes a Vec of Candles and resamples into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from candes in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close may
    // be incorrect.
    pub fn new_from_candles(datetime: DateTime<Utc>, candles: &Vec<Candle>) -> Self {
        let candle_tuple = candles.iter().fold(
            (
                candles.first().expect("No first trade for candle.").open, // open
                Decimal::MIN,                                              // high
                Decimal::MAX,                                              // low
                dec!(0),                                                   // close
                dec!(0),                                                   // volume
                dec!(0),                                                   // volume_net
                dec!(0),                                                   // volume_liquidation
                dec!(0),                                                   // value
                0,                                                         // count
                0,                                                         // liquidation_count,
                datetime,                                                  // last_trade_ts
                "".to_string(),                                            // last_trade_id
                candles.first().expect("No first trade.").first_trade_ts,  // first_trade_ts
                candles
                    .first()
                    .expect("No first trade.")
                    .first_trade_id
                    .to_string(), // first_trade_id
            ),
            |(o, h, l, _c, v, vn, vl, a, n, ln, _ts, _id, fts, fid), c| {
                (
                    o,
                    h.max(c.high),
                    l.min(c.low),
                    c.close,
                    v + c.volume,
                    vn + c.volume_net,
                    vl + c.volume_liquidation,
                    a + c.value,
                    n + c.trade_count,
                    ln + c.liquidation_count,
                    c.last_trade_ts,
                    c.last_trade_id.to_string(),
                    fts,
                    fid,
                )
            },
        );
        Self {
            datetime,
            open: candle_tuple.0,
            high: candle_tuple.1,
            low: candle_tuple.2,
            close: candle_tuple.3,
            volume: candle_tuple.4,
            volume_net: candle_tuple.5,
            volume_liquidation: candle_tuple.6,
            value: candle_tuple.7,
            trade_count: candle_tuple.8,
            liquidation_count: candle_tuple.9,
            last_trade_ts: candle_tuple.10,
            last_trade_id: candle_tuple.11,
            first_trade_ts: candle_tuple.12,
            first_trade_id: candle_tuple.13,
            is_validated: false,
        }
    }

    // This function will build a placeholder trade with 0 volume and
    // will populate OHLC from the last trade provided.
    pub fn new_from_last(
        datetime: DateTime<Utc>,
        last_trade_price: Decimal,
        last_trade_ts: DateTime<Utc>,
        last_trade_id: &str,
    ) -> Self {
        Self {
            datetime,
            open: last_trade_price, // All OHLC are = last trade price
            high: last_trade_price,
            low: last_trade_price,
            close: last_trade_price,
            volume: dec!(0),
            volume_net: dec!(0),
            volume_liquidation: dec!(0),
            value: dec!(0),
            trade_count: 0,
            liquidation_count: 0,
            last_trade_ts,
            last_trade_id: last_trade_id.to_string(),
            first_trade_ts: last_trade_ts,
            first_trade_id: last_trade_id.to_string(),
            is_validated: false,
        }
    }
}

pub fn resample_candles(candles: &Vec<Candle>, duration: Duration) -> Vec<Candle> {
    match candles.len() {
        0 => Vec::<Candle>::new(),
        _ => {
            // Get first and last candles
            let first_candle = candles.first().expect("There is no first candle.");
            let last_candle = candles.last().expect("There is no last candle.");

            // Get floor of first and last candles
            let floor_start = first_candle.datetime.duration_trunc(duration).unwrap();
            let floor_end = last_candle.datetime.duration_trunc(duration).unwrap();

            // Create Daterange for resample period
            let mut dr_start = floor_start.clone();
            let mut date_range = Vec::new();
            while dr_start <= floor_end {
                date_range.push(dr_start);
                dr_start = dr_start + duration
            }

            // Create candle for each date in daterange
            let resampled_candles = date_range.iter().fold(Vec::new(), |mut v, d| {
                let filtered_candles: Vec<Candle> = candles
                    .iter()
                    .filter(|c| c.datetime.duration_trunc(duration).unwrap() == *d)
                    .cloned()
                    .collect();
                let resampled_candle = Candle::new_from_candles(*d, &filtered_candles);
                v.push(resampled_candle);
                v
            });
            resampled_candles
        }
    }
}

pub async fn create_01d_candles(pool: &PgPool, exchange_name: &str, market_id: &Uuid) {
    // Gets 15t candles for market newer than last 01d candle
    let candles = match select_last_01d_candle(pool, market_id).await {
        Ok(c) => select_candles_gte_datetime(
            pool,
            exchange_name,
            market_id,
            c.datetime + Duration::days(1),
        )
        .await
        .expect("Could not fetch candles."),
        Err(sqlx::Error::RowNotFound) => select_candles(pool, exchange_name, market_id, 900)
            .await
            .expect("Could not fetch candles."),
        Err(e) => panic!("Sqlx Error: {:?}", e),
    };

    // If there are no candles, then return, nothing to archive
    if candles.len() == 0 {
        return;
    };

    // Filter candles for last full day
    let next_candle = candles.last().unwrap().datetime + Duration::seconds(900);
    let last_full_day = next_candle.duration_trunc(Duration::days(1)).unwrap();
    let filtered_candles: Vec<Candle> = candles
        .iter()
        .filter(|c| c.datetime < last_full_day)
        .cloned()
        .collect();

    // Resample to 01d candles
    let resampled_candles = resample_candles(&filtered_candles, Duration::days(1));

    // If there are no resampled candles, then return
    if resampled_candles.len() == 0 {
        return;
    };

    // Insert 01D candles
    insert_candles_01d(pool, market_id, &resampled_candles)
        .await
        .expect("Could not insert candles.");
}

pub async fn validate_hb_candles(
    pool: &PgPool,
    client: &RestClient,
    exchange_name: &str,
    market: &MarketId,
    config: &Settings,
) {
    let unvalidated_candles =
        select_unvalidated_candles(pool, exchange_name, &market.market_id, 900)
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
            get_ftx_candles(&client, market, first_candle, last_candle, 900).await;
        println!("Pulled {} candles from exchange.", exchange_candles.len());
        println!(
            "First returned candle is: {:?}",
            exchange_candles.first().unwrap()
        );
        println!(
            "Last returned candle is: {:?}",
            exchange_candles.last().unwrap()
        );
        for unvalidated_candle in unvalidated_candles {
            // validate candle - get candle from exchange, comp volume. if volume matches
            // consider it validated - if not - pull trades
            println!(
                "Validating {} candle {}.",
                &market.market_name, unvalidated_candle.datetime
            );
            let is_valid = validate_candle(&unvalidated_candle, &mut exchange_candles);
            if is_valid {
                // Update market details and candle with validated data
                update_market_last_validated(
                    pool,
                    &market.market_id,
                    &unvalidated_candle,
                    config.application.ip_addr.as_str(),
                )
                .await
                .expect("Could not update market details.");
                update_candle_validation(
                    pool,
                    exchange_name,
                    &market.market_id,
                    &unvalidated_candle,
                    900,
                )
                .await
                .expect("Could not update candle validation status.");
                // If there are trades (volume > 0) then move from processed to validated
                if unvalidated_candle.volume > dec!(0) {
                    // Update validated trades and move from processed to validated
                    let validated_trades = select_ftx_trades_by_time(
                        pool,
                        &market.market_id,
                        exchange_name,
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
                    insert_ftx_trades(
                        pool,
                        &market.market_id,
                        exchange_name,
                        validated_trades,
                        "validated",
                    )
                    .await
                    .expect("Could not insert validated trades.");
                    delete_ftx_trades_by_id(
                        pool,
                        &market.market_id,
                        exchange_name,
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
                    &market.market_name, unvalidated_candle.datetime
                );
            };
        }
    }
}

pub async fn validate_01d_candles(
    pool: &PgPool,
    client: &RestClient,
    exchange_name: &str,
    market: &MarketId,
) {
    // Get unvalidated 01d candles
    let unvalidated_candles =
        match select_unvalidated_candles(pool, exchange_name, &market.market_id, 86400).await {
            Ok(c) => c,
            Err(sqlx::Error::RowNotFound) => return,
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };
    println!("Unvalidated 01D candles: {:?}", unvalidated_candles);
    // If no candles returned from query - return function
    if unvalidated_candles.len() == 0 {
        return;
    };
    // Get exchange candles for validation
    let first_candle = unvalidated_candles.first().unwrap().datetime;
    let last_candle = unvalidated_candles.last().unwrap().datetime;
    let mut exchange_candles =
        get_ftx_candles(&client, &market, first_candle, last_candle, 86400).await;
    println!("Pulled {} candles from exchange.", exchange_candles.len());

    // Get 15T candles to compare
    let hb_candles = select_candles_by_daterange(
        pool,
        exchange_name,
        &market.market_id,
        first_candle,
        last_candle,
    )
    .await
    .expect("Could not fetch hb candles.");

    // Validate 01d candles - if all 15T candles are validated and volume = ftx value
    for candle in unvalidated_candles.iter() {
        println!(
            "Validating {} candle {}.",
            &market.market_name, candle.datetime
        );
        // Get 15T candles that make up 01d candle
        let filtered_candles: Vec<Candle> = hb_candles
            .iter()
            .filter(|c| c.datetime.duration_trunc(Duration::days(1)).unwrap() == candle.datetime)
            .cloned()
            .collect();
        // Check if all hb candles are valid
        let hb_is_validated = filtered_candles.iter().all(|c| c.is_validated);
        // Check if volume matches value
        let vol_is_validated = validate_candle(&candle, &mut exchange_candles);
        // Updated candle validation status
        if hb_is_validated && vol_is_validated {
            update_candle_validation(pool, exchange_name, &market.market_id, &candle, 86400)
                .await
                .expect("Could not update candle validation status.");
        } else {
            println!(
                "{:?} not validated. HB={}, VOL={}",
                candle, hb_is_validated, vol_is_validated
            );
        }
    }
}

pub fn validate_candle(candle: &Candle, exchange_candles: &mut Vec<CandleFtx>) -> bool {
    // FTX candle validation on FTX Volume = ED Value, FTX sets open = last trade event if the
    // last trades was in the prior time period.
    // Consider valid if candle.value == exchange_candle.volume.
    let exchange_candle = exchange_candles.iter().find(|c| c.time == candle.datetime);
    match exchange_candle {
        Some(c) => {
            if c.volume == candle.value {
                return true;
            } else if (c.volume / candle.value - dec!(1.0)) < dec!(0.0001) {
                // If there are more than 100 trades in a microsecond they may not be counted in
                // historical data pooling. Consider validated is volume < 1 bp.
                return true;
            } else {
                println!(
                    "Failed to validate: {:?} in \n {:?}",
                    candle, exchange_candle
                );
                return false;
            }
        }
        None => {
            if candle.volume == dec!(0) {
                return true;
            } else {
                println!(
                    "Failed to validate: {:?}. Volume not 0 and no exchange candle.",
                    candle
                );
                return false;
            }
        }
    }
}

pub async fn qc_unvalidated_candle(
    client: &RestClient,
    pool: &PgPool,
    exchange_name: &str,
    market: &MarketId,
    candle: &Candle,
) -> bool {
    // Create temp trade table and re-download trades for candle time period then
    // compare to exchange candle, track any 100 trades in a micro instances
    let candle_start = candle.datetime;
    let candle_end = candle_start + Duration::seconds(900);
    // Get markets
    let mut exchange_candles =
        get_ftx_candles(client, market, candle_start, candle_start, 900).await;
    println!("Exchange candle: {:?}", exchange_candles);
    // Create temp trades table
    let table = format!("candle_validation_{}", exchange_name);
    let sql_drop = format!(
        r#"
        DROP TABLE IF EXISTS {}
        "#,
        table
    );
    sqlx::query(&sql_drop)
        .execute(pool)
        .await
        .expect("Could not drop temp validation table.");
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            market_id uuid NOT NULL,
            trade_id BIGINT NOT NULL,
            PRIMARY KEY (trade_id),
            price NUMERIC NOT NULL,
            size NUMERIC NOT NULL,
            side TEXT NOT NULL,
            liquidation BOOLEAN NOT NULL,
            time timestamptz NOT NULL
        )
        "#,
        table
    );
    println!("{}", sql);
    sqlx::query(&sql)
        .execute(pool)
        .await
        .expect("Could not create temp trade table.");
    // Get trades for candle period
    let mut counter = 0;
    let mut candle_end_or_last_trade = candle_end.clone();
    while candle_start < candle_end_or_last_trade {
        // Prevent 429 errors by only requesting 4 per second
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let mut new_trades = match client
            .get_trades(
                market.market_name.as_str(),
                Some(5000),
                Some(candle_start),
                Some(candle_end_or_last_trade),
            )
            .await
        {
            Err(RestError::Reqwest(e)) => {
                if e.is_timeout() {
                    println!("Request timed out. Waiting 30 seconds before retrying.");
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                } else if e.is_connect() {
                    println!(
                        "Connect error with reqwest. Waiting 30 seconds before retry. {:?}",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                } else if e.status() == Some(reqwest::StatusCode::BAD_GATEWAY) {
                    println!("502 Bad Gateway. Waiting 30 seconds before retry. {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                } else if e.status() == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) {
                    println!(
                        "503 Service Unavailable. Waiting 60 seconds before retry. {:?}",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    continue;
                } else {
                    panic!("Error (not timeout or connect): {:?}", e)
                }
            }
            Err(e) => panic!("Other RestError: {:?}", e),
            Ok(result) => result,
        };
        let num_trades = new_trades.len();
        if num_trades > 0 {
            new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
            candle_end_or_last_trade = new_trades.first().unwrap().time;
            let first_trade = new_trades.last().unwrap().time;
            println!(
                "{} trades returned. First: {}, Last: {}",
                num_trades, candle_end_or_last_trade, first_trade
            );
            if candle_end_or_last_trade == first_trade {
                candle_end_or_last_trade = candle_end_or_last_trade - Duration::microseconds(1);
                counter = counter + 1;
                println!(
                    "More than 100 trades in microsecond. Resetting to: {}",
                    candle_end_or_last_trade
                );
            };
            println!("Inserting trades in temp table.");
            // Temp table name can be used instead of exhcange name as logic for table name is
            // located in the insert function
            insert_ftx_trades(pool, &market.market_id, "temp", new_trades, &table)
                .await
                .expect("Failed to insert tmp ftx trades.");
        };
        if num_trades < 5000 {
            // Trades returned are less than 100, end trade getting and make candle
            let interval_trades = select_ftx_trades_by_table(pool, &table)
                .await
                .expect("Could not fetch trades from temp table.");
            // Create candle from interval trades
            if interval_trades.len() > 0 {
                let new_candle = Candle::new_from_trades(candle_start, &interval_trades);
                println!("New candle to re-validate: {:?}", new_candle);
                // Validate new candle
                let is_valid = validate_candle(&new_candle, &mut exchange_candles);
                if is_valid {
                    // Delete all trades from _rest _ws and _processed
                    delete_ftx_trades_by_time(
                        pool,
                        &market.market_id,
                        exchange_name,
                        new_candle.datetime,
                        new_candle.datetime + Duration::seconds(900),
                        false,
                        false,
                    )
                    .await
                    .expect("Could not delete rest and ws trades.");
                    delete_ftx_trades_by_time(
                        pool,
                        &market.market_id,
                        exchange_name,
                        new_candle.datetime,
                        new_candle.datetime + Duration::seconds(900),
                        true,
                        false,
                    )
                    .await
                    .expect("Could not delete processed trades.");
                    // Delete existing candle
                    delete_candle(pool, exchange_name, &market.market_id, &new_candle.datetime)
                        .await
                        .expect("Could not delete old candle.");
                    // Insert trades into _validated
                    insert_ftx_trades(
                        pool,
                        &market.market_id,
                        exchange_name,
                        interval_trades,
                        "validated",
                    )
                    .await
                    .expect("Could not insert validated trades.");
                    // Insert candle with validated status
                    insert_candle(pool, exchange_name, &market.market_id, new_candle, true)
                        .await
                        .expect("Could not insert validated candle.");
                    return true;
                };
            };
            break;
        };
    }
    // Return false if you get to this point. Valid candle would have been inserted and updated
    false
}

pub async fn get_ftx_candles(
    client: &RestClient,
    market: &MarketId,
    start: DateTime<Utc>,
    mut end_or_last: DateTime<Utc>,
    seconds: u32,
) -> Vec<CandleFtx> {
    // If end = start then FTX will not return any candles, add 1 second if the are equal
    end_or_last = match start == end_or_last {
        true => end_or_last + Duration::seconds(1),
        _ => end_or_last,
    };
    let mut candles: Vec<CandleFtx> = Vec::new();
    while start < end_or_last {
        // Prevent 429 errors by only requesting 4 per second
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let mut new_candles = client
            .get_candles(
                &market.market_name,
                Some(seconds),
                Some(start),
                Some(end_or_last),
            )
            .await
            .expect("Could not fetch exchange candles.");
        let num_candles = new_candles.len();
        if num_candles > 0 {
            candles.append(&mut new_candles);
        };
        // Sort candles to get next last
        candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
        end_or_last = candles.first().unwrap().time;
        if num_candles < 1501 {
            // Max pagination on candles is 1501
            break;
        }
    }
    // Dedup candles
    candles.dedup_by(|c1, c2| c1.time == c2.time);
    candles
}

pub async fn insert_candle(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    candle: Candle,
    is_validated: bool,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            INSERT INTO candles_15T_{} (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                market_id, first_trade_ts, first_trade_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
        exchange_name,
    );
    sqlx::query(&sql)
        .bind(candle.datetime)
        .bind(candle.open)
        .bind(candle.high)
        .bind(candle.low)
        .bind(candle.close)
        .bind(candle.volume)
        .bind(candle.volume_net)
        .bind(candle.volume_liquidation)
        .bind(candle.value)
        .bind(candle.trade_count)
        .bind(candle.liquidation_count)
        .bind(candle.last_trade_ts)
        .bind(candle.last_trade_id)
        .bind(is_validated)
        .bind(market_id)
        .bind(candle.first_trade_ts)
        .bind(candle.first_trade_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_candles_01d(
    pool: &PgPool,
    market_id: &Uuid,
    candles: &Vec<Candle>,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO candles_01d (
            datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
            trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
            market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
            $16, $17, $18, $19)
        "#;
    for candle in candles.iter() {
        sqlx::query(&sql)
            .bind(candle.datetime)
            .bind(candle.open)
            .bind(candle.high)
            .bind(candle.low)
            .bind(candle.close)
            .bind(candle.volume)
            .bind(candle.volume_net)
            .bind(candle.volume_liquidation)
            .bind(candle.value)
            .bind(candle.trade_count)
            .bind(candle.liquidation_count)
            .bind(candle.last_trade_ts)
            .bind(&candle.last_trade_id)
            .bind(candle.is_validated)
            .bind(market_id)
            .bind(candle.first_trade_ts)
            .bind(&candle.first_trade_id)
            .bind(false)
            .bind(false)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn delete_candle(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            DELETE FROM candles_15T_{}
            WHERE market_id = $1
            AND datetime = $2
        "#,
        exchange_name,
    );
    sqlx::query(&sql)
        .bind(market_id)
        .bind(datetime)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_unvalidated_candles(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    seconds: u32,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = match seconds {
        900 => format!(
            r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1 and not is_validated
            ORDER BY datetime
            "#,
            exchange_name
        ),
        86400 => format!(
            r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1 and not is_validated
            ORDER BY datetime
            "#
        ),
        _ => panic!("Candle resolution not supported."),
    };
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    seconds: u32,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = match seconds {
        900 => format!(
            r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1
            ORDER BY datetime
            "#,
            exchange_name
        ),
        86400 => format!(
            r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1
            ORDER BY datatime
            "#
        ),
        _ => panic!("Not a supported candle resolution."),
    };
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles_gte_datetime(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    datetime: DateTime<Utc>,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        AND datetime >= $2
        ORDER BY datetime
        "#,
        exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .bind(datetime)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles_by_daterange(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        AND datetime >= $2 AND datetime < $3
        ORDER BY datetime
        "#,
        exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_last_01d_candle(
    pool: &PgPool,
    market_id: &Uuid,
) -> Result<DailyCandle, sqlx::Error> {
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE market_id = $1
        ORDER BY datetime DESC
        "#;
    let row = sqlx::query_as::<_, DailyCandle>(&sql)
        .bind(market_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn select_last_candle(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
) -> Result<Candle, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        ORDER BY datetime DESC
        "#,
        exchange_name
    );
    let row = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn select_candles_valid_not_archived(
    pool: &PgPool,
    market_id: &Uuid,
) -> Result<Vec<DailyCandle>, sqlx::Error> {
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE market_id = $1
        AND is_validated
        AND NOT is_archived
        ORDER BY datetime DESC
        "#;
    let rows = sqlx::query_as::<_, DailyCandle>(&sql)
        .bind(market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_previous_candle(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    datetime: DateTime<Utc>,
) -> Result<Candle, sqlx::Error> {
    let sql = format!(
        r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1
            AND datetime < $2
            ORDER BY datetime DESC
        "#,
        exchange_name
    );
    let row = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .bind(datetime)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn update_candle_validation(
    pool: &PgPool,
    exchange_name: &str,
    market_id: &Uuid,
    candle: &Candle,
    seconds: u32,
) -> Result<(), sqlx::Error> {
    let sql = match seconds {
        900 => format!(
            r#"
            UPDATE candles_15t_{}
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#,
            exchange_name
        ),
        86400 => format!(
            r#"
            UPDATE candles_01d
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#
        ),
        _ => panic!("Unsupported candle resolution."),
    };
    sqlx::query(&sql)
        .bind(candle.datetime)
        .bind(market_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_candle_archived(
    pool: &PgPool,
    market_id: &Uuid,
    candle: &DailyCandle,
) -> Result<(), sqlx::Error> {
    let sql = r#"
            UPDATE candles_01d
            SET is_archived = True
            WHERE datetime = $1
            AND market_id = $2
        "#;
    sqlx::query(&sql)
        .bind(candle.datetime)
        .bind(market_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::fetch_exchanges;
    use crate::markets::fetch_markets;
    use chrono::{TimeZone, Utc};

    pub fn sample_trades() -> Vec<Trade> {
        let mut trades: Vec<Trade> = Vec::new();
        trades.push(Trade {
            id: 1,
            price: Decimal::new(702, 1),
            size: Decimal::new(23, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524886322, 0),
        });
        trades.push(Trade {
            id: 2,
            price: Decimal::new(752, 1),
            size: Decimal::new(64, 1),
            side: "buy".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524887322, 0),
        });
        trades.push(Trade {
            id: 3,
            price: Decimal::new(810, 1),
            size: Decimal::new(4, 1),
            side: "buy".to_string(),
            liquidation: true,
            time: Utc.timestamp(1524888322, 0),
        });
        trades.push(Trade {
            id: 4,
            price: Decimal::new(767, 1),
            size: Decimal::new(13, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524889322, 0),
        });
        trades
    }

    #[test]
    pub fn new_from_last_returns_candle_populated_from_last_trade() {
        let mut trades = sample_trades();
        let last_trade = trades.pop().unwrap();
        let candle = Candle::new_from_last(
            last_trade.time,
            last_trade.price,
            last_trade.time,
            &last_trade.id.to_string(),
        );
        println!("Candle: {:?}", candle);
    }

    #[test]
    pub fn new_from_trades_returns_candle() {
        let trades = sample_trades();
        let first_trade = trades.first().unwrap();
        let candle = Candle::new_from_trades(first_trade.time, &trades);
        println!("Candle: {:?}", candle);
    }

    #[tokio::test]
    pub async fn select_last_01d_candle_returns_none() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Drop table if exists
        let sql = "DROP TABLE IF EXISTS candle_01d_none";
        sqlx::query(&sql)
            .execute(&pool)
            .await
            .expect("Could not drop table.");

        // Create table
        let sql = r#"
            CREATE TABLE IF NOT EXISTS candles_01d_none (
                datetime timestamptz NOT NULL,
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                volume NUMERIC NOT NULL,
                volume_net NUMERIC NOT NULL,
                volume_liquidation NUMERIC NOT NULL,
                value NUMERIC NOT NULL,
                trade_count BIGINT NOT NULL,
                liquidation_count BIGINT NOT NULL,
                last_trade_ts timestamptz NOT NULL,
                last_trade_id TEXT NOT NULL,
                is_validated BOOLEAN NOT NULL,
                market_id uuid NOT NULL,
                first_trade_ts timestamptz NOT NULL,
                first_trade_id TEXT NOT NULL,
                is_archived BOOLEAN NOT NULL,
                PRIMARY KEY (datetime, market_id)
            )
            "#;
        sqlx::query(&sql)
            .execute(&pool)
            .await
            .expect("Could not create 01d candle table.");

        // Select from empty table
        let row = sqlx::query_as::<_, DailyCandle>(
            r#"
            SELECT * FROM candles_01d
            ORDER BY datetime DESC
            "#,
        )
        .fetch_one(&pool)
        .await;
        match row {
            Ok(row) => {
                println!("Ok row: {:?}", row);
                panic!("Expected error!")
            }
            Err(e) => {
                println!("Err: {:?}", e)
            }
        }
    }

    #[tokio::test]
    pub async fn select_last_01d_candles_returns_candle() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let _pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
    }

    #[tokio::test]
    pub async fn revalidate_invalid_candle() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges.iter().find(|e| e.exchange_name == "ftx").unwrap();

        // Set client = FTX for hardcoded candle tests
        let client = RestClient::new_intl();

        // Get market ids
        let market_ids = fetch_markets(&pool, &exchange)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == "BTC-PERP")
            .unwrap();

        // Create test table
        let table = "invalid_candle";
        let sql_drop = format!("DROP TABLE IF EXISTS {}", table);
        sqlx::query(&sql_drop)
            .execute(&pool)
            .await
            .expect("Could not drop table.");
        let sql_create = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                datetime timestamptz NOT NULL,
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                volume NUMERIC NOT NULL,
                volume_net NUMERIC NOT NULL,
                volume_liquidation NUMERIC NOT NULL,
                value NUMERIC NOT NULL,
                trade_count BIGINT NOT NULL,
                liquidation_count BIGINT NOT NULL,
                last_trade_ts timestamptz NOT NULL,
                last_trade_id TEXT NOT NULL,
                is_validated BOOLEAN NOT NULL,
                market_id uuid NOT NULL,
                first_trade_ts timestamptz NOT NULL,
                first_trade_id TEXT NOT NULL,
                PRIMARY KEY (datetime, market_id)
            )
            "#,
            table
        );
        sqlx::query(&sql_create)
            .execute(&pool)
            .await
            .expect("Could not create table");

        // Insert bad candle to re-evaluate
        let sql_insert = format!(
            r#"
            INSERT INTO {} (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                market_id, first_trade_ts, first_trade_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            "#,
            table
        );
        sqlx::query(&sql_insert)
            .bind(Utc.ymd(2021, 6, 27).and_hms(2, 15, 0))
            .bind(dec!(32703))
            .bind(dec!(33198))
            .bind(dec!(32703))
            .bind(dec!(33068))
            .bind(dec!(2023.0286))
            .bind(dec!(606.8350))
            .bind(dec!(8.1311))
            .bind(dec!(66753198.2958))
            .bind(8532)
            .bind(95)
            .bind(Utc.ymd(2021, 6, 27).and_hms_micro(2, 29, 59, 789715))
            .bind("1364514169")
            .bind(false)
            .bind(Uuid::parse_str("bb8a0b07-9864-40eb-aa8d-0f87c2ac7464").unwrap())
            .bind(Utc.ymd(2021, 6, 27).and_hms_micro(2, 15, 1, 119634))
            .bind("1364455450")
            .execute(&pool)
            .await
            .expect("Could not insert bad candle.");

        // Select invalid candles from the test table and revalidate
        let sql = format!(
            r#"
            SELECT * FROM {}
            "#,
            table
        );
        let candles = sqlx::query_as::<_, Candle>(&sql)
            .fetch_all(&pool)
            .await
            .expect("Could not fetch invalidated candles.");
        for candle in candles.iter() {
            println!("Attempting to revalidate: {:?}", candle);
            let is_success =
                qc_unvalidated_candle(&client, &pool, &exchange.exchange_name, &market, &candle)
                    .await;
            println!("Revalidate success? {:?}", is_success);
        }
    }
}
