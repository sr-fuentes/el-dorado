use crate::exchanges::{ftx::Candle as CandleFtx, ftx::RestClient, ftx::Trade, Exchange};
use crate::markets::MarketId;
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

pub async fn get_ftx_candles(
    client: &RestClient,
    market: &MarketId,
    start: DateTime<Utc>,
    mut end_or_last: DateTime<Utc>,
) -> Vec<CandleFtx> {
    let mut candles: Vec<CandleFtx> = Vec::new();
    while start < end_or_last {
        // Prevent 429 errors by only requesting 4 per second
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let mut new_candles = client
            .get_candles(
                &market.market_name,
                Some(900),
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
    market: &MarketId,
    exchange: &Exchange,
    candle: Candle,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            INSERT INTO candles_15T_{} (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                market_id, first_trade_ts, first_trade_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
        exchange.exchange_name,
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
        .bind(false)
        .bind(market.market_id)
        .bind(candle.first_trade_ts)
        .bind(candle.first_trade_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_candles_01d(
    pool: &PgPool,
    market: &MarketId,
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
            .bind(market.market_id)
            .bind(candle.first_trade_ts)
            .bind(&candle.first_trade_id)
            .bind(false)
            .bind(false)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn select_unvalidated_candles(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1 and not is_validated
        ORDER BY datetime
       "#,
        exchange.exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market.market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        ORDER BY datetime
        "#,
        exchange.exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market.market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles_gte_datetime(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
    datetime: DateTime<Utc>,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        AND datetime >= $2
        ORDER BY datetime
        "#,
        exchange.exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market.market_id)
        .bind(datetime)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_last_01d_candle(
    pool: &PgPool,
    market: &MarketId,
) -> Result<DailyCandle, sqlx::Error> {
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE market_id = $1
        ORDER BY datetime DESC
        "#;
    let row = sqlx::query_as::<_, DailyCandle>(&sql)
        .bind(market.market_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn select_previous_candle(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
    datetime: DateTime<Utc>,
) -> Result<Candle, sqlx::Error> {
    let sql = format!(
        r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1
            AND datetime < $2
            ORDER BY datetime DESC
        "#,
        exchange.exchange_name
    );
    let row = sqlx::query_as::<_, Candle>(&sql)
        .bind(market.market_id)
        .bind(datetime)
        .fetch_one(pool)
        .await?;
    Ok(row)
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

pub async fn update_candle_validation(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
    candle: &Candle,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            UPDATE candles_15t_{}
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#,
        exchange.exchange_name
    );
    sqlx::query(&sql)
        .bind(candle.datetime)
        .bind(market.market_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::get_configuration;
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
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
    }
}
