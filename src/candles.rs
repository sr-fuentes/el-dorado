use crate::{
    configuration::Database,
    eldorado::ElDorado,
    exchanges::{
        error::RestError, ftx::Trade as FtxTrade, gdax::Candle as GdaxCandle,
        gdax::Trade as GdaxTrade, ExchangeName,
    },
    markets::{MarketArchiveDetail, MarketCandleDetail, MarketDetail},
    trades::{PrIdTi, Trade},
    utilities::{DateRange, TimeFrame},
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use csv::Writer;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::PathBuf;

pub trait Candle {
    fn datetime(&self) -> DateTime<Utc>;
    fn close(&self) -> Decimal;
    fn volume(&self) -> Decimal;
}

#[derive(Debug)]
pub enum CandleType {
    Research,
    Production,
    Ftx,
    Gdax,
}

impl CandleType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CandleType::Research => "research",
            CandleType::Production => "production",
            CandleType::Ftx => "ftx",
            CandleType::Gdax => "gdax",
        }
    }
}

impl TryFrom<String> for CandleType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "research" => Ok(Self::Research),
            "production" => Ok(Self::Production),
            "ftx" => Ok(Self::Ftx),
            "gdax" => Ok(Self::Gdax),
            other => Err(format!("{} is not a supported candle type.", other)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
pub struct ProductionCandle {
    pub datetime: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub volume_buy: Decimal,
    pub volume_sell: Decimal,
    pub volume_liq: Decimal,
    pub volume_liq_buy: Decimal,
    pub volume_liq_sell: Decimal,
    pub value: Decimal,
    pub value_buy: Decimal,
    pub value_sell: Decimal,
    pub value_liq: Decimal,
    pub value_liq_buy: Decimal,
    pub value_liq_sell: Decimal,
    pub trade_count: i64,
    pub trade_count_buy: i64,
    pub trade_count_sell: i64,
    pub liq_count: i64,
    pub liq_count_buy: i64,
    pub liq_count_sell: i64,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub first_trade_ts: DateTime<Utc>,
    pub first_trade_id: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
pub struct ResearchCandle {
    pub datetime: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub volume_buy: Decimal,
    pub volume_sell: Decimal,
    pub volume_liq: Decimal,
    pub volume_liq_buy: Decimal,
    pub volume_liq_sell: Decimal,
    pub value: Decimal,
    pub value_buy: Decimal,
    pub value_sell: Decimal,
    pub value_liq: Decimal,
    pub value_liq_buy: Decimal,
    pub value_liq_sell: Decimal,
    pub trade_count: i64,
    pub trade_count_buy: i64,
    pub trade_count_sell: i64,
    pub liq_count: i64,
    pub liq_count_buy: i64,
    pub liq_count_sell: i64,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub first_trade_ts: DateTime<Utc>,
    pub first_trade_id: String,
}

impl ProductionCandle {
    pub fn open_as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.first_trade_id.parse::<i64>().unwrap(),
            dt: self.first_trade_ts,
            price: self.open,
        }
    }

    pub fn close_as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.last_trade_id.parse::<i64>().unwrap(),
            dt: self.last_trade_ts,
            price: self.close,
        }
    }

    // Takes a Vec of Trade and aggregates into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from trades in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close / Datetime may
    // be incorrect.
    pub fn from_trades<T: Trade>(datetime: DateTime<Utc>, trades: &[&T]) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No first trade for candle.").price(), // open
                Decimal::MIN,                                                // high
                Decimal::MAX,                                                // low
                dec!(0),                                                     // close
                dec!(0),                                                     // volume
                dec!(0),                                                     // volume buy
                dec!(0),                                                     // volume sell
                dec!(0),                                                     // volume liq
                dec!(0),                                                     // volume liq buy
                dec!(0),                                                     // volume liq sell
                dec!(0),                                                     // value
                dec!(0),                                                     // value buy
                dec!(0),                                                     // value sell
                dec!(0),                                                     // value liq
                dec!(0),                                                     // value liq buy
                dec!(0),                                                     // value liq sell
                0,                                                           // count
                0,                                                           // count buy
                0,                                                           // count sell
                0,                                                           // liq count,
                0,                                                           // liq count buy,
                0,                                                           // liq count sell,
                datetime,                                                    // last_trade_ts
                "".to_string(),                                              // last_trade_id
                trades.first().expect("No first trade.").time(),             // first_trade_ts
                trades
                    .first()
                    .expect("No first trade.")
                    .trade_id()
                    .to_string(), // first_trade_id
            ),
            |(
                o,
                h,
                l,
                _c,
                v,
                vb,
                vs,
                vl,
                vlb,
                vls,
                u,
                ub,
                us,
                al,
                alb,
                als,
                n,
                nb,
                ns,
                ln,
                lnb,
                lns,
                _ts,
                _id,
                fts,
                fid,
            ),
             t| {
                // Put side and liq if statements here
                let value = t.size() * t.price();
                let (
                    volume_buy,
                    volume_sell,
                    volume_liq,
                    volume_liq_buy,
                    volume_liq_sell,
                    value_buy,
                    value_sell,
                    value_liq,
                    value_liq_buy,
                    value_liq_sell,
                    n_buy,
                    n_sell,
                    n_liq,
                    n_liq_buy,
                    n_liq_sell,
                ) = if t.side() == "buy" {
                    if t.liquidation() {
                        (
                            t.size(),
                            dec!(0),
                            t.size(),
                            t.size(),
                            dec!(0),
                            value,
                            dec!(0),
                            value,
                            value,
                            dec!(0),
                            1,
                            0,
                            1,
                            1,
                            0,
                        )
                    } else {
                        (
                            t.size(),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            value,
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            1,
                            0,
                            0,
                            0,
                            0,
                        )
                    }
                } else {
                    if t.liquidation() {
                        (
                            dec!(0),
                            t.size(),
                            t.size(),
                            dec!(0),
                            t.size(),
                            dec!(0),
                            value,
                            value,
                            dec!(0),
                            value,
                            0,
                            1,
                            1,
                            0,
                            1,
                        )
                    } else {
                        (
                            dec!(0),
                            t.size(),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            value,
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            0,
                            1,
                            0,
                            0,
                            0,
                        )
                    }
                };
                (
                    o,                // open
                    h.max(t.price()), // high
                    l.min(t.price()), // low
                    t.price(),        // close
                    v + t.size(),     // volume
                    vb + volume_buy,
                    vs + volume_sell,
                    vl + volume_liq,
                    vlb + volume_liq_buy,
                    vls + volume_liq_sell,
                    u + value,
                    ub + value_buy,
                    us + value_sell,
                    al + value_liq,
                    alb + value_liq_buy,
                    als + value_liq_sell,
                    n + 1,
                    nb + n_buy,
                    ns + n_sell,
                    ln + n_liq,
                    lnb + n_liq_buy,
                    lns + n_liq_sell,
                    t.time(),
                    t.trade_id().to_string(),
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
            volume_buy: candle_tuple.5,
            volume_sell: candle_tuple.6,
            volume_liq: candle_tuple.7,
            volume_liq_buy: candle_tuple.8,
            volume_liq_sell: candle_tuple.9,
            value: candle_tuple.10,
            value_buy: candle_tuple.11,
            value_sell: candle_tuple.12,
            value_liq: candle_tuple.13,
            value_liq_buy: candle_tuple.14,
            value_liq_sell: candle_tuple.15,
            trade_count: candle_tuple.16,
            trade_count_buy: candle_tuple.17,
            trade_count_sell: candle_tuple.18,
            liq_count: candle_tuple.19,
            liq_count_buy: candle_tuple.20,
            liq_count_sell: candle_tuple.21,
            last_trade_ts: candle_tuple.22,
            last_trade_id: candle_tuple.23,
            first_trade_ts: candle_tuple.24,
            first_trade_id: candle_tuple.25,
        }
    }

    // This function will build a placeholder trade with 0 volume and
    // will populate OHLC from the last trade provided.
    pub fn from_last(datetime: DateTime<Utc>, pit: &PrIdTi) -> Self {
        Self {
            datetime,
            open: pit.price, // All OHLC are = last trade price
            high: pit.price,
            low: pit.price,
            close: pit.price,
            volume: dec!(0),
            volume_buy: dec!(0),
            volume_sell: dec!(0),
            volume_liq: dec!(0),
            volume_liq_buy: dec!(0),
            volume_liq_sell: dec!(0),
            value: dec!(0),
            value_buy: dec!(0),
            value_sell: dec!(0),
            value_liq: dec!(0),
            value_liq_buy: dec!(0),
            value_liq_sell: dec!(0),
            trade_count: 0,
            trade_count_buy: 0,
            trade_count_sell: 0,
            liq_count: 0,
            liq_count_buy: 0,
            liq_count_sell: 0,
            last_trade_ts: pit.dt,
            last_trade_id: pit.id.to_string(),
            first_trade_ts: pit.dt,
            first_trade_id: pit.id.to_string(),
        }
    }

    // Takes a Vec of Candles and resamples into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from candes in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close may
    // be incorrect.
    pub fn new_from_candles(dt: &DateTime<Utc>, candles: &[Self]) -> Self {
        let first = match candles.first() {
            Some(c) => c,
            None => panic!("Cannot build candle from empty vec of candles."),
        };
        let candle = candles.iter().fold(
            (
                first.open,                   // OPEN
                Decimal::MIN,                 // HIGH
                Decimal::MAX,                 // LOW
                dec!(0),                      // CLOSE
                dec!(0),                      // VOLUME
                dec!(0),                      // VOLUME BUY
                dec!(0),                      // VOLUME SELL
                dec!(0),                      // VOLUME LIQ
                dec!(0),                      // VOLUME LIQ BUY
                dec!(0),                      // VOLUME LIQ SELL
                dec!(0),                      // VALUE
                dec!(0),                      // VALUE BUY
                dec!(0),                      // VALUE SELL
                dec!(0),                      // VALUE LIQ
                dec!(0),                      // VALUE LIQ BUY
                dec!(0),                      // VALUE LIQ SELL
                0,                            // COUNT
                0,                            // COUNT BUY
                0,                            // COUNT SELL
                0,                            // LIQ COUNT
                0,                            // LIQ COUNT BUY
                0,                            // LIQ COUNT SELL
                *dt,                          // LAST TRADE TS
                String::new(),                // LAST TRADE ID
                first.first_trade_ts,         // FIRST TRADE TS
                first.first_trade_id.clone(), // FIRST TRADE ID
            ),
            |(
                o,
                h,
                l,
                _c,
                v,
                vb,
                vs,
                vl,
                vlb,
                vls,
                u,
                ub,
                us,
                al,
                alb,
                als,
                n,
                nb,
                ns,
                ln,
                lnb,
                lns,
                _ts,
                _id,
                fts,
                fid,
            ),
             c| {
                (
                    o,
                    h.max(c.high),
                    l.min(c.low),
                    c.close,
                    v + c.volume,
                    vb + c.volume_buy,
                    vs + c.volume_sell,
                    vl + c.volume_liq,
                    vlb + c.volume_liq_buy,
                    vls + c.volume_liq_sell,
                    u + c.value,
                    ub + c.value_buy,
                    us + c.value_sell,
                    al + c.value_liq,
                    alb + c.value_liq_buy,
                    als + c.value_liq_sell,
                    n + c.trade_count,
                    nb + c.trade_count_buy,
                    ns + c.trade_count_sell,
                    ln + c.liq_count,
                    lnb + c.liq_count_buy,
                    lns + c.liq_count_sell,
                    c.last_trade_ts,
                    c.last_trade_id.clone(),
                    fts,
                    fid,
                )
            },
        );
        Self {
            datetime: *dt,
            open: candle.0,
            high: candle.1,
            low: candle.2,
            close: candle.3,
            volume: candle.4,
            volume_buy: candle.5,
            volume_sell: candle.6,
            volume_liq: candle.7,
            volume_liq_buy: candle.8,
            volume_liq_sell: candle.9,
            value: candle.10,
            value_buy: candle.11,
            value_sell: candle.12,
            value_liq: candle.13,
            value_liq_buy: candle.14,
            value_liq_sell: candle.15,
            trade_count: candle.16,
            trade_count_buy: candle.17,
            trade_count_sell: candle.18,
            liq_count: candle.19,
            liq_count_buy: candle.20,
            liq_count_sell: candle.21,
            last_trade_ts: candle.22,
            last_trade_id: candle.23,
            first_trade_ts: candle.24,
            first_trade_id: candle.25,
        }
    }

    pub fn from_trades_for_dr<T: Trade>(
        trades: &[T],
        mut last_trade: Option<PrIdTi>,
        tf: &TimeFrame,
        dr: &[DateTime<Utc>],
    ) -> Vec<Self> {
        // Iterate through dr, filter trades and make candles for each interval
        // TODO - Add validation that dr start interval has trades if the last trade is None to
        // prevent panic on unwrap of last trade for ::from_last() call
        let candles = dr.iter().fold(Vec::new(), |mut v, d| {
            let filtered_trades: Vec<_> = trades
                .iter()
                .filter(|t| t.time().duration_trunc(tf.as_dur()).unwrap() == *d)
                .collect();
            let new_candle = match filtered_trades.is_empty() {
                true => Self::from_last(*d, &last_trade.unwrap()),
                false => Self::from_trades(*d, &filtered_trades),
            };
            last_trade = Some(new_candle.close_as_pridti());
            v.push(new_candle);
            v
        });
        candles
    }

    pub fn resample(candles: &[Self], tf: &TimeFrame, dr: &[DateTime<Utc>]) -> Vec<Self> {
        // Check first that there are candles to resample
        if candles.is_empty() {
            // Return original empty vec
            candles.to_vec()
        } else {
            // Create a candle for each date in the daterange
            // TODO! - Test against drain filter for speed
            dr.iter().fold(Vec::new(), |mut v, d| {
                let interval_candles: Vec<_> = candles
                    .iter()
                    .filter(|c| c.datetime.duration_trunc(tf.as_dur()).unwrap() == *d)
                    .cloned()
                    .collect();
                let resampled_candle = Self::new_from_candles(d, &interval_candles);
                v.push(resampled_candle);
                v
            })
        }
    }

    pub async fn create_table(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
    ) -> Result<(), sqlx::Error> {
        // Cannot use query! macro as table does not exist
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS candles.production_{}_{}_{} (
                datetime timestamptz NOT NULL,
                PRIMARY KEY (datetime),
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                volume NUMERIC NOT NULL,
                volume_buy NUMERIC NOT NULL,
                volume_sell NUMERIC NOT NULL,
                volume_liq NUMERIC NOT NULL,
                volume_liq_buy NUMERIC NOT NULL,
                volume_liq_sell NUMERIC NOT NULL,
                value NUMERIC NOT NULL,
                value_buy NUMERIC NOT NULL,
                value_sell NUMERIC NOT NULL,
                value_liq NUMERIC NOT NULL,
                value_liq_buy NUMERIC NOT NULL,
                value_liq_sell NUMERIC NOT NULL,
                trade_count BIGINT NOT NULL,
                trade_count_buy BIGINT NOT NULL,
                trade_count_sell BIGINT NOT NULL,
                liq_count BIGINT NOT NULL,
                liq_count_buy BIGINT NOT NULL,
                liq_count_sell BIGINT NOT NULL,
                last_trade_ts timestamptz NOT NULL,
                last_trade_id TEXT NOT NULL,
                first_trade_ts timestamptz NOT NULL,
                first_trade_id TExT NOT NULL
            )
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql).execute(pool).await?;
        Ok(())
    }

    pub async fn insert(
        &self,
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
    ) -> Result<(), sqlx::Error> {
        // Cannot use query! macro as table may not exist at compile time
        let sql = format!(
            r#"
            INSERT INTO candles.production_{}_{}_{}
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24, $25, $26, $27)
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql)
            .bind(self.datetime)
            .bind(self.open)
            .bind(self.high)
            .bind(self.low)
            .bind(self.close)
            .bind(self.volume)
            .bind(self.volume_buy)
            .bind(self.volume_sell)
            .bind(self.volume_liq)
            .bind(self.volume_liq_buy)
            .bind(self.volume_liq_sell)
            .bind(self.value)
            .bind(self.value_buy)
            .bind(self.value_sell)
            .bind(self.value_liq)
            .bind(self.value_liq_buy)
            .bind(self.value_liq_sell)
            .bind(self.trade_count)
            .bind(self.trade_count_buy)
            .bind(self.trade_count_sell)
            .bind(self.liq_count)
            .bind(self.liq_count_buy)
            .bind(self.liq_count_sell)
            .bind(self.last_trade_ts)
            .bind(&self.last_trade_id)
            .bind(self.first_trade_ts)
            .bind(&self.first_trade_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn select_gte_dt(
        pool: &PgPool,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.production_{}_{}_{}
            WHERE datetime >= $1
            ORDER BY datetime ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            market.tf.as_str(),
        );
        let rows = sqlx::query_as::<_, ProductionCandle>(&sql)
            .bind(dt)
            .fetch_all(pool)
            .await?;
        Ok(rows)
    }

    pub async fn select_eq_dt(
        pool: &PgPool,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.production_{}_{}_{}
            WHERE datetime = $1
            ORDER BY datetime ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            market.tf.as_str(),
        );
        let row = sqlx::query_as::<_, ProductionCandle>(&sql)
            .bind(dt)
            .fetch_one(pool)
            .await?;
        Ok(row)
    }

    pub async fn select_first(pool: &PgPool, market: &MarketDetail) -> Result<Self, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.production_{}_{}_{}
            ORDER BY datetime ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            market.tf.as_str(),
        );
        let row = sqlx::query_as::<_, ProductionCandle>(&sql)
            .fetch_one(pool)
            .await?;
        Ok(row)
    }
    pub async fn select_last_non_zero(
        pool: &PgPool,
        market: &MarketDetail,
    ) -> Result<Self, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.production_{}_{}_{}
            WHERE volume > 0
            ORDER BY datetime DESC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            market.tf.as_str(),
        );
        let row = sqlx::query_as::<_, ProductionCandle>(&sql)
            .fetch_one(pool)
            .await?;
        Ok(row)
    }

    // Delete an trades less than a give date for a give market
    pub async fn delete_lt_dt(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
        dt: &DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let sql = format!(
            r#"
            DELETE FROM candles.production_{}_{}_{}
            WHERE datetime < $1
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql).bind(dt).execute(pool).await?;
        Ok(())
    }

    pub async fn delete_gt_dt(
        pool: &PgPool,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let sql = format!(
            r#"
            DELETE FROM candles.production_{}_{}_{}
            WHERE datetime > $1
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            market.tf.as_str(),
        );
        sqlx::query(&sql).bind(dt).execute(pool).await?;
        Ok(())
    }
}

impl Candle for ResearchCandle {
    fn datetime(&self) -> DateTime<Utc> {
        self.datetime
    }
    fn close(&self) -> Decimal {
        self.close
    }
    fn volume(&self) -> Decimal {
        self.volume
    }
}

impl ResearchCandle {
    pub fn close_as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.last_trade_id.parse::<i64>().unwrap(),
            dt: self.last_trade_ts,
            price: self.close,
        }
    }

    // Reduces the number of if statements in each iteration
    pub fn from_trades_v2<T: Trade>(datetime: DateTime<Utc>, trades: &[T]) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No first trade for candle.").price(), // open
                Decimal::MIN,                                                // high
                Decimal::MAX,                                                // low
                dec!(0),                                                     // close
                dec!(0),                                                     // volume
                dec!(0),                                                     // volume buy
                dec!(0),                                                     // volume sell
                dec!(0),                                                     // volume liq
                dec!(0),                                                     // volume liq buy
                dec!(0),                                                     // volume liq sell
                dec!(0),                                                     // value
                dec!(0),                                                     // value buy
                dec!(0),                                                     // value sell
                dec!(0),                                                     // value liq
                dec!(0),                                                     // value liq buy
                dec!(0),                                                     // value liq sell
                0,                                                           // count
                0,                                                           // count buy
                0,                                                           // count sell
                0,                                                           // liq count,
                0,                                                           // liq count buy,
                0,                                                           // liq count sell,
                datetime,                                                    // last_trade_ts
                "".to_string(),                                              // last_trade_id
                trades.first().expect("No first trade.").time(),             // first_trade_ts
                trades
                    .first()
                    .expect("No first trade.")
                    .trade_id()
                    .to_string(), // first_trade_id
            ),
            |(
                o,
                h,
                l,
                _c,
                v,
                vb,
                vs,
                vl,
                vlb,
                vls,
                u,
                ub,
                us,
                al,
                alb,
                als,
                n,
                nb,
                ns,
                ln,
                lnb,
                lns,
                _ts,
                _id,
                fts,
                fid,
            ),
             t| {
                // Put side and liq if statements here
                let value = t.size() * t.price();
                let (
                    volume_buy,
                    volume_sell,
                    volume_liq,
                    volume_liq_buy,
                    volume_liq_sell,
                    value_buy,
                    value_sell,
                    value_liq,
                    value_liq_buy,
                    value_liq_sell,
                    n_buy,
                    n_sell,
                    n_liq,
                    n_liq_buy,
                    n_liq_sell,
                ) = if t.side() == "buy" {
                    if t.liquidation() {
                        (
                            t.size(),
                            dec!(0),
                            t.size(),
                            t.size(),
                            dec!(0),
                            value,
                            dec!(0),
                            value,
                            value,
                            dec!(0),
                            1,
                            0,
                            1,
                            1,
                            0,
                        )
                    } else {
                        (
                            t.size(),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            value,
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            1,
                            0,
                            0,
                            0,
                            0,
                        )
                    }
                } else {
                    if t.liquidation() {
                        (
                            dec!(0),
                            t.size(),
                            t.size(),
                            dec!(0),
                            t.size(),
                            dec!(0),
                            value,
                            value,
                            dec!(0),
                            value,
                            0,
                            1,
                            1,
                            0,
                            1,
                        )
                    } else {
                        (
                            dec!(0),
                            t.size(),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            value,
                            dec!(0),
                            dec!(0),
                            dec!(0),
                            0,
                            1,
                            0,
                            0,
                            0,
                        )
                    }
                };
                (
                    o,                // open
                    h.max(t.price()), // high
                    l.min(t.price()), // low
                    t.price(),        // close
                    v + t.size(),     // volume
                    vb + volume_buy,
                    vs + volume_sell,
                    vl + volume_liq,
                    vlb + volume_liq_buy,
                    vls + volume_liq_sell,
                    u + value,
                    ub + value_buy,
                    us + value_sell,
                    al + value_liq,
                    alb + value_liq_buy,
                    als + value_liq_sell,
                    n + 1,
                    nb + n_buy,
                    ns + n_sell,
                    ln + n_liq,
                    lnb + n_liq_buy,
                    lns + n_liq_sell,
                    t.time(),
                    t.trade_id().to_string(),
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
            volume_buy: candle_tuple.5,
            volume_sell: candle_tuple.6,
            volume_liq: candle_tuple.7,
            volume_liq_buy: candle_tuple.8,
            volume_liq_sell: candle_tuple.9,
            value: candle_tuple.10,
            value_buy: candle_tuple.11,
            value_sell: candle_tuple.12,
            value_liq: candle_tuple.13,
            value_liq_buy: candle_tuple.14,
            value_liq_sell: candle_tuple.15,
            trade_count: candle_tuple.16,
            trade_count_buy: candle_tuple.17,
            trade_count_sell: candle_tuple.18,
            liq_count: candle_tuple.19,
            liq_count_buy: candle_tuple.20,
            liq_count_sell: candle_tuple.21,
            last_trade_ts: candle_tuple.22,
            last_trade_id: candle_tuple.23,
            first_trade_ts: candle_tuple.24,
            first_trade_id: candle_tuple.25,
        }
    }

    // This function will build a placeholder trade with 0 volume and
    // will populate OHLC from the last trade provided.
    pub fn from_last(datetime: DateTime<Utc>, pit: &PrIdTi) -> Self {
        Self {
            datetime,
            open: pit.price, // All OHLC are = last trade price
            high: pit.price,
            low: pit.price,
            close: pit.price,
            volume: dec!(0),
            volume_buy: dec!(0),
            volume_sell: dec!(0),
            volume_liq: dec!(0),
            volume_liq_buy: dec!(0),
            volume_liq_sell: dec!(0),
            value: dec!(0),
            value_buy: dec!(0),
            value_sell: dec!(0),
            value_liq: dec!(0),
            value_liq_buy: dec!(0),
            value_liq_sell: dec!(0),
            trade_count: 0,
            trade_count_buy: 0,
            trade_count_sell: 0,
            liq_count: 0,
            liq_count_buy: 0,
            liq_count_sell: 0,
            last_trade_ts: pit.dt,
            last_trade_id: pit.id.to_string(),
            first_trade_ts: pit.dt,
            first_trade_id: pit.id.to_string(),
        }
    }

    // Takes a Vec of Candles and resamples into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from candes in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close may
    // be incorrect.
    fn new_from_candles(dt: &DateTime<Utc>, candles: &[Self]) -> Self {
        let first = match candles.first() {
            Some(c) => c,
            None => panic!("Cannot build candle from empty vec of candles."),
        };
        let candle = candles.iter().fold(
            (
                first.open,                   // OPEN
                Decimal::MIN,                 // HIGH
                Decimal::MAX,                 // LOW
                dec!(0),                      // CLOSE
                dec!(0),                      // VOLUME
                dec!(0),                      // VOLUME BUY
                dec!(0),                      // VOLUME SELL
                dec!(0),                      // VOLUME LIQ
                dec!(0),                      // VOLUME LIQ BUY
                dec!(0),                      // VOLUME LIQ SELL
                dec!(0),                      // VALUE
                dec!(0),                      // VALUE BUY
                dec!(0),                      // VALUE SELL
                dec!(0),                      // VALUE LIQ
                dec!(0),                      // VALUE LIQ BUY
                dec!(0),                      // VALUE LIQ SELL
                0,                            // COUNT
                0,                            // COUNT BUY
                0,                            // COUNT SELL
                0,                            // LIQ COUNT
                0,                            // LIQ COUNT BUY
                0,                            // LIQ COUNT SELL
                *dt,                          // LAST TRADE TS
                String::new(),                // LAST TRADE ID
                first.first_trade_ts,         // FIRST TRADE TS
                first.first_trade_id.clone(), // FIRST TRADE ID
            ),
            |(
                o,
                h,
                l,
                _c,
                v,
                vb,
                vs,
                vl,
                vlb,
                vls,
                u,
                ub,
                us,
                al,
                alb,
                als,
                n,
                nb,
                ns,
                ln,
                lnb,
                lns,
                _ts,
                _id,
                fts,
                fid,
            ),
             c| {
                (
                    o,
                    h.max(c.high),
                    l.min(c.low),
                    c.close,
                    v + c.volume,
                    vb + c.volume_buy,
                    vs + c.volume_sell,
                    vl + c.volume_liq,
                    vlb + c.volume_liq_buy,
                    vls + c.volume_liq_sell,
                    u + c.value,
                    ub + c.value_buy,
                    us + c.value_sell,
                    al + c.value_liq,
                    alb + c.value_liq_buy,
                    als + c.value_liq_sell,
                    n + c.trade_count,
                    nb + c.trade_count_buy,
                    ns + c.trade_count_sell,
                    ln + c.liq_count,
                    lnb + c.liq_count_buy,
                    lns + c.liq_count_sell,
                    c.last_trade_ts,
                    c.last_trade_id.clone(),
                    fts,
                    fid,
                )
            },
        );
        Self {
            datetime: *dt,
            open: candle.0,
            high: candle.1,
            low: candle.2,
            close: candle.3,
            volume: candle.4,
            volume_buy: candle.5,
            volume_sell: candle.6,
            volume_liq: candle.7,
            volume_liq_buy: candle.8,
            volume_liq_sell: candle.9,
            value: candle.10,
            value_buy: candle.11,
            value_sell: candle.12,
            value_liq: candle.13,
            value_liq_buy: candle.14,
            value_liq_sell: candle.15,
            trade_count: candle.16,
            trade_count_buy: candle.17,
            trade_count_sell: candle.18,
            liq_count: candle.19,
            liq_count_buy: candle.20,
            liq_count_sell: candle.21,
            last_trade_ts: candle.22,
            last_trade_id: candle.23,
            first_trade_ts: candle.24,
            first_trade_id: candle.25,
        }
    }

    // This function will take a vec of ResearchCandle and convert each candle to a ProductionCandle
    // returning a vec of ProductionCandle
    pub fn as_production_candle(&self) -> ProductionCandle {
        ProductionCandle {
            datetime: self.datetime,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            volume_buy: self.volume_buy,
            volume_sell: self.volume_sell,
            volume_liq: self.volume_liq,
            volume_liq_buy: self.volume_liq_buy,
            volume_liq_sell: self.volume_liq_sell,
            value: self.value,
            value_buy: self.value_buy,
            value_sell: self.value_sell,
            value_liq: self.value_liq,
            value_liq_buy: self.value_liq_buy,
            value_liq_sell: self.value_liq_sell,
            trade_count: self.trade_count,
            trade_count_buy: self.trade_count_buy,
            trade_count_sell: self.trade_count_sell,
            liq_count: self.liq_count,
            liq_count_buy: self.liq_count_buy,
            liq_count_sell: self.liq_count_sell,
            last_trade_ts: self.last_trade_ts,
            last_trade_id: self.last_trade_id.clone(),
            first_trade_ts: self.first_trade_ts,
            first_trade_id: self.first_trade_id.clone(),
        }
    }

    pub fn from_trades_hm_for_dr<T: Trade>(
        trades: &HashMap<DateTime<Utc>, Vec<T>>,
        mut last_trade: Option<PrIdTi>,
        dr: &[DateTime<Utc>],
    ) -> Vec<Self> {
        let candles = dr.iter().fold(Vec::new(), |mut v, d| {
            let new_candle = if !trades.contains_key(d) {
                ResearchCandle::from_last(*d, &last_trade.unwrap())
            } else {
                ResearchCandle::from_trades_v2(*d, trades[d].as_slice())
            };
            last_trade = Some(new_candle.close_as_pridti());
            v.push(new_candle);
            v
        });
        candles
    }

    pub fn resample(candles: &[Self], tf: &TimeFrame, dr: &[DateTime<Utc>]) -> Vec<Self> {
        // Check first that there are candles to resample
        if candles.is_empty() {
            // Return original empty vec
            candles.to_vec()
        } else {
            // Create a candle for each date in the daterange
            // TODO! - Test against drain filter for speed
            dr.iter().fold(Vec::new(), |mut v, d| {
                let interval_candles: Vec<_> = candles
                    .iter()
                    .filter(|c| c.datetime().duration_trunc(tf.as_dur()).unwrap() == *d)
                    .cloned()
                    .collect();
                let resampled_candle = Self::new_from_candles(d, &interval_candles);
                v.push(resampled_candle);
                v
            })
        }
    }

    // This function will create the research candle table for the given market
    pub async fn create_table(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
    ) -> Result<(), sqlx::Error> {
        // Cannot use query! macro for query validation as the table does not exist
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS candles.research_{}_{}_{} (
                datetime timestamptz NOT NULL,
                PRIMARY KEY (datetime),
                open NUMERIC NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                volume NUMERIC NOT NULL,
                volume_buy NUMERIC NOT NULL,
                volume_sell NUMERIC NOT NULL,
                volume_liq NUMERIC NOT NULL,
                volume_liq_buy NUMERIC NOT NULL,
                volume_liq_sell NUMERIC NOT NULL,
                value NUMERIC NOT NULL,
                value_buy NUMERIC NOT NULL,
                value_sell NUMERIC NOT NULL,
                value_liq NUMERIC NOT NULL,
                value_liq_buy NUMERIC NOT NULL,
                value_liq_sell NUMERIC NOT NULL,
                trade_count BIGINT NOT NULL,
                trade_count_buy BIGINT NOT NULL,
                trade_count_sell BIGINT NOT NULL,
                liq_count BIGINT NOT NULL,
                liq_count_buy BIGINT NOT NULL,
                liq_count_sell BIGINT NOT NULL,
                last_trade_ts timestamptz NOT NULL,
                last_trade_id TEXT NOT NULL,
                first_trade_ts timestamptz NOT NULL,
                first_trade_id TExT NOT NULL
            )
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql).execute(pool).await?;
        Ok(())
    }

    // This function will select the last (latest datetime) research candle from the given table
    pub async fn select_last(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
    ) -> Result<Self, sqlx::Error> {
        // Cannot use query_as! macro to check for query validation as the table may or may not
        // exist.
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.research_{}_{}_{}
            ORDER BY datetime DESC LIMIT 1
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str()
        );
        let row = sqlx::query_as::<_, ResearchCandle>(&sql)
            .fetch_one(pool)
            .await?;
        Ok(row)
    }

    // This function will select all research candles for a given date range start and end
    pub async fn select_dr(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
    ) -> Result<Vec<Self>, sqlx::Error> {
        // Cannot use query_as! macro to check as the table may not exist at compile time.
        let sql = format!(
            r#"
            SELECT datetime, open, high, low, close, volume, volume_buy, volume_sell, volume_liq,
                volume_liq_buy, volume_liq_sell, value, value_buy, value_sell, value_liq,
                value_liq_buy, value_liq_sell, trade_count, trade_count_buy, trade_count_sell,
                liq_count, liq_count_buy, liq_count_sell, last_trade_ts, last_trade_id,
                first_trade_ts, first_trade_id
            FROM candles.research_{}_{}_{}
            WHERE datetime >= $1
            AND datetime < $2
            ORDER BY datetime ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str()
        );
        let rows = sqlx::query_as::<_, ResearchCandle>(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(pool)
            .await?;
        Ok(rows)
    }

    // This function will insert the candle into the database using the pool given
    pub async fn insert(
        &self,
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
    ) -> Result<(), sqlx::Error> {
        // Cannot user query! macro as table may not exist as compile time.
        let sql = format!(
            r#"
            INSERT INTO candles.research_{}_{}_{}
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24, $25, $26, $27)
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql)
            .bind(self.datetime)
            .bind(self.open)
            .bind(self.high)
            .bind(self.low)
            .bind(self.close)
            .bind(self.volume)
            .bind(self.volume_buy)
            .bind(self.volume_sell)
            .bind(self.volume_liq)
            .bind(self.volume_liq_buy)
            .bind(self.volume_liq_sell)
            .bind(self.value)
            .bind(self.value_buy)
            .bind(self.value_sell)
            .bind(self.value_liq)
            .bind(self.value_liq_buy)
            .bind(self.value_liq_sell)
            .bind(self.trade_count)
            .bind(self.trade_count_buy)
            .bind(self.trade_count_sell)
            .bind(self.liq_count)
            .bind(self.liq_count_buy)
            .bind(self.liq_count_sell)
            .bind(self.last_trade_ts)
            .bind(&self.last_trade_id)
            .bind(self.first_trade_ts)
            .bind(&self.first_trade_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    // Delete an trades less than a give date for a give market
    pub async fn delete_lt_dt(
        pool: &PgPool,
        market: &MarketDetail,
        tf: &TimeFrame,
        dt: &DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let sql = format!(
            r#"
            DELETE FROM candles.research_{}_{}_{}
            WHERE datetime < $1
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        sqlx::query(&sql).bind(dt).execute(pool).await?;
        Ok(())
    }
}

impl ElDorado {
    pub async fn create_candles_schema(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            CREATE SCHEMA IF NOT EXISTS candles
            "#;
        sqlx::query(sql).execute(pool).await?;
        Ok(())
    }

    pub async fn candle_table_exists(
        &self,
        market: &MarketDetail,
        tf: &TimeFrame,
        candle: &CandleType,
    ) -> bool {
        // Get the full trade table name and then check self fn for table and schema
        // ie research_ftx_btcperp_s15 or production_ftx_btcperp_t15
        let table = format!(
            "{}_{}_{}_{}",
            candle.as_str(),
            market.exchange_name.as_str(),
            market.as_strip(),
            tf.as_str(),
        );
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        ElDorado::table_exists(&self.pools[&db], "candles", &table)
            .await
            .expect("Failed to check table.")
    }

    pub fn convert_research_candles(&self, candles: &[ResearchCandle]) -> Vec<ProductionCandle> {
        candles.iter().map(|c| c.as_production_candle()).collect()
    }

    pub fn resample_and_convert_research_candles(
        &self,
        candles: &[ResearchCandle],
        tf: &TimeFrame,
    ) -> Vec<ProductionCandle> {
        // Different approaches for perfomance. Resample from Vec is slow as it re-filters entire
        // vec for each time frame
        let resampled_candles = self.resample_research_candles(candles, tf);
        self.convert_research_candles(&resampled_candles)
    }

    pub fn resample_and_convert_research_candles_by_hashmap(
        &self,
        candles: &[ResearchCandle],
        tf: &TimeFrame,
    ) -> Vec<ProductionCandle> {
        let resampled_candles = self.resample_research_candles_by_hashmap(candles, tf);
        // Finally convert to production
        self.convert_research_candles(&resampled_candles)
    }

    pub fn resample_and_convert_research_candles_by_hashmap_v2(
        &self,
        candles: &[ResearchCandle],
        tf: &TimeFrame,
    ) -> Vec<ProductionCandle> {
        // Convert the candle during the maping process to hashmap, then resample already converted
        let mut candle_map: HashMap<DateTime<Utc>, Vec<ProductionCandle>> = HashMap::new();
        for candle in candles.iter() {
            candle_map
                .entry(candle.datetime.duration_trunc(tf.as_dur()).unwrap())
                .and_modify(|v| v.push(candle.as_production_candle()))
                .or_insert_with(|| vec![candle.as_production_candle()]);
        }
        // Second create the date range for the resampled candles. If candles passed are S15
        // research candles then the last candle is 23:59:45. The last dt for the dr should be
        // for a T15 tf should be 23:45:00 so the end time for the DR:new is 00:00:00
        // dr can be unwrapped as there will be control on calling this function to make sure there
        // is at least one date in dr to resample. TODO: Convert to Result<Vec, Err>
        let dr = DateRange::new(
            &candles
                .first()
                .unwrap()
                .datetime
                .duration_trunc(tf.as_dur())
                .unwrap(),
            &(candles
                .last()
                .unwrap()
                .datetime
                .duration_trunc(tf.as_dur())
                .unwrap()
                + tf.as_dur()),
            tf,
        )
        .unwrap();
        // For each dr - aggregate the candles
        dr.dts.iter().fold(Vec::new(), |mut v, d| {
            v.push(ProductionCandle::new_from_candles(d, &candle_map[d]));
            v
        })
    }

    pub fn resample_research_candles_by_hashmap(
        &self,
        candles: &[ResearchCandle],
        tf: &TimeFrame,
    ) -> Vec<ResearchCandle> {
        // First map research candles into hash map, then resample each hashmap key
        let mut candle_map: HashMap<DateTime<Utc>, Vec<ResearchCandle>> = HashMap::new();
        for candle in candles.iter() {
            candle_map
                .entry(candle.datetime.duration_trunc(tf.as_dur()).unwrap())
                .and_modify(|v| v.push(candle.clone()))
                .or_insert_with(|| vec![candle.clone()]);
        }
        // Second create the date range for the resampled candles. If candles passed are S15
        // research candles then the last candle is 23:59:45. The last dt for the dr should be
        // for a T15 tf should be 23:45:00 so the end time for the DR:new is 00:00:00
        // dr can be unwrapped as there will be control on calling this function to make sure there
        // is at least one date in dr to resample. TODO: Convert to Result<Vec, Err>
        let dr = DateRange::new(
            &candles
                .first()
                .unwrap()
                .datetime
                .duration_trunc(tf.as_dur())
                .unwrap(),
            &(candles
                .last()
                .unwrap()
                .datetime
                .duration_trunc(tf.as_dur())
                .unwrap()
                + tf.as_dur()),
            tf,
        )
        .unwrap();
        // For each date in the daterange - aggregate the candles
        dr.dts.iter().fold(Vec::new(), |mut v, d| {
            v.push(ResearchCandle::new_from_candles(d, &candle_map[d]));
            v
        })
    }

    // Get date range and resample
    pub fn resample_research_candles(
        &self,
        candles: &[ResearchCandle],
        tf: &TimeFrame,
    ) -> Vec<ResearchCandle> {
        // Create date range
        if candles.is_empty() {
            // No candles to resample, return empty vec
            Vec::new()
        } else {
            // Create date range for resample period
            let first = candles
                .first()
                .unwrap()
                .datetime()
                .duration_trunc(tf.as_dur())
                .unwrap();
            let last = candles
                .last()
                .unwrap()
                .datetime()
                .duration_trunc(tf.as_dur())
                .unwrap();
            let dr = self.create_date_range(&first, &(last + tf.as_dur()), tf);
            ResearchCandle::resample(candles, tf, &dr)
        }
    }

    pub fn resample_production_candles(
        &self,
        candles: &[ProductionCandle],
        tf: &TimeFrame,
    ) -> Vec<ProductionCandle> {
        if !candles.is_empty() {
            // Create date range for reample period
            let first = candles
                .first()
                .expect("Expected candles.")
                .datetime
                .duration_trunc(tf.as_dur())
                .expect("Expected truncation");
            let last = candles
                .last()
                .expect("Expected candles.")
                .datetime
                .duration_trunc(tf.as_dur())
                .expect("Expected truncation.");
            let dr = self.create_date_range(&first, &(last + tf.as_dur()), tf);
            ProductionCandle::resample(candles, tf, &dr)
        } else {
            // No candles to resample, return empty vec
            Vec::new()
        }
    }

    pub fn make_research_candles_for_month(
        &self,
        market: &MarketDetail,
        mad: &Option<MarketArchiveDetail>,
        cdr: &DateRange,
        tdr: &DateRange,
    ) -> Vec<ResearchCandle> {
        // For each day in the dr, read trades from file and create candles
        let trades = self.read_gdax_trades_for_dr(market, &TimeFrame::S15, &tdr.dts);
        // Make candles for the candle dr
        let last_trade = mad.as_ref().map(|m| m.last_as_pridti());
        ResearchCandle::from_trades_hm_for_dr(&trades, last_trade, &cdr.dts)
    }

    pub async fn make_research_candles_for_dt_from_file(
        &self,
        market: &MarketDetail,
        mcd: &Option<MarketCandleDetail>,
        dt: &DateTime<Utc>,
        pb: &PathBuf,
    ) -> Option<Vec<ResearchCandle>> {
        // Load trade for given date and market
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => None,
            ExchangeName::Gdax => {
                let (trades_vec, trades_hm) = self.read_gdax_trades_from_file(pb, &TimeFrame::S15);
                if trades_vec.is_empty() {
                    // Case where there are not trades for the day - create entire day of candles
                    // with the last trade of the mcd. If there is no mcd - there is no last trade
                    // and there should not be any candles returned
                    match mcd {
                        Some(m) => {
                            let last_trade = Some(PrIdTi {
                                dt: m.last_trade_ts,
                                id: m.last_trade_id.parse::<i64>().unwrap(),
                                price: m.last_trade_price,
                            });
                            let dr = self.create_date_range(
                                dt,
                                &(*dt + Duration::days(1)),
                                &TimeFrame::S15,
                            );
                            Some(ResearchCandle::from_trades_hm_for_dr(
                                &trades_hm, last_trade, &dr,
                            ))
                        }
                        None => None,
                    }
                } else {
                    let last_trade = mcd.as_ref().map(|m| PrIdTi {
                        dt: m.last_trade_ts,
                        id: m.last_trade_id.parse::<i64>().unwrap(),
                        price: m.last_trade_price,
                    });
                    let dr = self.create_candles_dr_for_dt(
                        &TimeFrame::S15,
                        dt,
                        last_trade,
                        trades_vec.first().unwrap(),
                    );
                    println!(
                        "Candle date range for {}: {} to {}",
                        dt,
                        dr.first().unwrap(),
                        dr.last().unwrap()
                    );
                    Some(ResearchCandle::from_trades_hm_for_dr(
                        &trades_hm, last_trade, &dr,
                    ))
                }
            }
        }
    }

    pub async fn make_production_candles_for_dt_from_table(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        last_trade: Option<PrIdTi>,
    ) -> Option<Vec<ProductionCandle>> {
        // Select trades for the given date
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                let trades = FtxTrade::select_all(&self.pools[&Database::Ftx], market, dt)
                    .await
                    .expect("Failed to select trades.");
                if trades.is_empty() {
                    // TODO: Handle case where there are no trades for the day (Kraken in Jan 2018)
                    None
                } else {
                    let dr = self.create_candles_dr_for_dt(
                        &market.tf,
                        dt,
                        last_trade,
                        trades.first().unwrap(),
                    );
                    println!(
                        "Candle Date Range for MTD Date {}: {} to {}",
                        dt,
                        dr.first().unwrap(),
                        dr.last().unwrap()
                    );
                    Some(ProductionCandle::from_trades_for_dr(
                        &trades, last_trade, &market.tf, &dr,
                    ))
                }
            }
            ExchangeName::Gdax => {
                let trades = GdaxTrade::select_all(&self.pools[&Database::Gdax], market, dt)
                    .await
                    .expect("Failed to select trades.");
                if trades.is_empty() {
                    // TODO: Handle case where there are no trades for the day (Kraken in Jan 2018)
                    None
                } else {
                    let dr = self.create_candles_dr_for_dt(
                        &market.tf,
                        dt,
                        last_trade,
                        trades.first().unwrap(),
                    );
                    println!(
                        "Candle Date Range for MTD Date {}: {} to {}",
                        dt,
                        dr.first().unwrap(),
                        dr.last().unwrap()
                    );
                    Some(ProductionCandle::from_trades_for_dr(
                        &trades, last_trade, &market.tf, &dr,
                    ))
                }
            }
        }
    }

    pub fn make_production_candles_for_dt_from_vec<T: Trade>(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        last_trade: &Option<PrIdTi>,
        interval_start: &DateTime<Utc>,
        interval_end: &DateTime<Utc>,
        trades: &[T],
    ) -> Option<Vec<ProductionCandle>> {
        let dr = if trades.is_empty() && (*interval_end - *interval_start) == Duration::days(1) {
            // Zero trades can exist in two scenarios -
            // 1) There are no trades in last timeframe - common as tf decreases
            // 2) There are no trades for the entire day - rare but exists in hist
            // Rare edge case where there are not trades on the day - can happen with
            // exchange outages like Kraken in Jan 2018 create a date range for day
            // so that 0 volume candles are created
            self.create_date_range(dt, &(*dt + Duration::days(1)), &market.tf)
        } else if trades.is_empty() {
            // Handle scenario 1 with no trades but interval is not 1 day
            self.create_date_range(interval_start, interval_end, &market.tf)
        } else {
            // Set last trade to start if start is None
            self.create_fill_candles_dr(
                market,
                last_trade,
                interval_start,
                interval_end,
                trades.first().unwrap(),
            )
        };
        if !dr.is_empty() {
            println!(
                "Creating candlesfrom {} through {}",
                dr.first().unwrap(),
                dr.last().unwrap()
            );
            // Create candles
            Some(ProductionCandle::from_trades_for_dr(
                trades,
                *last_trade,
                &market.tf,
                &dr,
            ))
        } else {
            println!("No candles to make. DR len 0");
            None
        }
    }

    pub async fn make_production_candles_for_interval(
        &self,
        market: &MarketDetail,
        dr: &DateRange,
        last_trade: &PrIdTi,
    ) -> Option<Vec<ProductionCandle>> {
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => None,
            ExchangeName::Gdax => {
                // Get the trades for the interval
                self.select_gdax_trades_for_interval(market, dr).await.map(
                    |trades|  // Make candles with the trades
                        ProductionCandle::from_trades_for_dr(
                            &trades,
                            Some(*last_trade),
                            &market.tf,
                            &dr.dts,
                        ),
                )
            }
        }
    }

    // Create a date range or the candles expected for the day of the given dt from the mtd. There
    // are two possibilities for the start of the dr. If the give last trade PrIdTi is None:
    // then there have been no candles found to start the sync and the trade
    // will be the first candles. The day itself may be the first day of the market so
    // it will not start at 00:00 UTC and will need to start when the first trade occurs, truncated
    // to the timeframe. For example, if the first trade is at 00:07:23 and the TimeFrame is T05.
    // Then the start of the date range will be 00:05:00 instead of 00:00:00 if there was a last
    // trade to go from.
    pub fn create_candles_dr_for_dt<T: Trade>(
        &self,
        tf: &TimeFrame,
        dt: &DateTime<Utc>,
        last_trade: Option<PrIdTi>,
        trade: &T,
    ) -> Vec<DateTime<Utc>> {
        // Make date range for candles for the day.
        let dr_start = match last_trade {
            // Last trade from last candle given, use the date given as the start
            Some(_) => *dt,
            // There have been no previous trades or candles loaded for sync. If there are
            // trades on the date given, the first trade is when the candles should start.
            None => trade.time().duration_trunc(tf.as_dur()).unwrap(),
        };
        self.create_date_range(&dr_start, &(*dt + Duration::days(1)), tf)
    }

    pub fn create_fill_candles_dr<T: Trade>(
        &self,
        market: &MarketDetail,
        last_trade: &Option<PrIdTi>,
        interval_start: &DateTime<Utc>,
        interval_end: &DateTime<Utc>,
        trade: &T,
    ) -> Vec<DateTime<Utc>> {
        // Make date range for candles for the day.
        let dr_start = match last_trade {
            // Last trade from last candle given, use the date given as the start
            Some(_) => *interval_start,
            // There have been no previous trades or candles loaded for sync. If there are
            // trades on the date given, the first trade is when the candles should start.
            None => trade.time().duration_trunc(market.tf.as_dur()).unwrap(),
        };
        let dr_end = interval_end.duration_trunc(market.tf.as_dur()).unwrap();
        self.create_date_range(&dr_start, &dr_end, &market.tf)
    }

    pub async fn insert_production_candles(
        &self,
        market: &MarketDetail,
        candles: &[ProductionCandle],
    ) {
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        for candle in candles.iter() {
            candle
                .insert(&self.pools[&db], market, &market.tf)
                .await
                .expect("Failed to insert candle.");
        }
    }

    pub async fn insert_research_candles(&self, market: &MarketDetail, candles: &[ResearchCandle]) {
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        for candle in candles.iter() {
            candle
                .insert(&self.pools[&db], market, &TimeFrame::S15)
                .await
                .expect("Failed to insert candle.");
        }
    }

    // Get the GDAX daily gdax candle for the market and date provided. If the date is not complete
    // then return None or if the API returns no candle (before market began, return none)
    pub async fn get_gdax_daily_candle(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Option<GdaxCandle> {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            match self.clients[&ExchangeName::Gdax]
                .get_gdax_candles::<GdaxCandle>(
                    &market.market_name,
                    Some(86400),
                    Some(*dt),
                    Some(*dt),
                )
                .await
            {
                Ok(result) => {
                    if !result.is_empty() {
                        return Some(result.first().expect("Expected first item.").clone());
                    } else {
                        return None;
                    }
                }
                Err(e) => {
                    if self.handle_candle_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Unhandled rest error: {:?}", e);
                    }
                }
            }
        }
    }

    pub async fn handle_candle_rest_error(&self, e: &RestError) -> bool {
        match e {
            RestError::Reqwest(e) => {
                if e.is_timeout() || e.is_connect() || e.is_request() {
                    println!("Timeout/Connect/Request Error. Retry in 30 secs. {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    true
                } else if e.is_status() {
                    match e.status() {
                        Some(s) => match s.as_u16() {
                            500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                // Server error, keep trying every 30 seconds
                                println!("{} status code. Retry in 30 secs. {:?}", s, e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                true
                            }
                            429 => {
                                // Too many requests, chill for 90 seconds
                                println!("{} status code. Retry in 90 secs. {:?}", s, e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(90)).await;
                                true
                            }
                            _ => {
                                println!("{} status code not handled. Panic.", s);
                                false
                            }
                        },
                        None => {
                            println!("No status code for request error.");
                            false
                        }
                    }
                } else {
                    println!("Other Reqwest Error. Panic.");
                    false
                }
            }
            _ => {
                println!("Other Rest Error, not Reqwest.");
                false
            }
        }
    }

    pub async fn select_first_production_candle_full_day(
        &self,
        market: &MarketDetail,
    ) -> Option<ProductionCandle> {
        // Check that candle table exists
        if self
            .candle_table_exists(market, &market.tf, &CandleType::Production)
            .await
        {
            // Get first production candle
            let db = match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
                ExchangeName::Gdax => Database::Gdax,
            };
            match ProductionCandle::select_first(&self.pools[&db], market).await {
                Ok(c) => {
                    // Validate candle is start of day and has volume
                    if c.datetime == c.datetime.duration_trunc(Duration::days(1)).unwrap() {
                        Some(c)
                    } else {
                        Some(
                            ProductionCandle::select_eq_dt(
                                &self.pools[&db],
                                market,
                                &(c.datetime.duration_trunc(Duration::days(1)).unwrap()
                                    + Duration::days(1)),
                            )
                            .await
                            .expect("Failed to select candle."),
                        )
                    }
                }
                Err(sqlx::Error::RowNotFound) => None,
                Err(e) => panic!("Other sqlx error: {:?}", e),
            }
        } else {
            // No production candles for market - return None
            None
        }
    }

    pub fn write_research_candles_to_file_for_month(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        candles: &[ResearchCandle],
    ) {
        // Create file path
        let pb = self.prep_candle_archive_path(market, dt, &TimeFrame::S15);
        // Create file and write candles
        println!("Writing S15 candles for the month.");
        self.write_research_candles_to_file(&pb, candles);
        let mut resampled_candles = candles.to_owned();
        // Resample and write for other time frames
        for tf in TimeFrame::all_time_frames().iter().skip(1) {
            resampled_candles = self.resample_research_candles_by_hashmap(&resampled_candles, tf);
            let pb = self.prep_candle_archive_path(market, dt, tf);
            println!(
                "Writing {} {} candles for the month.",
                resampled_candles.len(),
                tf.as_str()
            );
            self.write_research_candles_to_file(&pb, &resampled_candles);
        }
    }

    fn write_research_candles_to_file(&self, pb: &PathBuf, candles: &[ResearchCandle]) {
        let mut wtr = Writer::from_path(pb).expect("Failed to open file.");
        for candle in candles.iter() {
            wtr.serialize(candle).expect("Failed to serialize candle.");
        }
        wtr.flush().expect("Failed to flush wtr.");
    }

    fn prep_candle_archive_path(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        tf: &TimeFrame,
    ) -> PathBuf {
        println!("Created candle archive path for {}.", market.market_name);
        let path = format!(
            "{}/candles/{}/{}/{}",
            &self.storage_path,
            &market.exchange_name.as_str(),
            &market.as_strip(),
            dt.format("%Y"),
        );
        std::fs::create_dir_all(&path).expect("Failed to create directories.");
        let f = format!(
            "{}_{}_{}{}.csv",
            market.as_strip(),
            tf.as_str(),
            dt.format("%Y"),
            dt.format("%m"),
        );
        std::path::Path::new(&path).join(f)
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::candles::{DailyCandle, ProductionCandle, ResearchCandle, TimeFrame};
//     use crate::configuration::get_configuration;
//     use crate::eldorado::ElDorado;
//     use crate::exchanges::{ftx::Trade as FtxTrade, ExchangeName};
//     use crate::inquisidor::Inquisidor;
//     use crate::markets::{MarketCandleDetail, MarketDataStatus, MarketTradeDetail};
//     use crate::utilities::{create_date_range, next_month_datetime};
//     use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
//     use csv::{Reader, Writer};
//     use rust_decimal::prelude::*;
//     use rust_decimal_macros::dec;
//     use sqlx::PgPool;
//     use std::{fs::File, path::PathBuf};
//     use uuid::Uuid;

//     pub fn sample_trades() -> Vec<FtxTrade> {
//         let mut trades: Vec<FtxTrade> = Vec::new();
//         trades.push(FtxTrade {
//             id: 1,
//             price: Decimal::new(702, 1),
//             size: Decimal::new(23, 1),
//             side: "sell".to_string(),
//             liquidation: false,
//             time: Utc.timestamp(1524886322, 0),
//         });
//         trades.push(FtxTrade {
//             id: 2,
//             price: Decimal::new(752, 1),
//             size: Decimal::new(64, 1),
//             side: "buy".to_string(),
//             liquidation: false,
//             time: Utc.timestamp(1524887322, 0),
//         });
//         trades.push(FtxTrade {
//             id: 3,
//             price: Decimal::new(810, 1),
//             size: Decimal::new(4, 1),
//             side: "buy".to_string(),
//             liquidation: true,
//             time: Utc.timestamp(1524888322, 0),
//         });
//         trades.push(FtxTrade {
//             id: 4,
//             price: Decimal::new(767, 1),
//             size: Decimal::new(13, 1),
//             side: "sell".to_string(),
//             liquidation: false,
//             time: Utc.timestamp(1524889322, 0),
//         });
//         trades
//     }

//     pub async fn prep_ftx_file(path: &str, seed: i64, date: DateTime<Utc>) {
//         // Create trades
//         let mut trades = Vec::new();
//         for i in seed..30 {
//             let trade = create_ftx_trade(i, date);
//             trades.push(trade)
//         }
//         // Get path to save file
//         let f = format!("SOLPERP_{}.csv", date.format("%F"));
//         // Set archive file path
//         let archive_path = format!(
//             "{}/trades/ftx/SOLPERP/{}/{}",
//             path,
//             date.format("%Y"),
//             date.format("%m")
//         );
//         // Check directory is created
//         std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
//         // Set filepath
//         let fp = std::path::Path::new(&archive_path).join(f);
//         // Write trades to file
//         let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
//         for trade in trades.iter() {
//             wtr.serialize(trade).expect("Failed to serialize trade.");
//         }
//         wtr.flush().expect("Failed to flush wtr.");
//     }

//     pub fn create_ftx_trade(id: i64, date: DateTime<Utc>) -> FtxTrade {
//         let price = Decimal::new(id, 0) + dec!(100);
//         let size = Decimal::new(id, 0) * dec!(10);
//         FtxTrade {
//             id,
//             price,
//             size,
//             side: "buy".to_string(),
//             liquidation: false,
//             time: Utc
//                 .ymd(date.year(), date.month(), date.day())
//                 .and_hms(0, id as u32 % 30, 0),
//         }
//     }

//     pub fn read_sample_research_candles(pb: &PathBuf) -> Vec<ResearchCandle> {
//         let file = File::open(pb).expect("Failed to open file.");
//         let mut candles = Vec::new();
//         let mut rdr = Reader::from_reader(file);
//         for result in rdr.deserialize() {
//             let record: ResearchCandle = result.expect("Failed to deserialize record.");
//             candles.push(record);
//         }
//         candles
//     }

//     #[tokio::test]
//     pub async fn resample_and_convert_research_to_production_candles() {
//         let eld = ElDorado::new()
//             .await
//             .expect("Failed to create eldorado instance.");
//         let fp = std::path::Path::new("tests").join("FTTPERP_2022-01.csv");
//         println!("Loading 1 month of S15 candles.");
//         let start_0 = Utc::now();
//         let candles = read_sample_research_candles(&fp);
//         let end_0 = Utc::now();
//         println!("Candles loaded in {}", end_0 - start_0);
//         let tf = TimeFrame::T15;
//         println!("Candles Len: {}", candles.len());
//         println!("{} Starting Resample 1.", Utc::now());
//         let start_1 = Utc::now();
//         let resampled_1 = eld.resample_and_convert_research_candles(&candles, &tf);
//         let end_1 = Utc::now();
//         println!("{} Starting Resample 2.", Utc::now());
//         let start_2 = Utc::now();
//         let resampled_2 = eld.resample_and_convert_research_candles_by_hashmap(&candles, &tf);
//         let end_2 = Utc::now();
//         println!("{} Starting Resample 3.", Utc::now());
//         let start_3 = Utc::now();
//         let resampled_3 = eld.resample_and_convert_research_candles_by_hashmap_v2(&candles, &tf);
//         let end_3 = Utc::now();
//         println!("Resampled 1: {}", end_1 - start_1);
//         println!("Resampled 2: {}", end_2 - start_2);
//         println!("Resampled 3: {}", end_3 - start_3);
//         println!(
//             "First Candles:\n{:?}\n{:?}\n{:?}",
//             resampled_1.first(),
//             resampled_2.first(),
//             resampled_3.first()
//         );
//         println!(
//             "Last Candles:\n{:?}\n{:?}\n{:?}",
//             resampled_1.last(),
//             resampled_2.last(),
//             resampled_3.last()
//         );
//     }

//     #[test]
//     pub fn new_from_last_returns_candle_populated_from_last_trade() {
//         let market_id = Uuid::new_v4();
//         let mut trades = sample_trades();
//         let last_trade = trades.pop().unwrap();
//         let candle = ProductionCandle::new_from_last(
//             market_id,
//             last_trade.time,
//             last_trade.price,
//             last_trade.time,
//             &last_trade.id.to_string(),
//         );
//         println!("Candle: {:?}", candle);
//     }

//     #[test]
//     pub fn new_from_trades_returns_candle() {
//         let market_id = Uuid::new_v4();
//         let trades = sample_trades();
//         let first_trade = trades.first().unwrap();
//         let candle = ProductionCandle::new_from_trades(market_id, first_trade.time, &trades);
//         println!("Candle: {:?}", candle);
//     }

//     #[test]
//     pub fn new_from_trades_returns_research_candle() {
//         // Create trades
//         let dt = Utc.ymd(2022, 1, 1).and_hms(0, 0, 0);
//         let mut trades = Vec::new();
//         for i in 1..1000000 {
//             let trade = create_ftx_trade(i, dt);
//             trades.push(trade);
//         }
//         let mi = Uuid::new_v4();
//         // Start timer
//         let start_v1 = Utc::now();
//         let candle = ResearchCandle::new_from_trades(dt, &trades);
//         let end_v1 = Utc::now();
//         let start_v2 = Utc::now();
//         let candle_2 = ResearchCandle::new_from_trades_v2(dt, &trades);
//         let end_v2 = Utc::now();
//         let start_v3 = Utc::now();
//         let candle_3 = ProductionCandle::new_from_trades(mi, dt, &trades);
//         let end_v3 = Utc::now();
//         println!("Candle 1: {:?}", end_v1 - start_v1);
//         println!("Candle 2: {:?}", end_v2 - start_v2);
//         println!("Candle 2: {:?}", end_v3 - start_v3);
//         println!("Candle 1: {:?}", candle);
//         println!("Candle 2: {:?}", candle_2);
//         println!("Candle 3: {:?}", candle_3);
//     }

//     #[tokio::test]
//     pub async fn select_last_01d_candle_returns_none() {
//         // Load configuration
//         let configuration = get_configuration().expect("Failed to read configuration.");
//         println!("Configuration: {:?}", configuration);

//         // Create db connection
//         let pool = PgPool::connect_with(configuration.ftx_db.with_db())
//             .await
//             .expect("Failed to connect to Postgres.");

//         // Drop table if exists
//         let sql = "DROP TABLE IF EXISTS candle_01d_none";
//         sqlx::query(&sql)
//             .execute(&pool)
//             .await
//             .expect("Could not drop table.");

//         // Create table
//         let sql = r#"
//             CREATE TABLE IF NOT EXISTS candles_01d_none (
//                 datetime timestamptz NOT NULL,
//                 open NUMERIC NOT NULL,
//                 high NUMERIC NOT NULL,
//                 low NUMERIC NOT NULL,
//                 close NUMERIC NOT NULL,
//                 volume NUMERIC NOT NULL,
//                 volume_net NUMERIC NOT NULL,
//                 volume_liquidation NUMERIC NOT NULL,
//                 value NUMERIC NOT NULL,
//                 trade_count BIGINT NOT NULL,
//                 liquidation_count BIGINT NOT NULL,
//                 last_trade_ts timestamptz NOT NULL,
//                 last_trade_id TEXT NOT NULL,
//                 is_validated BOOLEAN NOT NULL,
//                 market_id uuid NOT NULL,
//                 first_trade_ts timestamptz NOT NULL,
//                 first_trade_id TEXT NOT NULL,
//                 is_archived BOOLEAN NOT NULL,
//                 PRIMARY KEY (datetime, market_id)
//             )
//             "#;
//         sqlx::query(&sql)
//             .execute(&pool)
//             .await
//             .expect("Could not create 01d candle table.");

//         // Select from empty table
//         let row = sqlx::query_as::<_, DailyCandle>(
//             r#"
//             SELECT * FROM candles_01d_none
//             ORDER BY datetime DESC
//             "#,
//         )
//         .fetch_one(&pool)
//         .await;
//         match row {
//             Ok(row) => {
//                 println!("Ok row: {:?}", row);
//                 panic!("Expected error!")
//             }
//             Err(e) => {
//                 println!("Err: {:?}", e)
//             }
//         }
//     }

//     #[tokio::test]
//     pub async fn select_last_01d_candles_returns_candle() {
//         // Load configuration
//         let configuration = get_configuration().expect("Failed to read configuration.");
//         println!("Configuration: {:?}", configuration);

//         // Create db connection
//         let _pool = PgPool::connect_with(configuration.ftx_db.with_db())
//             .await
//             .expect("Failed to connect to Postgres.");
//     }

//     #[tokio::test]
//     pub async fn resample_tests() {
//         println!("All candles resampled: {:?}", Utc::now());
//     }

//     #[tokio::test]
//     pub async fn make_candles_with_invalid_market_does_nothing_mtd() {
//         // Setup market with no mtd
//         let ig = Inquisidor::new().await;
//         // Update market to active with valid timestamp
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market trade details
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // New ig instance will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get test market
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         // Test no mtd returns false
//         assert!(
//             !ig.validate_market_eligibility_for_candles(market, &None)
//                 .await
//         );
//         // Create mtd
//         let mtd = MarketTradeDetail::select(&ig.ig_pool, &market)
//             .await
//             .expect("Failed to select mtd.");
//         // Test with mtd created but backfill not completed
//         assert!(
//             !ig.validate_market_eligibility_for_candles(market, &Some(mtd))
//                 .await
//         );
//         // Modify mtd
//         let mut mtd = MarketTradeDetail::select(&ig.ig_pool, &market)
//             .await
//             .expect("Failed to select mtd.");
//         mtd.previous_status = MarketDataStatus::Completed;
//         mtd.next_trade_day = Some(mtd.previous_trade_day);
//         println!("{:?}", mtd);
//         assert!(
//             !ig.validate_market_eligibility_for_candles(market, &Some(mtd))
//                 .await
//         );
//         // Modify mtd valid scenario
//         let mut mtd = MarketTradeDetail::select(&ig.ig_pool, &market)
//             .await
//             .expect("Failed to select mtd.");
//         mtd.first_trade_ts = mtd.first_trade_ts - Duration::days(2);
//         mtd.previous_status = MarketDataStatus::Completed;
//         mtd.next_trade_day = Some(mtd.previous_trade_day + Duration::days(2));
//         println!("{:?}", mtd);
//         assert!(
//             ig.validate_market_eligibility_for_candles(market, &Some(mtd))
//                 .await
//         );
//     }

//     #[tokio::test]
//     pub async fn make_candles_with_no_mcd_creates_mcd_and_first_candle() {
//         // Set up market and mtd
//         let ig = Inquisidor::new().await;
//         // Update market to active with valid timestamp
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market trade details
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market candle details
//         let sql = r#"
//             DELETE FROM market_candle_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // New ig instance will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get test market
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         // Get mtd
//         let mut mtd = MarketTradeDetail::select(&ig.ig_pool, &market)
//             .await
//             .expect("Failed to select mtd.");
//         // Modify mtd to for test - set next trade day - first trade ts
//         mtd.first_trade_ts = mtd.first_trade_ts - Duration::days(2);
//         mtd.previous_status = MarketDataStatus::Completed;
//         mtd.next_trade_day = Some(mtd.previous_trade_day + Duration::days(2));
//         println!("{:?}", mtd);
//         // Create trade files for 11/29 and 11/30
//         prep_ftx_file(&ig.settings.application.archive_path, 1, mtd.first_trade_ts).await;
//         prep_ftx_file(
//             &ig.settings.application.archive_path,
//             2,
//             mtd.first_trade_ts + Duration::days(1),
//         )
//         .await;
//         // Test
//         let mcd = ig.make_mcd_and_first_candle(market, &mtd).await;
//         println!("MCD: {:?}", mcd);
//         assert_eq!(mcd.first_candle, mtd.first_trade_ts);
//         assert_eq!(
//             mcd.last_candle,
//             next_month_datetime(mtd.first_trade_ts) - Duration::seconds(15)
//         );
//         // Assert runnign mack candles from last_candle does nothing further
//         let sql = r#"
//             UPDATE market_trade_details
//             SET next_trade_day = '2021-12-02'
//             WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ig.make_candles_from_last_candle(&market, &mcd).await;
//         // Get the mcd now and assert it is the same as before
//         let new_mcds = MarketCandleDetail::select_all(&ig.ig_pool)
//             .await
//             .expect("Failed to select all mcd.");
//         let new_mcd = new_mcds
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         assert_eq!(mcd.last_candle, new_mcd.last_candle);
//     }

//     #[tokio::test]
//     pub async fn make_candles_with_mcd_makes_until_last_full_month() {
//         // Set up market and mtd
//         let ig = Inquisidor::new().await;
//         // Update market to active with valid timestamp
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market trade details
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market candle details
//         let sql = r#"
//             DELETE FROM market_candle_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // New ig instance will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get test market
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         // Set up mtd details
//         let _mtd = MarketTradeDetail::select(&ig.ig_pool, &market)
//             .await
//             .expect("Failed to select mtd.");
//         let sql = r#"
//             UPDATE market_trade_details
//             SET next_trade_day = '2022-02-02'
//             WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Set up mcd details
//         let sql = r#"
//             INSERT INTO market_candle_details
//             VALUES ('19994c6a-fa3c-4b0b-96c4-c744c43a9514', 'ftx', 'SOL-PERP', 's15', '2021-11-29',
//                 '2021-11-30 23:59:45', '2021-11-30 00:29:00', '29', 129);
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Now add trade files for the next 3 months
//         let months = [
//             Utc.ymd(2021, 12, 1).and_hms(0, 0, 0),
//             Utc.ymd(2022, 1, 1).and_hms(0, 0, 0),
//         ];
//         for month in months.iter() {
//             // Create date range for month
//             let dr = create_date_range(*month, next_month_datetime(*month), Duration::days(1));
//             for d in dr.iter() {
//                 prep_ftx_file(&ig.settings.application.archive_path, d.day() as i64, *d).await;
//             }
//         }
//         // Get the mcd
//         let mcds = MarketCandleDetail::select_all(&ig.ig_pool)
//             .await
//             .expect("Failed to select all mcd.");
//         let mcd = mcds.iter().find(|m| m.market_name == "SOL-PERP").unwrap();
//         println!("{:?}", mcd);
//         // Run the function
//         ig.make_candles_from_last_candle(&market, &mcd).await;
//         // Assert
//     }

//     #[tokio::test]
//     pub async fn check_mcd_for_months_to_load_tests() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mcd = MarketCandleDetail {
//             market_id: Uuid::new_v4(),
//             exchange_name: ExchangeName::Ftx,
//             market_name: "SOL-PERP".to_string(),
//             time_frame: TimeFrame::S15,
//             first_candle: Utc.ymd(2022, 3, 15).and_hms(4, 30, 15),
//             last_candle: Utc.ymd(2022, 9, 30).and_hms(23, 59, 45),
//             last_trade_ts: Utc.ymd(2022, 9, 30).and_hms(23, 59, 55),
//             last_trade_id: "1234".to_string(),
//             last_trade_price: dec!(123.0),
//         };
//         let _table = "candles_ftx_solperp_s15";
//         let _schema = "archive";
//         let drop_sql = r#"
//             DROP TABLE IF EXISTS archive.candles_ftx_solperp_s15
//         "#;
//         let insert_sql = r#"
//             INSERT INTO archive.candles_ftx_solperp_s15
//             VALUES ('2022-03-31 23:59:45', 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
//                 1.0, 1.0, 1.0, 1.0, 1.0, 10, 10, 10, 10, 10, 10, '2022-03-31 23:59:59', '1234',
//                 '2022-03-31 23:59:46', '4321')
//         "#;

//         // Archive table exists with candles & no next month in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ResearchCandle::create_table(&ig.archive_pool, &market, &mcd.time_frame)
//             .await
//             .expect("Failed to create table.");
//         sqlx::query(insert_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         mcd.last_candle = Utc.ymd(2022, 4, 10).and_hms(23, 59, 30);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(dr.is_empty());

//         // Archive table exists with candles & there are new months in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ResearchCandle::create_table(&ig.archive_pool, &market, &mcd.time_frame)
//             .await
//             .expect("Failed to create table.");
//         sqlx::query(insert_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         mcd.last_candle = Utc.ymd(2022, 6, 10).and_hms(23, 59, 30);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(!dr.is_empty());
//         assert_eq!(dr.len(), 2);

//         // Archive table exists without candles & no new months in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ResearchCandle::create_table(&ig.archive_pool, &market, &mcd.time_frame)
//             .await
//             .expect("Failed to create table.");
//         mcd.first_candle = Utc.ymd(2022, 9, 15).and_hms(4, 30, 15);
//         mcd.last_candle = Utc.ymd(2022, 9, 30).and_hms(23, 59, 30);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(dr.is_empty());

//         // Archive table exists without candles & there are new months in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ResearchCandle::create_table(&ig.archive_pool, &market, &mcd.time_frame)
//             .await
//             .expect("Failed to create table.");
//         mcd.first_candle = Utc.ymd(2022, 9, 15).and_hms(4, 30, 15);
//         mcd.last_candle = Utc.ymd(2022, 11, 14).and_hms(23, 59, 45);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(!dr.is_empty());
//         assert_eq!(dr.len(), 2);

//         // Archive table does not exists & there are no months in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         mcd.first_candle = Utc.ymd(2022, 9, 15).and_hms(4, 30, 15);
//         mcd.last_candle = Utc.ymd(2022, 9, 30).and_hms(23, 59, 30);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(dr.is_empty());

//         // Archive table does not exist & there are new months in mcd
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         mcd.first_candle = Utc.ymd(2022, 9, 15).and_hms(4, 30, 15);
//         mcd.last_candle = Utc.ymd(2022, 9, 30).and_hms(23, 59, 45);
//         let dr = ig.check_mcd_for_months_to_load(&mcd).await;
//         println!("{:?}", dr);
//         assert!(!dr.is_empty());
//         assert_eq!(dr.len(), 1);
//     }

//     #[tokio::test]
//     pub async fn read_candle_and_insert() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mcd = MarketCandleDetail {
//             market_id: Uuid::new_v4(),
//             exchange_name: ExchangeName::Ftx,
//             market_name: "SOL-PERP".to_string(),
//             time_frame: TimeFrame::S15,
//             first_candle: Utc.ymd(2022, 3, 15).and_hms(4, 30, 15),
//             last_candle: Utc.ymd(2022, 9, 30).and_hms(23, 59, 45),
//             last_trade_ts: Utc.ymd(2022, 9, 30).and_hms(23, 59, 55),
//             last_trade_id: "1234".to_string(),
//             last_trade_price: dec!(123.0),
//         };
//         let _table = "candles_ftx_solperp_s15";
//         let _schema = "archive";
//         let drop_sql = r#"
//             DROP TABLE IF EXISTS archive.candles_ftx_solperp_s15
//         "#;
//         sqlx::query(drop_sql)
//             .execute(&ig.archive_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         ResearchCandle::create_table(&ig.archive_pool, &market, &mcd.time_frame)
//             .await
//             .expect("Failed to create table.");
//         let dr = vec![Utc.ymd(2022, 3, 1).and_hms(0, 0, 0)];
//         let mut candles = Vec::new();
//         for i in 1..30 {
//             let candle = ResearchCandle {
//                 datetime: Utc.ymd(2022, 3, i).and_hms(0, 0, 0),
//                 open: dec!(0),
//                 high: dec!(0),
//                 low: dec!(0),
//                 close: dec!(0),
//                 volume: dec!(0),
//                 volume_buy: dec!(0),
//                 volume_sell: dec!(0),
//                 volume_liq: dec!(0),
//                 volume_liq_buy: dec!(0),
//                 volume_liq_sell: dec!(0),
//                 value: dec!(0),
//                 value_buy: dec!(0),
//                 value_sell: dec!(0),
//                 value_liq: dec!(0),
//                 value_liq_buy: dec!(0),
//                 value_liq_sell: dec!(0),
//                 trade_count: 1,
//                 trade_count_buy: 1,
//                 trade_count_sell: 1,
//                 liq_count: 1,
//                 liq_count_buy: 1,
//                 liq_count_sell: 1,
//                 last_trade_ts: Utc.ymd(2022, 3, 1).and_hms(0, 0, 0),
//                 last_trade_id: "1234".to_string(),
//                 first_trade_ts: Utc.ymd(2022, 3, 1).and_hms(0, 0, 0),
//                 first_trade_id: "1234".to_string(),
//             };
//             candles.push(candle);
//         }
//         let f = "SOLPERP_2022-03.csv";
//         let p = format!(
//             "{}/candles/ftx/SOLPERP/2022",
//             ig.settings.application.archive_path
//         );
//         std::fs::create_dir_all(&p).expect("Failed to create directories.");
//         let fp = std::path::Path::new(&p).join(f);
//         // Write trades to file
//         let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
//         for candle in candles.iter() {
//             wtr.serialize(candle).expect("Failed to serialize trade.");
//         }
//         wtr.flush().expect("Failed to flush wtr.");
//         // Run
//         println!("Loading {:?} for {:?}", dr, mcd.market_name);
//         ig.load_candles_for_months(&mcd, &dr).await;
//         // Assert
//     }
// }
