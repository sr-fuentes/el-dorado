use crate::exchanges::{client::RestClient, error::RestError, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{MarketCandleDetail, MarketDetail, MarketTradeDetail};
use crate::mita::Mita;
use crate::trades::*;
use crate::utilities::{
    create_date_range, create_monthly_date_range, next_month_datetime, trunc_month_datetime,
    TimeFrame, Trade,
};
use crate::validation::insert_candle_validation;
use chrono::{DateTime, Duration, DurationRound, Utc};
use csv::Writer;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
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
    pub market_id: Uuid,
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
    pub fn new_from_trades<T: Trade>(
        market_id: Uuid,
        datetime: DateTime<Utc>,
        trades: &[T],
    ) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No first trade for candle.").price(), // open
                Decimal::MIN,                                                // high
                Decimal::MAX,                                                // low
                dec!(0),                                                     // close
                dec!(0),                                                     // volume
                dec!(0),                                                     // volume_net
                dec!(0),                                                     // volume_liquidation
                dec!(0),                                                     // value
                0,                                                           // count
                0,                                                           // liquidation_count,
                datetime,                                                    // last_trade_ts
                "".to_string(),                                              // last_trade_id
                trades.first().expect("No first trade.").time(),             // first_trade_ts
                trades
                    .first()
                    .expect("No first trade.")
                    .trade_id()
                    .to_string(), // first_trade_id
            ),
            |(o, h, l, _c, v, vn, vl, a, n, ln, _ts, _id, fts, fid), t| {
                (
                    o,
                    h.max(t.price()),
                    l.min(t.price()),
                    t.price(),
                    v + t.size(),
                    if t.side() == "sell" {
                        vn + (t.size() * dec!(-1))
                    } else {
                        vn + t.size()
                    },
                    if t.liquidation() { vl + t.size() } else { vl },
                    a + (t.size() * t.price()),
                    n + 1,
                    if t.liquidation() { ln + 1 } else { ln },
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
            market_id,
        }
    }

    // Takes a Vec of Candles and resamples into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from candes in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close may
    // be incorrect.
    pub fn new_from_candles(market_id: Uuid, datetime: DateTime<Utc>, candles: &[Candle]) -> Self {
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
            market_id,
        }
    }

    // This function will build a placeholder trade with 0 volume and
    // will populate OHLC from the last trade provided.
    pub fn new_from_last(
        market_id: Uuid,
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
            market_id,
        }
    }
}

impl ResearchCandle {
    // Takes a Vec of Trade and aggregates into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from trades in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close / Datetime may
    // be incorrect.
    pub fn new_from_trades<T: Trade>(datetime: DateTime<Utc>, trades: &[T]) -> Self {
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
                (
                    o,                // open
                    h.max(t.price()), // high
                    l.min(t.price()), // low
                    t.price(),        // close
                    v + t.size(),     // volume
                    if t.side() == "buy" { vb + t.size() } else { vb },
                    if t.side() == "sell" {
                        vs + t.size()
                    } else {
                        vs
                    },
                    if t.liquidation() { vl + t.size() } else { vl },
                    if t.liquidation() && t.side() == "buy" {
                        vlb + t.size()
                    } else {
                        vlb
                    },
                    if t.liquidation() && t.side() == "sell" {
                        vls + t.size()
                    } else {
                        vls
                    },
                    u + (t.size() * t.price()),
                    if t.side() == "buy" {
                        ub + (t.size() * t.price())
                    } else {
                        ub
                    },
                    if t.side() == "sell" {
                        us + (t.size() * t.price())
                    } else {
                        us
                    },
                    if t.liquidation() {
                        al + (t.size() * t.price())
                    } else {
                        al
                    },
                    if t.liquidation() && t.side() == "buy" {
                        alb + (t.size() * t.price())
                    } else {
                        alb
                    },
                    if t.liquidation() && t.side() == "sell" {
                        als + (t.size() * t.price())
                    } else {
                        als
                    },
                    n + 1,
                    if t.side() == "buy" { nb + 1 } else { nb },
                    if t.side() == "sell" { ns + 1 } else { ns },
                    if t.liquidation() { ln + 1 } else { ln },
                    if t.liquidation() && t.side() == "buy" {
                        lnb + 1
                    } else {
                        lnb
                    },
                    if t.liquidation() && t.side() == "sell" {
                        lns + 1
                    } else {
                        lns
                    },
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

    // Reduces the number of if statements in each iteration
    pub fn new_from_trades_v2<T: Trade>(datetime: DateTime<Utc>, trades: &[T]) -> Self {
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

    // // Takes a Vec of Candles and resamples into a Candle with the Datetime = the
    // // datetime passed as argument. Candle built from candes in the order they are in
    // // the Vec, sort before calling this function otherwise Open / Close may
    // // be incorrect.
    // pub fn new_from_candles(market_id: Uuid, datetime: DateTime<Utc>, candles: &[Candle]) -> Self {
    //     let candle_tuple = candles.iter().fold(
    //         (
    //             candles.first().expect("No first trade for candle.").open, // open
    //             Decimal::MIN,                                              // high
    //             Decimal::MAX,                                              // low
    //             dec!(0),                                                   // close
    //             dec!(0),                                                   // volume
    //             dec!(0),                                                   // volume_net
    //             dec!(0),                                                   // volume_liquidation
    //             dec!(0),                                                   // value
    //             0,                                                         // count
    //             0,                                                         // liquidation_count,
    //             datetime,                                                  // last_trade_ts
    //             "".to_string(),                                            // last_trade_id
    //             candles.first().expect("No first trade.").first_trade_ts,  // first_trade_ts
    //             candles
    //                 .first()
    //                 .expect("No first trade.")
    //                 .first_trade_id
    //                 .to_string(), // first_trade_id
    //         ),
    //         |(o, h, l, _c, v, vn, vl, a, n, ln, _ts, _id, fts, fid), c| {
    //             (
    //                 o,
    //                 h.max(c.high),
    //                 l.min(c.low),
    //                 c.close,
    //                 v + c.volume,
    //                 vn + c.volume_net,
    //                 vl + c.volume_liquidation,
    //                 a + c.value,
    //                 n + c.trade_count,
    //                 ln + c.liquidation_count,
    //                 c.last_trade_ts,
    //                 c.last_trade_id.to_string(),
    //                 fts,
    //                 fid,
    //             )
    //         },
    //     );
    //     Self {
    //         datetime,
    //         open: candle_tuple.0,
    //         high: candle_tuple.1,
    //         low: candle_tuple.2,
    //         close: candle_tuple.3,
    //         volume: candle_tuple.4,
    //         volume_net: candle_tuple.5,
    //         volume_liquidation: candle_tuple.6,
    //         value: candle_tuple.7,
    //         trade_count: candle_tuple.8,
    //         liquidation_count: candle_tuple.9,
    //         last_trade_ts: candle_tuple.10,
    //         last_trade_id: candle_tuple.11,
    //         first_trade_ts: candle_tuple.12,
    //         first_trade_id: candle_tuple.13,
    //         is_validated: false,
    //         market_id,
    //     }
    // }

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
            last_trade_ts,
            last_trade_id: last_trade_id.to_string(),
            first_trade_ts: last_trade_ts,
            first_trade_id: last_trade_id.to_string(),
        }
    }
}

impl Mita {
    pub async fn validate_candles<T: crate::utilities::Candle + DeserializeOwned>(
        &self,
        client: &RestClient,
        market: &MarketDetail,
    ) {
        let unvalidated_candles = select_unvalidated_candles(
            &self.ed_pool,
            &market.exchange_name,
            &market.market_id,
            TimeFrame::T15,
        )
        .await
        .expect("Could not fetch unvalidated candles.");
        // Validate heartbeat candles
        validate_hb_candles::<T>(
            &self.ed_pool,
            &self.trade_pool,
            client,
            &self.exchange.name,
            market,
            &unvalidated_candles,
        )
        .await;
        // Create 01d candles
        create_01d_candles(&self.ed_pool, &self.exchange.name, &market.market_id).await;
        // Validate 01d candles
        validate_01d_candles::<T>(
            &self.ed_pool,
            &self.trade_pool,
            client,
            &self.exchange.name,
            market,
        )
        .await;
    }

    pub async fn create_interval_candles<T: Trade + std::clone::Clone>(
        &self,
        market: &MarketDetail,
        date_range: Vec<DateTime<Utc>>,
        trades: &[T],
    ) -> Vec<Candle> {
        // Takes a vec of trades and a date range and builds candles for each date in the range
        // Get previous candle - to be used to forward fill if there are no trades
        let mut previous_candle = match select_previous_candle(
            &self.ed_pool,
            &self.exchange.name,
            &market.market_id,
            *date_range.first().unwrap(),
            self.hbtf,
        )
        .await
        {
            Ok(c) => Some(c),
            Err(sqlx::Error::RowNotFound) => None,
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };
        let candles = date_range.iter().fold(Vec::new(), |mut v, d| {
            let mut filtered_trades: Vec<T> = trades
                .iter()
                .filter(|t| t.time().duration_trunc(Duration::seconds(900)).unwrap() == *d)
                .cloned()
                .collect();
            let new_candle = match filtered_trades.len() {
                0 => previous_candle.as_ref().map(|pc| {
                    Candle::new_from_last(
                        market.market_id,
                        *d,
                        pc.close,
                        pc.last_trade_ts,
                        &pc.last_trade_id.to_string(),
                    )
                }),
                _ => {
                    filtered_trades.sort_by_key(|t1| t1.trade_id());
                    Some(Candle::new_from_trades(
                        market.market_id,
                        *d,
                        &filtered_trades,
                    ))
                }
            };
            previous_candle = new_candle.clone();
            v.push(new_candle);
            v
        });
        candles.into_iter().flatten().collect()
    }

    pub async fn insert_candles(&self, market: &MarketDetail, candles: Vec<Candle>) {
        for candle in candles.into_iter() {
            insert_candle(
                &self.ed_pool,
                &self.exchange.name,
                &market.market_id,
                candle,
                false,
            )
            .await
            .expect("Could not insert new candle.");
        }
    }
}

impl Inquisidor {
    pub async fn make_candles(&self) {
        // Aggregate trades for a market into timeframe buckets. Track the latest trade details to
        // determine if the month has been completed and can be aggregated
        // Get market to aggregate trades
        let market = match self.get_valid_market().await {
            Some(m) => m,
            None => return,
        };
        // Get the market candle detail
        let mcd = self.get_market_candle_detail(&market).await;
        match mcd {
            Some(m) => {
                // Start from last candle
                self.make_candles_from_last_candle(&market, &m).await;
            }
            None => {
                // Create mcd and start from first trade month
                let mcd = self.validate_and_make_mcd_and_first_candle(&market).await;
                if mcd.is_some() {
                    self.make_candles_from_last_candle(&market, &mcd.unwrap())
                        .await;
                };
            }
        }
    }

    pub async fn validate_market_eligibility_for_candles(
        &self,
        market: &MarketDetail,
        mtd: &Option<MarketTradeDetail>,
    ) -> bool {
        // Check there is a market trade detail
        match mtd {
            Some(mtd) => {
                // Check there are trades validated and archived
                if mtd.next_trade_day.is_none() {
                    println!(
                        "{:?} market has not completed backfill. Cannot make candles.",
                        market.market_name
                    );
                    return false;
                };
                // Check that there are at least first month of trades to make into candles
                if mtd.next_trade_day.unwrap() <= next_month_datetime(mtd.first_trade_ts) {
                    println!(
                        "Less than one full months of trades for {:?}. Cannot make candles.",
                        market.market_name
                    );
                    false
                } else {
                    true
                }
            }
            None => {
                println!(
                    "Market trade detail does not exist for {:?}. Cannot make candes.",
                    market.market_name
                );
                false
            }
        }
    }

    pub async fn validate_and_make_mcd_and_first_candle(
        &self,
        market: &MarketDetail,
    ) -> Option<MarketCandleDetail> {
        // Get the market trade details and check if one exists for market
        let market_trade_details = MarketTradeDetail::select_all(&self.ig_pool)
            .await
            .expect("Failed to select market trade details.");
        let mtd = market_trade_details
            .iter()
            .find(|mtd| mtd.market_id == market.market_id)
            .cloned();
        // Validate that the market is in a state to make candles - ie trades have been
        // archived, a market_trade_detail record exists, and that there are trades through
        // the end of the first month
        if !self
            .validate_market_eligibility_for_candles(market, &mtd)
            .await
        {
            return None;
        };
        // Safe to unwrap as None would fail eligibility
        let mtd = mtd.unwrap();
        Some(self.make_mcd_and_first_candle(market, &mtd).await)
    }

    pub async fn make_mcd_and_first_candle(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
    ) -> MarketCandleDetail {
        // Create candle daterange from first trade to end of first month
        let candle_dr = create_date_range(
            mtd.first_trade_ts
                .duration_trunc(TimeFrame::S15.as_dur())
                .unwrap(),
            next_month_datetime(mtd.first_trade_ts),
            TimeFrame::S15.as_dur(),
        );
        // Create trade file daterange
        let trades_dr = create_date_range(
            mtd.first_trade_ts
                .duration_trunc(Duration::days(1))
                .unwrap(),
            next_month_datetime(mtd.first_trade_ts),
            Duration::days(1),
        );
        // Make the candle and write to file
        let candles = self
            .make_candle_for_month(market, &None, &candle_dr, &trades_dr)
            .await;
        // Create the mcd
        let mcd = MarketCandleDetail {
            market_id: market.market_id,
            exchange_name: market.exchange_name,
            market_name: market.market_name.clone(),
            time_frame: TimeFrame::S15,
            first_candle: candles.first().unwrap().datetime,
            last_candle: candles.last().unwrap().datetime,
            last_trade_ts: candles.last().unwrap().last_trade_ts,
            last_trade_id: candles.last().unwrap().last_trade_id.clone(),
            last_trade_price: candles.last().unwrap().close,
        };
        // Insert the mcd
        mcd.insert(&self.ig_pool)
            .await
            .expect("Failed to insert market candle detail.");
        // Return the mcd
        mcd
    }

    pub async fn make_candle_for_month(
        &self,
        market: &MarketDetail,
        mcd: &Option<MarketCandleDetail>,
        candle_dr: &[DateTime<Utc>],
        trades_dr: &[DateTime<Utc>],
    ) -> Vec<ResearchCandle> {
        // Load the trades
        let trades = self.load_trades_for_dr(market, trades_dr).await;
        // Create the first months candles
        let candles = self.make_candles_for_dr(mcd, candle_dr, &trades).await;
        // Write candles to file
        let f = format!(
            "{}_{}-{}.csv",
            market.as_strip(),
            candle_dr.first().unwrap().format("%Y"),
            candle_dr.first().unwrap().format("%m")
        );
        let archive_path = format!(
            "{}/candles/{}/{}/{}",
            &self.settings.application.archive_path,
            &market.exchange_name.as_str(),
            &market.as_strip(),
            candle_dr.first().unwrap().format("%Y"),
        );
        // Create directories if not exists
        std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
        let c_path = std::path::Path::new(&archive_path).join(f);
        // Write trades to file
        let mut wtr = Writer::from_path(c_path).expect("Failed to open file.");
        for candle in candles.iter() {
            wtr.serialize(candle).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
        candles
    }

    pub async fn make_candles_for_dr<T: Trade + std::clone::Clone>(
        &self,
        mcd: &Option<MarketCandleDetail>,
        dr: &[DateTime<Utc>],
        trades: &[T],
    ) -> Vec<ResearchCandle> {
        let (mut last_price, mut last_id, mut last_ts) = match mcd {
            Some(mcd) => (
                mcd.last_trade_price,
                mcd.last_trade_id.clone(),
                mcd.last_trade_ts,
            ),
            None => (dec!(-1), String::new(), Utc::now()),
        };
        let candles = dr.iter().fold(Vec::new(), |mut v, d| {
            let mut filtered_trades: Vec<T> = trades
                .iter()
                .filter(|t| t.time().duration_trunc(TimeFrame::S15.as_dur()).unwrap() == *d)
                .cloned()
                .collect();
            let new_candle = match filtered_trades.len() {
                0 => ResearchCandle::new_from_last(*d, last_price, last_ts, &last_id),
                _ => {
                    filtered_trades.sort_by_key(|t1| t1.trade_id());
                    ResearchCandle::new_from_trades_v2(*d, &filtered_trades)
                }
            };
            last_price = new_candle.close;
            last_ts = new_candle.last_trade_ts;
            last_id = new_candle.last_trade_id.clone();
            v.push(new_candle);
            v
        });
        candles
    }

    pub async fn make_candles_from_last_candle(
        &self,
        market: &MarketDetail,
        mcd: &MarketCandleDetail,
    ) {
        // Get mtd for the market - it must exist to have a mcd existing
        let mtd = self.get_market_trade_detail(market).await;
        // Get month date range for candle build
        let months_dr = create_monthly_date_range(
            mcd.last_candle + mcd.time_frame.as_dur(),
            trunc_month_datetime(mtd.next_trade_day.unwrap() - Duration::days(1)),
        );
        // Get mcd for modification
        let mut mcd = mcd.clone();
        println!("Months dr for candles: {:?}", months_dr);
        // For each month, make candles and update the mcd
        for month in months_dr.iter() {
            let candle_dr =
                create_date_range(*month, next_month_datetime(*month), mcd.time_frame.as_dur());
            let trades_dr =
                create_date_range(*month, next_month_datetime(*month), Duration::days(1));
            let candles = self
                .make_candle_for_month(market, &Some(mcd.clone()), &candle_dr, &trades_dr)
                .await;
            // Update the mcd
            if !candles.is_empty() {
                mcd = mcd
                    .update_last(&self.ig_pool, candles.last().unwrap())
                    .await
                    .expect("Failed to update last market candle detail.");
            }
        }
    }

    pub async fn get_market_candle_detail(
        &self,
        market: &MarketDetail,
    ) -> Option<MarketCandleDetail> {
        let market_candle_details = MarketCandleDetail::select_all(&self.ig_pool)
            .await
            .expect("Failed to select market candle details.");
        market_candle_details
            .iter()
            .find(|m| m.market_id == market.market_id)
            .cloned()
    }
}

pub async fn create_exchange_candle_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<(), sqlx::Error> {
    // Create candles table for exchange
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS candles_15t_{} (
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
        exchange_name.as_str()
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub fn resample_candles(market_id: Uuid, candles: &[Candle], duration: Duration) -> Vec<Candle> {
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
            let mut dr_start = floor_start;
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
                let resampled_candle = Candle::new_from_candles(market_id, *d, &filtered_candles);
                v.push(resampled_candle);
                v
            });
            resampled_candles
        }
    }
}

pub async fn create_01d_candles(pool: &PgPool, exchange_name: &ExchangeName, market_id: &Uuid) {
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
    if candles.is_empty() {
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
    let resampled_candles = resample_candles(*market_id, &filtered_candles, Duration::days(1));

    // If there are no resampled candles, then return
    if resampled_candles.is_empty() {
        return;
    };

    // Insert 01D candles
    insert_candles_01d(pool, market_id, &resampled_candles, false)
        .await
        .expect("Could not insert candles.");
}

pub async fn validate_hb_candles<T: crate::utilities::Candle + DeserializeOwned>(
    pool: &PgPool,
    trade_pool: &PgPool,
    client: &RestClient,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    unvalidated_candles: &[Candle],
) {
    if unvalidated_candles.is_empty() {
        return;
    };
    // Safe to call unwrap() as there is at least one candle based on above return check
    let first_candle = unvalidated_candles.first().unwrap().datetime;
    let last_candle = unvalidated_candles.last().unwrap().datetime;
    // Match exchange because the exchange candles will be in different formats
    let mut exchange_candles: Vec<T> = match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            get_ftx_candles_daterange(client, market, first_candle, last_candle, 900).await
        }
        ExchangeName::Gdax => {
            get_gdax_candles_daterange(client, market, first_candle, last_candle, 900).await
        }
    };
    for unvalidated_candle in unvalidated_candles.iter() {
        println!(
            "Validating {} candle {}.",
            &market.market_name, unvalidated_candle.datetime
        );
        let is_valid = match exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                validate_ftx_candle(unvalidated_candle, &mut exchange_candles)
            }
            ExchangeName::Gdax => {
                validate_gdax_candle_by_volume(unvalidated_candle, &mut exchange_candles)
            }
        };
        process_validation_result(
            pool,
            trade_pool,
            exchange_name,
            market,
            unvalidated_candle,
            is_valid,
        )
        .await;
    }
}

pub async fn process_validation_result(
    pool: &PgPool,
    trade_pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    unvalidated_candle: &Candle,
    is_valid: bool,
) {
    if is_valid {
        update_candle_validation(
            pool,
            exchange_name,
            &market.market_id,
            unvalidated_candle,
            TimeFrame::T15, // REMOVE HARDCODED TF
        )
        .await
        .expect("Could not update candle validation status.");
        // If there are trades (volume > 0) then move from processed to validated
        if unvalidated_candle.volume > dec!(0) {
            // Update validated trades and move from processed to validated
            select_insert_delete_trades(
                trade_pool,
                exchange_name,
                market,
                unvalidated_candle.datetime,
                unvalidated_candle.datetime + TimeFrame::T15.as_dur(), // REMOVE HARDCODED TF
                "processed",
                "validated",
            )
            .await
            .expect("Failed to select insert delete trades.");
        }
    } else {
        // Add to candle validation table
        println!(
            "Candle not validated adding to validation table: {} \t {}",
            &market.market_name, unvalidated_candle.datetime
        );
        insert_candle_validation(
            pool,
            exchange_name,
            &market.market_id,
            &unvalidated_candle.datetime,
            900, // REMOVE HARDCODED TF
        )
        .await
        .expect("Failed to insert candle validation.");
    };
}

pub async fn validate_01d_candles<T: crate::utilities::Candle + DeserializeOwned>(
    eld_pool: &PgPool,
    trade_pool: &PgPool,
    client: &RestClient,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) {
    // Get unvalidated 01d candles
    let unvalidated_candles = match select_unvalidated_candles(
        eld_pool,
        exchange_name,
        &market.market_id,
        TimeFrame::D01,
    )
    .await
    {
        Ok(c) => c,
        Err(sqlx::Error::RowNotFound) => return,
        Err(e) => panic!("Sqlx Error: {:?}", e),
    };
    // println!("Unvalidated 01D candles: {:?}", unvalidated_candles);
    // If no candles returned from query - return function
    if unvalidated_candles.is_empty() {
        return;
    };
    // Get exchange candles for validation. unwrap() safe as there must be at least 1 candle
    let first_candle = unvalidated_candles.first().unwrap().datetime;
    let last_candle = unvalidated_candles.last().unwrap().datetime;
    let mut exchange_candles: Vec<T> = match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            get_ftx_candles_daterange(client, market, first_candle, last_candle, 86400).await
        }
        ExchangeName::Gdax => {
            get_gdax_candles_daterange(client, market, first_candle, last_candle, 86400).await
        }
    };
    println!("Pulled {} candles from exchange.", exchange_candles.len());

    // Get 15T candles to compare
    let hb_candles = select_candles_by_daterange(
        eld_pool,
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
            "Validating {} 01d candle {}.",
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
        // Check if candle is valid
        // a value of None means the validation could not take place (REST error or something)
        let daily_is_validated = match exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                Some(validate_ftx_candle(candle, &mut exchange_candles))
            }
            ExchangeName::Gdax => {
                validate_gdax_candle_by_trade_ids(
                    trade_pool,
                    client,
                    market,
                    candle,
                    &mut exchange_candles,
                    &TimeFrame::D01,
                    "validated",
                )
                .await
            }
        };
        // Updated candle validation status
        match daily_is_validated {
            Some(v) => {
                if hb_is_validated && v {
                    update_candle_validation(
                        eld_pool,
                        exchange_name,
                        &market.market_id,
                        candle,
                        TimeFrame::D01,
                    )
                    .await
                    .expect("Could not update candle validation status.");
                } else {
                    println!(
                        "{:?} 01d not validated adding to validation table. HB={}, 01D={}",
                        candle.datetime, hb_is_validated, v
                    );
                    insert_candle_validation(
                        eld_pool,
                        exchange_name,
                        &market.market_id,
                        &candle.datetime,
                        86400,
                    )
                    .await
                    .expect("Failed to insert candle validation.");
                }
            }
            None => {
                // There was no validation completed, return without doing anything
                println!("There was no result from validation, try again.");
            }
        };
    }
}

pub fn validate_ftx_candle<T: crate::utilities::Candle + DeserializeOwned>(
    candle: &Candle,
    exchange_candles: &mut [T],
) -> bool {
    // FTX candle validation on FTX Volume = ED Value, FTX sets open = last trade event if the
    // last trades was in the prior time period.
    // Consider valid if candle.value == exchange_candle.volume.
    let exchange_candle = exchange_candles
        .iter()
        .find(|c| c.datetime() == candle.datetime);
    match exchange_candle {
        Some(c) => {
            if c.volume() == candle.value {
                true
            } else {
                println!(
                    "Failed to validate: El-D Val: {:?} Ftx Vol: {:?}",
                    candle.value,
                    c.volume()
                );
                false
            }
        }
        None => {
            if candle.volume == dec!(0) {
                true
            } else {
                println!(
                    "Failed to validate: {:?}. Volume not 0 and no exchange candle.",
                    candle.datetime
                );
                false
            }
        }
    }
}

pub fn validate_gdax_candle_by_volume<T: crate::utilities::Candle + DeserializeOwned>(
    candle: &Candle,
    exchange_candles: &mut [T],
) -> bool {
    // GDAX candle validation on GDAX Volume = ED Volume and trade id count matches id first/last.
    // Consider valid if candle.volume == exchange_candle.volume.
    let exchange_candle = exchange_candles
        .iter()
        .find(|c| c.datetime() == candle.datetime);
    match exchange_candle {
        Some(c) => {
            if c.volume() == candle.volume
                && candle.last_trade_id.parse::<i32>().unwrap()
                    - candle.first_trade_id.parse::<i32>().unwrap()
                    + 1
                    == candle.trade_count as i32
            {
                // Volume matches - candle valid
                true
            } else {
                println!(
                    "Failed to validate: El-D Val: {:?} Gdax Vol: {:?}",
                    candle.volume,
                    c.volume()
                );
                println!(
                    "First Trade ID: {} Last Trade ID: {}. Num Trade {} Expected {}",
                    candle.last_trade_id,
                    candle.first_trade_id,
                    candle.trade_count,
                    candle.last_trade_id.parse::<i32>().unwrap()
                        - candle.first_trade_id.parse::<i32>().unwrap()
                        + 1,
                );
                false
            }
        }
        None => {
            if candle.volume == dec!(0) {
                true
            } else {
                println!(
                    "Failed to validate: {:?}. Volume not 0 and no exchange candle.",
                    candle.datetime
                );
                false
            }
        }
    }
}

pub async fn validate_gdax_candle_by_trade_ids<T: crate::utilities::Candle + DeserializeOwned>(
    pool: &PgPool,
    client: &RestClient,
    market: &MarketDetail,
    candle: &Candle,
    exchange_candles: &mut [T],
    time_frame: &TimeFrame,
    trade_table: &str,
) -> Option<bool> {
    // Get all trades for candle
    // Validate the trades - GDAX trade ids are sequential per product. Validate:
    // 1) There are no gaps in trade ids. Highest ID - Lowest ID + 1 = Num Trades
    // ie 1001 - 94 + 1 = 908 = 908 trades
    // 2) The next trade id in sequence falls on the next day
    // 3) The prev trade id in the sequence falls on the previous day
    let exchange_candle = exchange_candles
        .iter()
        .find(|c| c.datetime() == candle.datetime);
    let start = candle.datetime;
    let end = start + time_frame.as_dur();
    let mut trades = select_gdax_trades_by_time(pool, market, trade_table, start, end)
        .await
        .expect("Failed to select GDAX trades.");
    // Sort trades by id
    trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
    if trades.is_empty() {
        // If there are no trades & there is no exchange candle, pass as validated. If there is an
        // exchange candle - validated if volume is 0.
        match exchange_candle {
            Some(ec) => {
                println!(
                    "Exchange Candle Vol: {:?} & Trades Reported = None",
                    ec.volume()
                );
                Some(ec.volume() == dec!(0))
            }
            None => {
                println!("No exchange candle and no trades. Do not pass as valid");
                // Validation should be manual to confirm 0 trades for the day
                Some(false)
            }
        }
    } else {
        // There are trades. Validated 1 2 & 3. Trade id count, next and prev ids.
        // unwrap is save as trades is not empty so there is at least one
        let first_trade = trades.first().unwrap();
        let last_trade = trades.last().unwrap();
        println!("First: {:?}", first_trade);
        println!("Last: {:?}", last_trade);
        let validation_1 = last_trade.trade_id - first_trade.trade_id + 1 == trades.len() as i64;
        let validation_2 = {
            let next_trade = match client
                .get_gdax_next_trade(market.market_name.as_str(), last_trade.trade_id as i32)
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        return None; // Return None - handle None values from calling function
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    return None;
                                    // Leave event incomplete and try to process again
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            println!("Next trade: {:?}", next_trade);
            if !next_trade.is_empty() {
                // Pop off the trade
                let next_trade = next_trade.first().unwrap();
                // Compare the day of the next trade to the last trade day of the trades
                // to validated
                time_frame.is_gt_timeframe(last_trade.time(), next_trade.time())
                // next_trade.time().day() > last_trade.time().day()
            } else {
                // If next trade is empty, return false. There should always be a next trade
                false
            }
        };
        let validation_3 = {
            let previous_trade = match client
                .get_gdax_previous_trade(market.market_name.as_str(), first_trade.trade_id as i32)
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        return None; // Return None - handle None values from calling function
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    return None;
                                    // Leave event incomplete and try to process again
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            println!("Previous trade: {:?}", previous_trade);
            if !previous_trade.is_empty() {
                // Pop off the trade
                let previous_trade = previous_trade.first().unwrap();
                // Compare the day of the previous trade to the first trade day of the trades
                // to validated
                time_frame.is_lt_timeframe(first_trade.time(), previous_trade.time())
                // previous_trade.time().day() < last_trade.time().day() // Works only for 01d
            } else {
                // If previous trade is empty, check if the first trade id = 1. If so there is no
                // previous trade or time period so it is valid
                first_trade.trade_id == 1
            }
        };
        // Valid if all three are valid
        Some(validation_1 && validation_2 && validation_3)
    }
}

pub async fn get_ftx_candles_daterange<T: crate::utilities::Candle + DeserializeOwned>(
    client: &RestClient,
    market: &MarketDetail,
    start: DateTime<Utc>,
    mut end_or_last: DateTime<Utc>,
    seconds: i32,
) -> Vec<T> {
    // If end = start then FTX will not return any candles, add 1 second if the are equal
    end_or_last = match start == end_or_last {
        true => end_or_last + Duration::seconds(1),
        _ => end_or_last,
    };
    let mut candles: Vec<T> = Vec::new();
    while start < end_or_last {
        // Prevent 429 errors by only requesting 4 per second
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
        let mut new_candles = match client
            .get_ftx_candles(
                &market.market_name,
                Some(seconds),
                Some(start),
                Some(end_or_last),
            )
            .await
        {
            Err(RestError::Reqwest(e)) => {
                if e.is_timeout() || e.is_connect() || e.is_request() {
                    println!(
                        "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                } else if e.is_status() {
                    match e.status() {
                        Some(s) => match s.as_u16() {
                            500 | 502 | 503 | 520 | 530 => {
                                println!(
                                    "{} status code. Waiting 30 seconds before retry {:?}",
                                    s, e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                continue;
                            }
                            404 => {
                                println!("404 status code: {:?} / {:?}", s, e);
                                return Vec::new();
                            }
                            _ => {
                                panic!("Status code not handled: {:?} {:?}", s, e)
                            }
                        },
                        None => panic!("No status code for request error: {:?}", e),
                    }
                } else {
                    panic!("Error (not timeout / connect / request): {:?}", e)
                }
            }
            Err(e) => panic!("Other RestError: {:?}", e),
            Ok(result) => result,
        };
        let num_candles = new_candles.len();
        end_or_last = if num_candles > 0 {
            candles.append(&mut new_candles);
            candles.first().unwrap().datetime()
        } else {
            end_or_last
        };
        // Sort candles to get next last
        candles.sort_by_key(|c1| c1.datetime());
        if num_candles < 1501 {
            // Max pagination on candles is 1501
            break;
        }
    }
    // Dedup candles
    candles.dedup_by(|c1, c2| c1.datetime() == c2.datetime());
    candles
}

pub async fn get_gdax_candles_daterange<T: crate::utilities::Candle + DeserializeOwned>(
    client: &RestClient,
    market: &MarketDetail,
    mut start: DateTime<Utc>,
    end: DateTime<Utc>,
    seconds: i32,
) -> Vec<T> {
    // Initialize empty vec to hold all exchange candles
    let mut candles: Vec<T> = Vec::new();
    println!("Getting gdax candles from {} to {}", start, end);
    // GDAX API returns 300 candles per call. Loop until start and end are completed.
    while start <= end {
        let max_end = (start + Duration::minutes(15 * 300)).min(end);
        // Prevent 429 errors by only request 1 per second
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        // println!("Api call start {} and end {}", start, max_end);
        let mut new_candles = match client
            .get_gdax_candles(
                &market.market_name,
                Some(seconds),
                Some(start),
                Some(max_end),
            )
            .await
        {
            Err(RestError::Reqwest(e)) => {
                if e.is_timeout() || e.is_connect() || e.is_request() {
                    println!(
                        "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                } else if e.is_status() {
                    match e.status() {
                        Some(s) => match s.as_u16() {
                            502 | 503 | 520 | 530 => {
                                println!(
                                    "{} status code. Waiting 30 seconds before retry {:?}",
                                    s, e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                continue;
                            }
                            _ => {
                                panic!("Status code not handled: {:?} {:?}", s, e)
                            }
                        },
                        None => panic!("No status code for request error: {:?}", e),
                    }
                } else {
                    panic!("Error (not timeout / connect / request): {:?}", e)
                }
            }
            Err(e) => panic!("Other RestError: {:?}", e),
            Ok(result) => result,
        };
        if !new_candles.is_empty() {
            candles.append(&mut new_candles);
        };
        // Increment start, however if start == max end, increment by one further second to break
        if start == max_end {
            start = max_end + Duration::seconds(1);
        } else {
            start = max_end;
        };
    }
    // Sort and dedup
    candles.sort_by_key(|c1| c1.datetime());
    candles.dedup_by(|c1, c2| c1.datetime() == c2.datetime());
    println!("returning {} candles.", candles.len());
    candles
}

pub async fn insert_candle(
    pool: &PgPool,
    exchange_name: &ExchangeName,
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
        exchange_name.as_str(),
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
    candles: &[Candle],
    is_validated: bool,
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
        sqlx::query(sql)
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
            .bind(is_validated)
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
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            DELETE FROM candles_15T_{}
            WHERE market_id = $1
            AND datetime = $2
        "#,
        exchange_name.as_str(),
    );
    sqlx::query(&sql)
        .bind(market_id)
        .bind(datetime)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_candle_01d(
    pool: &PgPool,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = r#"
            DELETE FROM candles_01d
            WHERE market_id = $1
            AND datetime = $2
        "#;
    sqlx::query(sql)
        .bind(market_id)
        .bind(datetime)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_unvalidated_candles(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    tf: TimeFrame,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = match tf {
        TimeFrame::T15 => format!(
            r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1 and not is_validated
            ORDER BY datetime
            "#,
            exchange_name.as_str()
        ),
        TimeFrame::D01 => r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1 and not is_validated
            ORDER BY datetime
            "#
        .to_string(),
        _ => panic!("Candle resolution not supported."),
    };
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles_unvalidated_lt_datetime(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    datetime: DateTime<Utc>,
    tf: TimeFrame,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = match tf {
        TimeFrame::T15 => format!(
            r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1 AND not is_validated
            AND datetime < $2
            ORDER BY datetime
            "#,
            exchange_name.as_str()
        ),
        TimeFrame::D01 => r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1 AND not is_validated
            AND datetime < $2
            ORDER BY datetime
            "#
        .to_string(),
        _ => panic!("Candle resolution not supported."),
    };
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .bind(datetime)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_candles(
    pool: &PgPool,
    exchange_name: &ExchangeName,
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
            exchange_name.as_str()
        ),
        86400 => r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1
            ORDER BY datetime
            "#
        .to_string(),
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
    exchange_name: &ExchangeName,
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
        exchange_name.as_str()
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
    exchange_name: &ExchangeName,
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
        exchange_name.as_str()
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
    let row = sqlx::query_as::<_, DailyCandle>(sql)
        .bind(market_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn select_first_01d_candle(
    pool: &PgPool,
    market_id: &Uuid,
) -> Result<DailyCandle, sqlx::Error> {
    let sql = r#"
        SELECT * FROM candles_01d
        WHERE market_id = $1
        ORDER BY datetime
        "#;
    let row = sqlx::query_as::<_, DailyCandle>(sql)
        .bind(market_id)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn select_last_candle(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    tf: TimeFrame,
) -> Result<Candle, sqlx::Error> {
    let sql = match tf {
        TimeFrame::T15 => format!(
            r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1
        ORDER BY datetime DESC
        "#,
            exchange_name.as_str()
        ),
        TimeFrame::D01 => r#"
        SELECT * FROM candles_01d
        WHERE market_id = $1
        ORDER BY datetime DESC
        "#
        .to_string(),
        _ => panic!("Candle resolution not supported."),
    };
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
    let rows = sqlx::query_as::<_, DailyCandle>(sql)
        .bind(market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_previous_candle(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    datetime: DateTime<Utc>,
    tf: TimeFrame,
) -> Result<Candle, sqlx::Error> {
    let sql = match tf {
        TimeFrame::T15 => format!(
            r#"
            SELECT * FROM candles_15t_{}
            WHERE market_id = $1
            AND datetime < $2
            ORDER BY datetime DESC
        "#,
            exchange_name.as_str()
        ),
        TimeFrame::D01 => r#"
            SELECT * FROM candles_01d
            WHERE market_id = $1
            AND datetime < $2
            ORDER BY datetime DESC
            "#
        .to_string(),
        _ => panic!("Candle resolution not supported"),
    };
    let row = sqlx::query_as::<_, Candle>(&sql)
        .bind(market_id)
        .bind(datetime)
        .fetch_one(pool)
        .await?;
    Ok(row)
}

pub async fn update_candle_validation(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market_id: &Uuid,
    candle: &Candle,
    tf: TimeFrame,
) -> Result<(), sqlx::Error> {
    let sql = match tf {
        TimeFrame::T15 => format!(
            r#"
            UPDATE candles_15t_{}
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#,
            exchange_name.as_str()
        ),
        TimeFrame::D01 => r#"
            UPDATE candles_01d
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#
        .to_string(),
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
    sqlx::query(sql)
        .bind(candle.datetime)
        .bind(market_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::candles::{
        resample_candles, select_candles_gte_datetime, Candle, DailyCandle, TimeFrame,
    };
    use crate::configuration::get_configuration;
    use crate::exchanges::select_exchanges;
    use crate::exchanges::{client::RestClient, ftx::Trade as FtxTrade};
    use crate::inquisidor::Inquisidor;
    use crate::markets::{
        select_market_detail, select_market_ids_by_exchange, MarketCandleDetail, MarketDataStatus,
    };
    use crate::utilities::{create_date_range, next_month_datetime};
    use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
    use csv::Writer;
    use rust_decimal::prelude::*;
    use rust_decimal_macros::dec;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::ResearchCandle;

    pub fn sample_trades() -> Vec<FtxTrade> {
        let mut trades: Vec<FtxTrade> = Vec::new();
        trades.push(FtxTrade {
            id: 1,
            price: Decimal::new(702, 1),
            size: Decimal::new(23, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524886322, 0),
        });
        trades.push(FtxTrade {
            id: 2,
            price: Decimal::new(752, 1),
            size: Decimal::new(64, 1),
            side: "buy".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524887322, 0),
        });
        trades.push(FtxTrade {
            id: 3,
            price: Decimal::new(810, 1),
            size: Decimal::new(4, 1),
            side: "buy".to_string(),
            liquidation: true,
            time: Utc.timestamp(1524888322, 0),
        });
        trades.push(FtxTrade {
            id: 4,
            price: Decimal::new(767, 1),
            size: Decimal::new(13, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524889322, 0),
        });
        trades
    }

    pub async fn prep_ftx_file(path: &str, seed: i64, date: DateTime<Utc>) {
        // Create trades
        let mut trades = Vec::new();
        for i in seed..30 {
            let trade = create_ftx_trade(i, date);
            trades.push(trade)
        }
        // Get path to save file
        let f = format!("SOLPERP_{}.csv", date.format("%F"));
        // Set archive file path
        let archive_path = format!(
            "{}/trades/ftx/SOLPERP/{}/{}",
            path,
            date.format("%Y"),
            date.format("%m")
        );
        // Check directory is created
        std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
        // Set filepath
        let fp = std::path::Path::new(&archive_path).join(f);
        // Write trades to file
        let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
        for trade in trades.iter() {
            wtr.serialize(trade).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
    }

    pub fn create_ftx_trade(id: i64, date: DateTime<Utc>) -> FtxTrade {
        let price = Decimal::new(id, 0) + dec!(100);
        let size = Decimal::new(id, 0) * dec!(10);
        FtxTrade {
            id,
            price,
            size,
            side: "buy".to_string(),
            liquidation: false,
            time: Utc
                .ymd(date.year(), date.month(), date.day())
                .and_hms(0, id as u32 % 30, 0),
        }
    }

    #[test]
    pub fn new_from_last_returns_candle_populated_from_last_trade() {
        let market_id = Uuid::new_v4();
        let mut trades = sample_trades();
        let last_trade = trades.pop().unwrap();
        let candle = Candle::new_from_last(
            market_id,
            last_trade.time,
            last_trade.price,
            last_trade.time,
            &last_trade.id.to_string(),
        );
        println!("Candle: {:?}", candle);
    }

    #[test]
    pub fn new_from_trades_returns_candle() {
        let market_id = Uuid::new_v4();
        let trades = sample_trades();
        let first_trade = trades.first().unwrap();
        let candle = Candle::new_from_trades(market_id, first_trade.time, &trades);
        println!("Candle: {:?}", candle);
    }

    #[test]
    pub fn new_from_trades_returns_research_candle() {
        // Create trades
        let dt = Utc.ymd(2022, 1, 1).and_hms(0, 0, 0);
        let mut trades = Vec::new();
        for i in 1..1000000 {
            let trade = create_ftx_trade(i, dt);
            trades.push(trade);
        }
        let mi = Uuid::new_v4();
        // Start timer
        let start_v1 = Utc::now();
        let candle = ResearchCandle::new_from_trades(dt, &trades);
        let end_v1 = Utc::now();
        let start_v2 = Utc::now();
        let candle_2 = ResearchCandle::new_from_trades_v2(dt, &trades);
        let end_v2 = Utc::now();
        let start_v3 = Utc::now();
        let candle_3 = Candle::new_from_trades(mi, dt, &trades);
        let end_v3 = Utc::now();
        println!("Candle 1: {:?}", end_v1 - start_v1);
        println!("Candle 2: {:?}", end_v2 - start_v2);
        println!("Candle 2: {:?}", end_v3 - start_v3);
        println!("Candle 1: {:?}", candle);
        println!("Candle 2: {:?}", candle_2);
        println!("Candle 3: {:?}", candle_3);
    }

    #[tokio::test]
    pub async fn select_last_01d_candle_returns_none() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
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
            SELECT * FROM candles_01d_none
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
        let _pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
    }

    #[tokio::test]
    pub async fn revalidate_invalid_candle() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges.iter().find(|e| e.name.as_str() == "ftx").unwrap();

        // Set client = FTX for hardcoded candle tests
        let _client = RestClient::new(&exchange.name);

        // Get market ids
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == "BTC-PERP")
            .unwrap();
        let _market_detail = select_market_detail(&pool, &market)
            .await
            .expect("Could not fetch market detail.");

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
    }

    #[tokio::test]
    pub async fn resample_tests() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Get exchanges from database
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == "ftxus")
            .unwrap();
        // Get market ids
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == "BTC/USD")
            .unwrap();
        let _market_detail = select_market_detail(&pool, &market)
            .await
            .expect("Could not fetch market detail.");
        println!("Select last 91 days of hb candles: {:?}", Utc::now());
        let candles = select_candles_gte_datetime(
            &pool,
            &exchange.name,
            &market.market_id,
            Utc::now() - Duration::days(91),
        )
        .await
        .expect("Failed to select candles.");
        println!("Start resample through tfs: {:?}", Utc::now());
        for tf in TimeFrame::time_frames().iter().skip(1) {
            println!("Process tf {:?} resample candle: {:?}", tf, Utc::now());
            // Resample to new time frame
            let _resampled_candles = resample_candles(market.market_id, &candles, tf.as_dur());
        }
        println!("All candles resampled: {:?}", Utc::now());
    }

    #[tokio::test]
    pub async fn make_candles_with_invalid_market_does_nothing_mtd() {
        // Setup market with no mtd
        let ig = Inquisidor::new().await;
        // Update market to active with valid timestamp
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market trade details
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // New ig instance will pick up new data items
        let ig = Inquisidor::new().await;
        // Get test market
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        // Test no mtd returns false
        assert!(
            !ig.validate_market_eligibility_for_candles(market, &None)
                .await
        );
        // Create mtd
        let mtd = ig.get_market_trade_detail(&market).await;
        // Test with mtd created but backfill not completed
        assert!(
            !ig.validate_market_eligibility_for_candles(market, &Some(mtd))
                .await
        );
        // Modify mtd
        let mut mtd = ig.get_market_trade_detail(&market).await;
        mtd.previous_status = MarketDataStatus::Completed;
        mtd.next_trade_day = Some(mtd.previous_trade_day);
        println!("{:?}", mtd);
        assert!(
            !ig.validate_market_eligibility_for_candles(market, &Some(mtd))
                .await
        );
        // Modify mtd valid scenario
        let mut mtd = ig.get_market_trade_detail(&market).await;
        mtd.first_trade_ts = mtd.first_trade_ts - Duration::days(2);
        mtd.previous_status = MarketDataStatus::Completed;
        mtd.next_trade_day = Some(mtd.previous_trade_day + Duration::days(2));
        println!("{:?}", mtd);
        assert!(
            ig.validate_market_eligibility_for_candles(market, &Some(mtd))
                .await
        );
    }

    #[tokio::test]
    pub async fn make_candles_with_no_mcd_creates_mcd_and_first_candle() {
        // Set up market and mtd
        let ig = Inquisidor::new().await;
        // Update market to active with valid timestamp
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market trade details
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market candle details
        let sql = r#"
            DELETE FROM market_candle_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // New ig instance will pick up new data items
        let ig = Inquisidor::new().await;
        // Get test market
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        // Get mtd
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd to for test - set next trade day - first trade ts
        mtd.first_trade_ts = mtd.first_trade_ts - Duration::days(2);
        mtd.previous_status = MarketDataStatus::Completed;
        mtd.next_trade_day = Some(mtd.previous_trade_day + Duration::days(2));
        println!("{:?}", mtd);
        // Create trade files for 11/29 and 11/30
        prep_ftx_file(&ig.settings.application.archive_path, 1, mtd.first_trade_ts).await;
        prep_ftx_file(
            &ig.settings.application.archive_path,
            2,
            mtd.first_trade_ts + Duration::days(1),
        )
        .await;
        // Test
        let mcd = ig.make_mcd_and_first_candle(market, &mtd).await;
        println!("MCD: {:?}", mcd);
        assert_eq!(mcd.first_candle, mtd.first_trade_ts);
        assert_eq!(
            mcd.last_candle,
            next_month_datetime(mtd.first_trade_ts) - Duration::seconds(15)
        );
        // Assert runnign mack candles from last_candle does nothing further
        let sql = r#"
            UPDATE market_trade_details
            SET next_trade_day = '2021-12-02'
            WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        ig.make_candles_from_last_candle(&market, &mcd).await;
        // Get the mcd now and assert it is the same as before
        let new_mcds = MarketCandleDetail::select_all(&ig.ig_pool)
            .await
            .expect("Failed to select all mcd.");
        let new_mcd = new_mcds
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        assert_eq!(mcd.last_candle, new_mcd.last_candle);
    }

    #[tokio::test]
    pub async fn make_candles_with_mcd_makes_until_last_full_month() {
        // Set up market and mtd
        let ig = Inquisidor::new().await;
        // Update market to active with valid timestamp
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market trade details
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market candle details
        let sql = r#"
            DELETE FROM market_candle_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // New ig instance will pick up new data items
        let ig = Inquisidor::new().await;
        // Get test market
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        // Set up mtd details
        let _mtd = ig.get_market_trade_detail(&market).await;
        let sql = r#"
            UPDATE market_trade_details
            SET next_trade_day = '2022-02-02'
            WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Set up mcd details
        let sql = r#"
            INSERT INTO market_candle_details
            VALUES ('19994c6a-fa3c-4b0b-96c4-c744c43a9514', 'ftx', 'SOL-PERP', 's15', '2021-11-29',
                '2021-11-30 23:59:45', '2021-11-30 00:29:00', '29', 129);
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Now add trade files for the next 3 months
        let months = [
            Utc.ymd(2021, 12, 1).and_hms(0, 0, 0),
            Utc.ymd(2022, 1, 1).and_hms(0, 0, 0),
        ];
        for month in months.iter() {
            // Create date range for month
            let dr = create_date_range(*month, next_month_datetime(*month), Duration::days(1));
            for d in dr.iter() {
                prep_ftx_file(&ig.settings.application.archive_path, d.day() as i64, *d).await;
            }
        }
        // Get the mcd
        let mcds = MarketCandleDetail::select_all(&ig.ig_pool)
            .await
            .expect("Failed to select all mcd.");
        let mcd = mcds.iter().find(|m| m.market_name == "SOL-PERP").unwrap();
        println!("{:?}", mcd);
        // Run the function
        ig.make_candles_from_last_candle(&market, &mcd).await;
        // Assert
    }
}
