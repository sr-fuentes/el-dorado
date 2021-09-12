use crate::exchanges::ftx::Trade;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
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
}

impl Candle {
    pub fn new_from_trades(datetime: DateTime<Utc>, trades: Vec<Trade>) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No trade to make candle.").price,
                Decimal::MIN,
                Decimal::MAX,
                dec!(0),
                dec!(0),
                0,
            ),
            |(o, h, l, c, v, n), t| {
                (
                    o,
                    h.max(t.price),
                    l.min(t.price),
                    t.price,
                    v + t.size,
                    n + 1,
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
            volume_net: candle_tuple.4,         // todo!
            volume_liquidation: candle_tuple.4, // todo!
            value: candle_tuple.4,              // todo!
            trade_count: 0,                     // todo!
            liquidation_count: 0,               // todo!
            last_trade_ts: datetime,            // todo!
            last_trade_id: "TODO".to_string(),  // todo!
        }
    }

    pub fn new_from_last(
        datetime: DateTime<Utc>,
        last_trade_price: Decimal,
        last_trade_ts: DateTime<Utc>,
        last_trade_id: String,
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
            last_trade_id,
        }
    }
}
