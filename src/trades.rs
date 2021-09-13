use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trade {
    pub datetime: DateTime<Utc>,
    pub id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: Side,
    pub liquidation: Boolean,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trades(Vec<Trade>);