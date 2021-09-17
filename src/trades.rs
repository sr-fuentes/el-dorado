use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trade {
    pub datetime: DateTime<Utc>,
    pub id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: String,
    pub liquidation: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trades(Vec<Trade>);

impl Trade {
    pub const BUY: &'static str = "buy";
    pub const SELL: &'static str = "sell";

    pub fn new(
        datetime: DateTime<Utc>,
        id: String,
        price: Decimal,
        size: Decimal,
        side: &str,
        liquidation: bool,
    ) -> Self {
        Self {
            datetime,
            id,
            price,
            size,
            side: side.to_string(),
            liquidation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    pub fn new_trade_returns_trade() {
        let new_trade = Trade::new(
            Utc.timestamp(1524886322, 0),
            1.to_string(),
            dec!(70.2),
            dec!(23.1),
            Trade::BUY,
            false,
        );
        println!("New Trade: {:?}", new_trade);
    }
}
