use crate::exchanges::ftx::Trade;
use crate::markets::CandleTest;
use rust_decimal::prelude::*;
use std::io::{self, Write};

pub fn get_input<U: std::str::FromStr>(prompt: &str) -> U {
    loop {
        let mut input = String::new();

        // Reads the input from STDIN and places it in the String
        println!("{}", prompt);
        // Flush stdout to get on same line as prompt.
        let _ = io::stdout().flush().expect("Failed to flush stdout.");
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read input.");

        // Convert to another type
        // If successful, bind to a new variable named input.
        // If failed, restart loop.
        let input = match input.trim().parse::<U>() {
            Ok(parsed_input) => parsed_input,
            Err(_) => continue,
        };
        return input;
    }
}

pub fn make_candles(trades: Vec<Trade>, seconds: i32) -> Vec<CandleTest> {
    let candles: Vec<CandleTest> = vec![];
    candles
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use rust_decimal::prelude::*;
    use rust_decimal_macros::dec;

    #[test]
    pub fn make_candles_works_for_one_candle() {
        // Arrange
        // Create vec of trades
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
        println!("Trades Vec:\n {:?}", trades);
        // Sort trades by price
        trades.sort_by(|t1, t2| t1.price.cmp(&t2.price));
        println!("Trades Sorted by Price:\n {:?}", trades);
        // Sort trades by time
        trades.sort_by(|t1, t2| t1.time.cmp(&t2.time));
        println!("Trades Sorted by Time:\n {:?}", trades);

        let candle = trades.iter().fold(
            (
                trades
                    .first()
                    .expect("Cannot create candle without trades.")
                    .price,
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
        println!("Open, High, Low, Close, Volume & Count: {:?}", candle);
    }
}
