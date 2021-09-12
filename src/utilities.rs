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
    use crate::markets::CandleTest;
    use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
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

    #[test]
    pub fn build_range_from_vec_trades() {
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
            time: Utc.timestamp(1524892322, 0),
        });
        // Sort trades by time
        trades.sort_by(|t1, t2| t1.time.cmp(&t2.time));
        println!("Trades Sorted by Time:\n {:?}", trades);

        // Get first and last trade
        let first_trade = trades.first().expect("There is no first trade.");
        let last_trade = trades.last().expect("There is no last trade.");

        println!(
            "First and last trade times {} - {}.",
            first_trade.time, last_trade.time
        );

        // Get floors of first and last trades
        let floor_start = first_trade
            .time
            .duration_trunc(Duration::seconds(900))
            .unwrap();
        let floor_end = last_trade
            .time
            .duration_trunc(Duration::seconds(900))
            .unwrap();
        println!("Start and end floors {} - {}.", floor_start, floor_end);

        // Create Vec<DateTime> for range by 15T
        let mut dr_start = floor_start.clone();
        let mut date_range = Vec::new();
        while dr_start <= floor_end {
            date_range.push(dr_start);
            dr_start = dr_start + Duration::seconds(900);
        }
        println!("DateRange: {:?}", date_range);

        // // For each item in daterange, create candle
        // let candles = date_range.iter().fold(dec!(0), |v, dr| {
        //     trades.drain_filter(|t| t.time.duration_trunc(Duration::seconds(900)) == dr).iter().fold(dec!(0), |v, t| v + t.size)

        // };
        let dr_start = floor_start.clone();
        let candle = trades
            .iter()
            .filter(|t| t.time.duration_trunc(Duration::seconds(900)).unwrap() == dr_start)
            .fold(dec!(0), |v, t| v + t.size);
        println!("DT * V: {:?} & {:?}", dr_start, candle);

        let candles = date_range
            .iter()
            .fold((Vec::new(), dec!(0)), |(mut v, l), d| {
                let filtered_trades: Vec<Trade> = trades
                    .iter()
                    .filter(|t| t.time.duration_trunc(Duration::seconds(900)).unwrap() == *d)
                    .cloned()
                    .collect();
                println!("Filtered trades: {:?}", filtered_trades);
                let candle = match filtered_trades.len() {
                    0 => {
                        let candle = CandleTest {
                            datetime: *d,
                            open: l,
                            high: l,
                            low: l,
                            close: l,
                            volume: dec!(0),
                        };
                        candle
                    }
                    _ => {
                        let candle = filtered_trades.iter().fold(
                            (
                                filtered_trades
                                    .first()
                                    .expect("No trade to make candle.")
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
                        let candle = CandleTest {
                            datetime: *d,
                            open: candle.0,
                            high: candle.1,
                            low: candle.2,
                            close: candle.3,
                            volume: candle.4,
                        };
                        candle
                    }
                };
                v.push(candle);
                (v, candle.close)
            });
        println!("Candles: {:?}", candles);

        println!("DT * V: {:?} & {:?}", dr_start, candle);

        let candle = trades.iter().fold(
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
        println!("Open, High, Low, Close, Volume & Count: {:?}", candle);
    }
}
