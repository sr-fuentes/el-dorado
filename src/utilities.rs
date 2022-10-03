use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::convert::TryFrom;
use std::env;
use std::io::{self, Write};
use twilio::{OutboundMessage, TwilioClient};

#[derive(Debug)]
pub struct Twilio {
    pub has_twilio: bool,
    pub client: Option<TwilioClient>,
    pub to_number: Option<String>,
    pub from_number: Option<String>,
}

impl Default for Twilio {
    fn default() -> Self {
        Self::new()
    }
}

impl Twilio {
    pub fn new() -> Self {
        let mut has_twilio = true;
        let account_sid = match env::var("TWILIO_ACCOUNT_SID") {
            Ok(val) => Some(val),
            Err(_) => {
                has_twilio = false;
                None
            }
        };
        let auth_token = match env::var("TWILIO_AUTH_TOKEN") {
            Ok(val) => Some(val),
            Err(_) => {
                has_twilio = false;
                None
            }
        };
        let to_number = match env::var("MY_PHONE_NUMBER") {
            Ok(val) => Some(val),
            Err(_) => {
                has_twilio = false;
                None
            }
        };
        let from_number = match env::var("MY_TWILIO_NUMBER") {
            Ok(val) => Some(val),
            Err(_) => {
                has_twilio = false;
                None
            }
        };
        let client = match has_twilio {
            true => Some(TwilioClient::new(
                &account_sid.unwrap(),
                &auth_token.unwrap(),
            )),
            false => None,
        };
        Self {
            has_twilio,
            client,
            to_number,
            from_number,
        }
    }

    pub async fn send_sms(&self, message: &str) {
        match self.has_twilio {
            true => match self
                .client
                .as_ref()
                .unwrap()
                .send_message(OutboundMessage::new(
                    self.from_number.as_ref().unwrap(),
                    self.to_number.as_ref().unwrap(),
                    message,
                ))
                .await
            {
                Ok(m) => println!("{:?}", m),
                Err(e) => eprintln!("{:?}", e),
            },
            false => todo!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum TimeFrame {
    S15,
    S30,
    T01,
    T03,
    T05,
    T15,
    T30,
    H01,
    H02,
    H03,
    H04,
    H06,
    H12,
    D01,
    D03,
    W01,
}

impl TimeFrame {
    pub fn as_str(&self) -> &'static str {
        match self {
            TimeFrame::S15 => "s15",
            TimeFrame::S30 => "s30",
            TimeFrame::T01 => "t01",
            TimeFrame::T03 => "t03",
            TimeFrame::T05 => "t05",
            TimeFrame::T15 => "t15",
            TimeFrame::T30 => "t30",
            TimeFrame::H01 => "h01",
            TimeFrame::H02 => "h01",
            TimeFrame::H03 => "h03",
            TimeFrame::H04 => "h04",
            TimeFrame::H06 => "h06",
            TimeFrame::H12 => "h12",
            TimeFrame::D01 => "d01",
            TimeFrame::D03 => "d03",
            TimeFrame::W01 => "w01",
        }
    }

    pub fn as_secs(&self) -> i64 {
        match self {
            TimeFrame::S15 => 15,
            TimeFrame::S30 => 30,
            TimeFrame::T01 => 60,
            TimeFrame::T03 => 180,
            TimeFrame::T05 => 300,
            TimeFrame::T15 => 900,
            TimeFrame::T30 => 1800,
            TimeFrame::H01 => 3600,
            TimeFrame::H02 => 7200,
            TimeFrame::H03 => 10800,
            TimeFrame::H04 => 14400,
            TimeFrame::H06 => 21600,
            TimeFrame::H12 => 43200,
            TimeFrame::D01 => 86400,
            TimeFrame::D03 => 259200,
            TimeFrame::W01 => 604800,
        }
    }

    pub fn as_dur(&self) -> Duration {
        match self {
            TimeFrame::S15 => Duration::seconds(15),
            TimeFrame::S30 => Duration::seconds(30),
            TimeFrame::T01 => Duration::minutes(1),
            TimeFrame::T03 => Duration::minutes(3),
            TimeFrame::T05 => Duration::minutes(5),
            TimeFrame::T15 => Duration::minutes(15),
            TimeFrame::T30 => Duration::minutes(30),
            TimeFrame::H01 => Duration::hours(1),
            TimeFrame::H02 => Duration::hours(2),
            TimeFrame::H03 => Duration::hours(3),
            TimeFrame::H04 => Duration::hours(4),
            TimeFrame::H06 => Duration::hours(6),
            TimeFrame::H12 => Duration::hours(12),
            TimeFrame::D01 => Duration::days(1),
            TimeFrame::D03 => Duration::days(3),
            TimeFrame::W01 => Duration::weeks(1),
        }
    }

    pub fn time_frames() -> Vec<TimeFrame> {
        let time_frames = vec![
            TimeFrame::T15,
            TimeFrame::H01,
            TimeFrame::H04,
            TimeFrame::H12,
            TimeFrame::D01,
        ];
        time_frames
    }

    pub fn is_gt_timeframe(&self, dt1: DateTime<Utc>, dt2: DateTime<Utc>) -> bool {
        dt1.duration_trunc(self.as_dur()).unwrap() < dt2.duration_trunc(self.as_dur()).unwrap()
    }

    pub fn is_lt_timeframe(&self, dt1: DateTime<Utc>, dt2: DateTime<Utc>) -> bool {
        dt1.duration_trunc(self.as_dur()).unwrap() > dt2.duration_trunc(self.as_dur()).unwrap()
    }
}

impl TryFrom<String> for TimeFrame {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "t15" => Ok(Self::T15),
            "h01" => Ok(Self::H01),
            "h04" => Ok(Self::H04),
            "h12" => Ok(Self::H12),
            "d01" => Ok(Self::D01),
            other => Err(format!("{} is not a supported TimeFrame.", other)),
        }
    }
}

pub fn get_input<U: std::str::FromStr>(prompt: &str) -> U {
    loop {
        let mut input = String::new();

        // Reads the input from STDIN and places it in the String
        println!("{}", prompt);
        // Flush stdout to get on same line as prompt.
        io::stdout().flush().expect("Failed to flush stdout.");
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

pub fn min_to_dp(increment: Decimal) -> i32 {
    if increment < dec!(1) {
        let dp = increment.scale() as i32;
        if dec!(10).powi(dp as i64) * increment == dec!(1) {
            dp
        } else if dec!(10).powi(dp as i64) * increment == dec!(5) {
            dp - 1
        } else {
            dp - 2
        }
    } else {
        let log10 = increment.log10();
        if log10.scale() == 0 {
            -log10.trunc().mantissa() as i32
        } else {
            -log10.trunc().mantissa() as i32 - 1
        }
    }
}

pub trait Trade {
    fn trade_id(&self) -> i64;
    fn price(&self) -> Decimal;
    fn size(&self) -> Decimal;
    fn side(&self) -> String;
    fn liquidation(&self) -> bool;
    fn time(&self) -> DateTime<Utc>;
}

pub trait Candle {
    fn datetime(&self) -> DateTime<Utc>;
    fn volume(&self) -> Decimal;
}

pub trait Market {
    fn name(&self) -> String;
    fn market_type(&self) -> String;
    fn dp_quantity(&self) -> i32;
    fn dp_price(&self) -> i32;
    fn min_quantity(&self) -> Option<Decimal>;
    fn base_currency(&self) -> Option<String>;
    fn quote_currency(&self) -> Option<String>;
    fn underlying(&self) -> Option<String>;
    fn usd_volume_24h(&self) -> Option<Decimal>;
}

#[cfg(test)]
mod tests {
    use crate::exchanges::ftx::Trade;
    use crate::utilities::Twilio;
    use chrono::{Duration, DurationRound, TimeZone, Utc};
    use rust_decimal::prelude::*;
    use rust_decimal_macros::dec;

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

        // let candles = date_range
        //     .iter()
        //     .fold((Vec::new(), dec!(0)), |(mut v, l), d| {
        //         let filtered_trades: Vec<Trade> = trades
        //             .iter()
        //             .filter(|t| t.time.duration_trunc(Duration::seconds(900)).unwrap() == *d)
        //             .cloned()
        //             .collect();
        //         println!("Filtered trades: {:?}", filtered_trades);

        //         v.push(candle);
        //     });
    }

    #[tokio::test]
    pub async fn test_sms_send() {
        let client = Twilio::new();
        let message = "Test rust / twilio sms send.";
        client.send_sms(message).await;
    }

    #[test]
    pub fn is_gt_timeframe() {
        // Assert that a given datetime is in a greate timeframe than first datetime
        let dt1 = Utc.ymd(2021, 6, 27).and_hms(2, 29, 59);
        let dt2 = Utc.ymd(2021, 6, 27).and_hms(2, 30, 01);
        assert!(super::TimeFrame::T15.is_gt_timeframe(dt1, dt2));
    }
}
