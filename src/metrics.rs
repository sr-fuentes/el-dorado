use crate::candles::{Candle, TimeFrame};
use crate::exchanges::ExchangeName;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::VecDeque;
use uuid::Uuid;

impl TimeFrame {
    pub fn lbps(&self) -> [i64; 3] {
        match self {
            TimeFrame::T15 => [672, 2880, 8640],
            TimeFrame::H01 => [168, 720, 2160],
            TimeFrame::H04 => [42, 180, 540],
            TimeFrame::H12 => [14, 60, 180],
            TimeFrame::D01 => [7, 30, 90],
        }
    }
}

pub struct Metric {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub datetime: DateTime<Utc>,
    pub time_frame: TimeFrame,
    pub lbp: i64,
    pub close: Decimal,
    pub r: Decimal,
    pub n: Decimal,
    pub rs: Decimal,
    pub trs: Decimal,
    pub uws: Decimal,
    pub mbs: Decimal,
    pub lws: Decimal,
}

impl Metric {
    // Takes vec of candles for a time frame and calculates metrics for the period
    pub fn new(tf: TimeFrame, candles: &[Candle]) {
        // Get look back periods for TimeFrame
        let lbps = tf.lbps();
        // Iterate through candles and return Vecs of Decimals to use for calculations
        let vecs = candles.iter().fold(
            (
                dec!(0),    // close
                Vec::new(), // [close]
                Vec::new(), // [high]
                Vec::new(), // [low]
                Vec::new(), // [volume]
                Vec::new(), // [value]
                Vec::new(), // [netvol]
                Vec::new(), // [return]
                Vec::new(), // [true range]
                Vec::new(), // [upper wick % of candle]
                Vec::new(), // [body % of candle]
                Vec::new(), // [lower wick % of candle]
            ),
            |(
                c,
                mut vc,
                mut vh,
                mut vl,
                mut vv,
                mut va,
                mut vn,
                mut vr,
                mut vtr,
                mut vuw,
                mut vb,
                mut vlw,
            ),
             can| {
                // Map close, high and low to their vecs
                vc.push(can.close);
                vh.push(can.high);
                vl.push(can.low);
                vv.push(can.volume);
                va.push(can.value);
                vn.push(can.volume_net);
                // Calc return as current candle close / previous candle close - 1. If first candle
                // return 0. The initial zero should not be used when calculating mean.
                let r = match c.is_zero() {
                    true => c,
                    false => can.close / c - dec!(1),
                };
                vr.push(r);
                let hl = can.high - can.low;
                // True range is max of high - low, abs(high - previous close),
                // and abs(previous close - low). If it is the first one then put high - low
                let tr = match c.is_zero() {
                    true => hl,
                    false => {
                        let hpdc = (can.high - c).abs();
                        let pdcl = (c - can.low).abs();
                        hl.max(hpdc).max(pdcl)
                    }
                };
                vtr.push(tr);
                // Upper wick as the % of the candle (h - l) that is attributed to the upper wick
                // If the candle has no change (h = l) return 0
                let uwp = match hl.is_zero() {
                    true => dec!(0),
                    false => (can.high - can.open.max(can.close)) / hl,
                };
                vuw.push(uwp);
                // Body as the % of candle
                // If the candle has no change return 0
                let bp = match hl.is_zero() {
                    true => dec!(0),
                    false => (can.open - can.close).abs() / hl,
                };
                vb.push(bp);
                // Lower wick as the % of the candle
                let lwp = match hl.is_zero() {
                    true => dec!(0),
                    false => (can.open.min(can.close) - can.low) / hl,
                };
                vlw.push(lwp);
                (can.close, vc, vh, vl, vv, va, vn, vr, vtr, vuw, vb, vlw)
            },
        );
        // println!("Vecs: {:?}", vecs);
        //let mut metrics: Vec<Metric> = Vec::new();
        for lbp in &lbps[0..1] {
            println!("Look Back Period: {}", lbp);
            let n = Metric::ewma(&vecs.8, *lbp);
            println!("Vec: {:?}", vecs.8);
            println!("N: {:?}", n);
        }

        // pub market_id: Uuid,
        // pub exchange_name: ExchangeName,
        // pub datetime: DateTime<Utc>,
        // pub time_frame: TimeFrame,
        // pub lbp: i64,
        // pub close: Decimal,
        // pub r: Decimal,
        // pub n: Decimal,
        // pub rs: Decimal,
        // pub trs: Decimal,
        // pub uws: Decimal,
        // pub mbs: Decimal,
        // pub lws: Decimal,
    }

    pub fn new_deque(_tf: TimeFrame, candles: &[Candle]) {
        let _lbp = 7;
        let metric = candles.iter().fold(
            (
                dec!(0),         // close
                VecDeque::new(), // [return]
                dec!(0),         // running sum of return
                VecDeque::new(),
                dec!(0), // running sum of tr
            ),
            |(c, mut vr, sr, mut vtr, str), can| {
                // Calc return as current candle close / previous candle close - 1. If first candle
                // return 0. The initial zero should not be used when calculating mean.
                let r = match c.is_zero() {
                    true => c,
                    false => can.close / c - dec!(1),
                };
                vr.push_back(r);
                let tr = match c.is_zero() {
                    true => c,
                    false => {
                        let hl = can.high - can.low;
                        let hpdc = (can.high - c).abs();
                        let pdcl = (c - can.low).abs();
                        hl.max(hpdc).max(pdcl)
                    }
                };
                vtr.push_back(tr);
                (can.close, vr, sr, vtr, str)
            },
        );
        println!("Metric: {:?}", metric);
    }

    pub fn ewma(v: &[Decimal], lbp: i64) -> Decimal {
        // Set k = smoothing factor
        let k = dec!(2) / (Decimal::from_i64(lbp).unwrap() + dec!(1));
        let ik = dec!(1.0) - k;
        let mut ewma = v[0];
        for i in 1..v.len() {
            ewma = v[i] * k + ewma * ik;
        }
        ewma
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candles::select_candles_gte_datetime;
    use crate::configuration::get_configuration;
    use chrono::{TimeZone, Utc};
    use sqlx::PgPool;

    #[tokio::test]
    pub async fn new_metrics_calculations_and_times() {
        // Select set of candles and calc metrics for them.
        // Expand to 91 days scenario and calculations for all time frame and lbps
        // Exclude time to fetch candles from database in this calc.
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Get candles from db
        let start = Utc.ymd(2021, 12, 25).and_hms(0, 0, 0);
        let candles = select_candles_gte_datetime(
            &pool,
            "ftxus",
            &Uuid::parse_str("2246c870-769f-44a4-b989-ffa2de37f8b1").unwrap(),
            start,
        )
        .await
        .expect("Failed to select candles.");
        println!("Num Candles: {:?}", candles.len());
        let timer_start = Utc::now();
        let _metrics = Metric::new(TimeFrame::T15, &candles);
        let timer_end = Utc::now();
        let time_elapsed = timer_end - timer_start;
        println!("Time to calc: {:?}", time_elapsed);
    }

    #[tokio::test]
    pub async fn calc_daily_ema() {
        // Get daily BTC-PERP candles from FTX
        let client = crate::exchanges::ftx::RestClient::new_intl();
        let mut candles = client
            .get_candles("BTC-PERP", Some(86400), None, None)
            .await
            .expect("Failed to get candles.");
        // Sort candles and put close prices into vector
        candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
        let vc: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
        // Calc the EMA
        let period = 90 as i64;
        let ewma = Metric::ewma(&vc, period);
        println!("Candle Closes: {:?}", vc);
        println!("EWMA {}: {:?}", period, ewma);
    }

    #[tokio::test]
    pub async fn calc_atr() {
        // Get daily BTC-PERP candles from FTX
        let client = crate::exchanges::ftx::RestClient::new_intl();
        let mut candles = client
            .get_candles("BTC-PERP", Some(86400), None, None)
            .await
            .expect("Failed to get candles.");
        // Sort candles and put tr values into vector
        candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
        // let vtr = candles.iter().fold(Vec::new(), |c, )
        // Calc the EMA
        let period = 90 as i64;
        let ewma = Metric::ewma(&vc, period);
        println!("Candle Closes: {:?}", vc);
        println!("EWMA {}: {:?}", period, ewma);
    }
}
