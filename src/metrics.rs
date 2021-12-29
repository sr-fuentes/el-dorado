use crate::candles::{Candle, TimeFrame};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

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
    pub datetime: DateTime<Utc>,
    pub time_frame: TimeFrame,
    pub lbp: i64,
    pub r#return: Decimal,
    pub tr: Decimal,
    pub n: Decimal,
}

impl Metric {
    // Takes vec of candles for a time frame and calculates metrics for the period
    pub fn new(_tf: TimeFrame, candles: &[Candle]) {
        let metric = candles.iter().fold(
            (
                dec!(0),    // close
                Vec::new(), // [return]
                Vec::new(), // [true range]
            ),
            |(c, mut vr, mut vtr), can| {
                let r = match c.is_zero() {
                    true => c,
                    false => can.close / c - dec!(1),
                };
                vr.push(r);
                let tr = match c.is_zero() {
                    true => c,
                    false => {
                        let hl = can.high - can.low;
                        let hpdc = (can.high - c).abs();
                        let pdcl = (c - can.low).abs();
                        hl.max(hpdc).max(pdcl)
                    }
                };
                vtr.push(tr);
                (can.close, vr, vtr)
            },
        );
        println!("Metric: {:?}", metric);
    }
}
