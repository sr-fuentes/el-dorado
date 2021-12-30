use crate::candles::{Candle, TimeFrame};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

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
        let lbp = 7;
        let candle_calcs = candles.iter().fold(
            (
                dec!(0),    // close
                Vec::new(), // [return]
                Vec::new(), // [true range]
                Vec::new(), // [upper wick % of candle]
                Vec::new(), // [body % of candle]
                Vec::new(), // [lower wick % of candle]
            ),
            |(c, mut vr, mut vtr, mut vu, mut vb, mut vl), can| {
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
                vu.push(uwp);
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
                vl.push(lwp);
                (can.close, vr, vtr, vu, vb, vl)
            },
        );
        println!("Candle Calcs: {:?}", candle_calcs);
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
}
