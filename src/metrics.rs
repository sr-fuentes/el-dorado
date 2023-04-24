use std::{cmp::Ordering, collections::HashMap, fs::File, path::PathBuf};

use crate::{
    candles::ProductionCandle,
    configuration::Database,
    eldorado::{ElDorado, ElDoradoError},
    exchanges::ExchangeName,
    markets::MarketDetail,
    mita::Heartbeat,
    utilities::TimeFrame,
};
use chrono::{DateTime, Utc};
use csv::Reader;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

impl TimeFrame {
    pub fn lbps(&self) -> [i64; 3] {
        match self {
            TimeFrame::T15 => [672, 2880, 8640],
            TimeFrame::H01 => [168, 720, 2160],
            TimeFrame::H04 => [42, 180, 540],
            TimeFrame::H12 => [14, 60, 180],
            TimeFrame::D01 => [7, 30, 90],
            _ => [0, 0, 0], // To be defined
        }
    }

    pub fn lbp_l(&self) -> i64 {
        match self {
            TimeFrame::D01 => 90,
            TimeFrame::H12 => 90,
            TimeFrame::H08 => 90,
            TimeFrame::H06 => 180,
            TimeFrame::H04 => 270,
            TimeFrame::H03 => 360,
            TimeFrame::H02 => 360,
            TimeFrame::H01 => 360,
            TimeFrame::T30 => 630,
            TimeFrame::T15 => 540,
            TimeFrame::T05 => 540,
            TimeFrame::T03 => 540,
            TimeFrame::T01 => 630,
            TimeFrame::S30 => 360,
            TimeFrame::S15 => 270,
        }
    }

    pub fn lbp_s(&self) -> i64 {
        match self {
            TimeFrame::D01 => 10,
            TimeFrame::H12 => 14,
            TimeFrame::H08 => 21,
            TimeFrame::H06 => 30,
            TimeFrame::H04 => 21,
            TimeFrame::H03 => 45,
            TimeFrame::H02 => 60,
            TimeFrame::H01 => 90,
            TimeFrame::T30 => 60,
            TimeFrame::T15 => 90,
            TimeFrame::T05 => 90,
            TimeFrame::T03 => 60,
            TimeFrame::T01 => 60,
            TimeFrame::S30 => 60,
            TimeFrame::S15 => 90,
        }
    }

    pub fn max_len(&self) -> i64 {
        match self {
            TimeFrame::T15 => 9312,
            TimeFrame::H01 => 2328,
            TimeFrame::H04 => 582,
            TimeFrame::H12 => 194,
            TimeFrame::D01 => 97,
            _ => 1, // To be defined
        }
    }

    pub fn resample_from(&self) -> TimeFrame {
        match self {
            TimeFrame::S15 => TimeFrame::S15,
            TimeFrame::S30 => TimeFrame::S15,
            TimeFrame::T01 => TimeFrame::S15,
            TimeFrame::T03 => TimeFrame::T01,
            TimeFrame::T05 => TimeFrame::T01,
            TimeFrame::T15 => TimeFrame::T05,
            TimeFrame::T30 => TimeFrame::T15,
            TimeFrame::H01 => TimeFrame::T15,
            TimeFrame::H02 => TimeFrame::H01,
            TimeFrame::H03 => TimeFrame::H01,
            TimeFrame::H04 => TimeFrame::H01,
            TimeFrame::H06 => TimeFrame::H01,
            TimeFrame::H08 => TimeFrame::H04,
            TimeFrame::H12 => TimeFrame::H04,
            TimeFrame::D01 => TimeFrame::H12,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
pub struct MetricAP {
    pub market_name: String,
    pub exchange_name: ExchangeName,
    pub datetime: DateTime<Utc>,
    pub time_frame: TimeFrame,
    pub lbp: i64,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub r: Decimal,
    pub h004c: Decimal,
    pub l004c: Decimal,
    pub h004h: Decimal,
    pub l004l: Decimal,
    pub h008c: Decimal,
    pub l008c: Decimal,
    pub h008h: Decimal,
    pub l008l: Decimal,
    pub h012c: Decimal,
    pub l012c: Decimal,
    pub h012h: Decimal,
    pub l012l: Decimal,
    pub h024c: Decimal,
    pub l024c: Decimal,
    pub h024h: Decimal,
    pub l024l: Decimal,
    pub h048c: Decimal,
    pub l048c: Decimal,
    pub h048h: Decimal,
    pub l048l: Decimal,
    pub h096c: Decimal,
    pub l096c: Decimal,
    pub h096h: Decimal,
    pub l096l: Decimal,
    pub h192c: Decimal,
    pub l192c: Decimal,
    pub h192h: Decimal,
    pub l192l: Decimal,
    pub ema1: Decimal,
    pub ema2: Decimal,
    pub ema3: Decimal,
    pub mv1: Decimal,
    pub mv2: Decimal,
    pub mv3: Decimal,
    pub atr: Decimal,
    pub vw: Decimal,
    pub ma: Decimal,
    pub ofz: Decimal,
    pub vz: Decimal,
    pub rz: Decimal,
    pub trz: Decimal,
    pub uwz: Decimal,
    pub bz: Decimal,
    pub lwz: Decimal,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResearchMetric {
    pub market_id: Uuid,
    pub tf: TimeFrame,
    pub datetime: DateTime<Utc>,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub atr_l: Decimal,
    pub atr_s: Decimal,
    pub ma_filter: MetricFilter,
    pub atr_filter: MetricFilter,
    pub direction: MetricDirection,
    pub return_z_l: Decimal,
    pub return_z_s: Decimal,
    pub tr_z_l: Decimal,
    pub tr_z_s: Decimal,
    pub upper_wick_z_l: Decimal,
    pub upper_wick_z_s: Decimal,
    pub body_z_l: Decimal,
    pub body_z_s: Decimal,
    pub lower_wick_z_l: Decimal,
    pub lower_wick_z_s: Decimal,
    pub volume_z_l: Decimal,
    pub volume_z_s: Decimal,
    pub volume_net_z_l: Decimal,
    pub volume_net_z_s: Decimal,
    pub volume_pct_z_l: Decimal,
    pub volume_pct_z_s: Decimal,
    pub volume_liq_z_l: Decimal,
    pub volume_liq_z_s: Decimal,
    pub volume_liq_net_z_l: Decimal,
    pub volume_liq_net_z_s: Decimal,
    pub volume_liq_pct_z_l: Decimal,
    pub volume_liq_pct_z_s: Decimal,
    pub value_z_l: Decimal,
    pub value_z_s: Decimal,
    pub value_net_z_l: Decimal,
    pub value_net_z_s: Decimal,
    pub value_pct_z_l: Decimal,
    pub value_pct_z_s: Decimal,
    pub value_liq_z_l: Decimal,
    pub value_liq_z_s: Decimal,
    pub value_liq_net_z_l: Decimal,
    pub value_liq_net_z_s: Decimal,
    pub value_liq_pct_z_l: Decimal,
    pub value_liq_pct_z_s: Decimal,
    pub trade_count_z_l: Decimal,
    pub trade_count_z_s: Decimal,
    pub trade_count_net_z_l: Decimal,
    pub trade_count_net_z_s: Decimal,
    pub trade_count_pct_z_l: Decimal,
    pub trade_count_pct_z_s: Decimal,
    pub liq_count_z_l: Decimal,
    pub liq_count_z_s: Decimal,
    pub liq_count_net_z_l: Decimal,
    pub liq_count_net_z_s: Decimal,
    pub liq_count_pct_z_l: Decimal,
    pub liq_count_pct_z_s: Decimal,
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::Type, Eq, PartialEq)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum MetricFilter {
    SL,
    LS,
    Equal,
}

impl MetricFilter {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricFilter::SL => "sl",
            MetricFilter::LS => "ls",
            MetricFilter::Equal => "equal",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, sqlx::Type, Eq, PartialEq)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum MetricDirection {
    Up,
    Down,
    NC,
}

impl MetricDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricDirection::Up => "up",
            MetricDirection::Down => "down",
            MetricDirection::NC => "nc",
        }
    }
}

pub struct Metric {}

impl Metric {
    pub fn ewma(v: &[Decimal], lbp: i64) -> Decimal {
        // Set k = smoothing factor
        let k = dec!(2) / (Decimal::from_i64(lbp).unwrap() + Decimal::ONE);
        let ik = dec!(1.0) - k;
        let mut ewma = v[0];
        for i in v.iter().skip(1) {
            ewma = i * k + ewma * ik;
        }
        ewma
    }

    pub fn z(v: &[Decimal], rs: usize, re: usize) -> Decimal {
        // Calculate the deviation mulitple of the last item in candle
        // to standard deviation of the period
        let n = Decimal::from(v[rs..re].len());
        let mean = v[rs..re].iter().sum::<Decimal>() / n;
        let sd = (v[rs..re]
            .iter()
            .fold(dec!(0), |s, x| s + (mean - x).powi(2))
            / n)
            .sqrt()
            .unwrap();
        (v[v.len() - 1] - mean)
            .checked_div(sd)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn dons(c: &[Decimal], h: &[Decimal], l: &[Decimal]) -> Vec<Decimal> {
        // For each of the ranges below, calc the higheset and lowest value
        let mut dons = Vec::new();
        let ranges = [4, 8, 12, 24, 48, 96, 192];
        // Set min and max to last element of vecs (first item to check)
        let mut i = 2; // Set to 2 to skip the last element in DON calc
        let mut min_c = Decimal::MAX;
        let mut min_l = Decimal::MAX;
        let mut max_c = Decimal::MIN;
        let mut max_h = Decimal::MIN;
        // For each item in range (don window), check min and max until next range skipping the last
        // item which is the current c/h/l price
        for range in ranges.iter() {
            // For range = 4, check the last 4 elements of vec skipping the last item
            // i = 2, range = 4
            // 2 <= (4 + 1)     2 <= 5 = True, i += 1
            // 3 <= (4 + 1)     3 <= 5 = True, i += 1
            // 4 <= (4 + 1)     4 <= 5 = True, i += 1
            // 5 <= (4 + 1)     5 <= 5 = True, i += 1
            // 6 <= (4 + 1)     6 <= 5 = False, push dons for range
            // 6 <= (8 + 1)     6 <= 9 = True, i += 1...
            while i <= (*range as usize + 1) && i <= c.len() {
                // Compare current min/max to len()-i value
                min_c = min_c.min(c[c.len() - i]);
                min_l = min_l.min(l[l.len() - i]);
                max_c = max_c.max(c[c.len() - i]);
                max_h = max_h.max(h[h.len() - i]);
                i += 1;
            }
            dons.push(max_c);
            dons.push(min_c);
            dons.push(max_h);
            dons.push(min_l);
        }
        dons
    }
}

impl MetricAP {
    // Takes vec of candles for a time frame and calculates metrics for the period
    pub fn new(
        market: &MarketDetail,
        tf: TimeFrame,
        candles: &[ProductionCandle],
    ) -> Vec<MetricAP> {
        // Get look back periods for TimeFrame
        let lbps = tf.lbps();
        let n = candles.len();
        let n_i64 = n as i64;
        let datetime = candles[n - 1].datetime;
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
                dec!(0),    // high
                dec!(0),    // low
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
                _h,
                _l,
            ),
             can| {
                // Map close, high and low to their vecs
                vc.push(can.close);
                vh.push(can.high);
                vl.push(can.low);
                vv.push(can.volume);
                va.push(can.value);
                vn.push(can.volume);
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
                (
                    can.close, vc, vh, vl, vv, va, vn, vr, vtr, vuw, vb, vlw, can.high, can.low,
                )
            },
        );
        // Create empty vec to hold metrics
        let mut metrics = Vec::new();
        // Calculate the metrics
        let dons = Metric::dons(&vecs.1, &vecs.2, &vecs.3);
        let ema1 = Metric::ewma(&vecs.1, 7).round_dp(8);
        let ema2 = Metric::ewma(&vecs.1, 30).round_dp(8);
        let ema3 = Metric::ewma(&vecs.1, 90).round_dp(8);
        let mvr = if n_i64.ge(&7) { n - 7 } else { 0 };
        let mv1: Decimal = (vecs.5[mvr..].iter().sum::<Decimal>()
            / vecs.4[mvr..].iter().sum::<Decimal>())
        .round_dp(8);
        let mvr = if n_i64.ge(&30) { n - 30 } else { 0 };
        let mv2: Decimal = (vecs.5[mvr..].iter().sum::<Decimal>()
            / vecs.4[mvr..].iter().sum::<Decimal>())
        .round_dp(8);
        let mvr = if n_i64.ge(&90) { n - 90 } else { 0 };
        let mv3: Decimal = (vecs.5[mvr..].iter().sum::<Decimal>()
            / vecs.4[mvr..].iter().sum::<Decimal>())
        .round_dp(8);
        // For each look back period, calc period specific metrics
        for lbp in lbps.iter() {
            // Set slice ranges
            let range_start = if n_i64.ge(lbp) {
                n - *lbp as usize
            } else {
                usize::MIN
            };
            let range_shift_start = if range_start == 0 { 0 } else { range_start - 1 };
            let range_shift_end = n - 1;
            // println!(
            //     "N / n_i64 / LBP / range_start / rss / rse: {} {} {} {} {} {}",
            //     n, n_i64, lbp, range_start, range_shift_start, range_shift_end
            // );
            // println!("Look Back Period: {}", lbp);
            // Calc metrics
            let atr = Metric::ewma(&vecs.8, *lbp);
            let vw = (vecs.5[range_start..].iter().sum::<Decimal>()
                / vecs.4[range_start..].iter().sum::<Decimal>())
            .round_dp(8);
            let ma = Metric::ewma(&vecs.1[..range_shift_end], *lbp).round_dp(8);
            let ofz = Metric::z(&vecs.6, range_shift_start, range_shift_end).round_dp(2);
            let vz = Metric::z(&vecs.4, range_shift_start, range_shift_end).round_dp(2);
            let rz = Metric::z(&vecs.7, range_shift_start, range_shift_end).round_dp(2);
            let trz = Metric::z(&vecs.8, range_shift_start, range_shift_end).round_dp(2);
            let uwz = Metric::z(&vecs.9, range_shift_start, range_shift_end).round_dp(2);
            let bz = Metric::z(&vecs.10, range_shift_start, range_shift_end).round_dp(2);
            let lwz = Metric::z(&vecs.11, range_shift_start, range_shift_end).round_dp(2);
            let new_metric = MetricAP {
                market_name: market.market_name.clone(),
                exchange_name: market.exchange_name,
                datetime,
                time_frame: tf,
                lbp: *lbp,
                high: vecs.12,
                low: vecs.13,
                close: vecs.0,
                r: vecs.7[n - 1].round_dp(8),
                h004c: dons[0],
                l004c: dons[1],
                h004h: dons[2],
                l004l: dons[3],
                h008c: dons[4],
                l008c: dons[5],
                h008h: dons[6],
                l008l: dons[7],
                h012c: dons[8],
                l012c: dons[9],
                h012h: dons[10],
                l012l: dons[11],
                h024c: dons[12],
                l024c: dons[13],
                h024h: dons[14],
                l024l: dons[15],
                h048c: dons[16],
                l048c: dons[17],
                h048h: dons[18],
                l048l: dons[19],
                h096c: dons[20],
                l096c: dons[21],
                h096h: dons[22],
                l096l: dons[23],
                h192c: dons[24],
                l192c: dons[25],
                h192h: dons[26],
                l192l: dons[27],
                ema1,
                ema2,
                ema3,
                mv1,
                mv2,
                mv3,
                atr: atr.round_dp(8),
                vw,
                ma,
                ofz,
                vz,
                rz,
                trz,
                uwz,
                bz,
                lwz,
            };
            metrics.push(new_metric);
        }
        metrics
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            INSERT INTO metrics_ap (
                exchange_name, market_name, datetime, time_frame, lbp, high, low, close, r, H004R, 
                H004C, L004R,
                L004C, H008R, H008C, L008R, L008C, H012R, H012C, L012R,
                L012C, H024R, H024C, L024R, L024C, H048R, H048C, L048R,
                L048C, H096R, H096C, L096R, L096C, H192R, H192C, L192R,
                L192C, EMA1, EMA2, EMA3, MV1, MV2, MV3,
                ofs, vs, rs, n, trs, uws, mbs, lws, ma, vw, insert_ts)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35,
                $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52,
                $53, now())
            "#;
        sqlx::query(sql)
            .bind(self.exchange_name.as_str())
            .bind(&self.market_name)
            .bind(self.datetime)
            .bind(self.time_frame.as_str())
            .bind(self.lbp)
            .bind(self.high)
            .bind(self.low)
            .bind(self.close)
            .bind(self.r)
            .bind(self.h004h)
            .bind(self.h004c)
            .bind(self.l004l)
            .bind(self.l004c)
            .bind(self.h008h)
            .bind(self.h008c)
            .bind(self.l008l)
            .bind(self.l008c)
            .bind(self.h012h)
            .bind(self.h012c)
            .bind(self.l012l)
            .bind(self.l012c)
            .bind(self.h024h)
            .bind(self.h024c)
            .bind(self.l024l)
            .bind(self.l024c)
            .bind(self.h048h)
            .bind(self.h048c)
            .bind(self.l048l)
            .bind(self.l048c)
            .bind(self.h096h)
            .bind(self.h096c)
            .bind(self.l096l)
            .bind(self.l096c)
            .bind(self.h192h)
            .bind(self.h192c)
            .bind(self.l192l)
            .bind(self.l192c)
            .bind(self.ema1)
            .bind(self.ema2)
            .bind(self.ema3)
            .bind(self.mv1)
            .bind(self.mv2)
            .bind(self.mv3)
            .bind(self.ofz)
            .bind(self.vz)
            .bind(self.rz)
            .bind(self.atr)
            .bind(self.trz)
            .bind(self.uwz)
            .bind(self.bz)
            .bind(self.lwz)
            .bind(self.ma)
            .bind(self.vw)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn delete_by_market(pool: &PgPool, market: &MarketDetail) -> Result<(), sqlx::Error> {
        let sql = r#"
            DELETE FROM metrics_ap
            WHERE exchange_name = $1
            AND market_name = $2
            "#;
        sqlx::query(sql)
            .bind(market.exchange_name.as_str())
            .bind(&market.market_name)
            .execute(pool)
            .await?;
        Ok(())
    }
}

impl ResearchMetric {
    pub fn new(market: &MarketDetail, tf: TimeFrame, candles: &[ProductionCandle]) -> Self {
        let n = candles.len();
        let n_i64 = n as i64;
        let datetime = candles[n - 1].datetime;
        // Iterate through candles and return Vecs of Decimals to use for z calcs
        let vecs = candles.iter().fold(
            (
                Decimal::ZERO, // vecs.0 close
                Vec::new(),    // vecs.1 [close] - to be used for emas
                Vec::new(),    // vecs.2 [high] - to be used for dons
                Vec::new(),    // vecs.3 [low] - to be used for dons
                Vec::new(),    // vecs.4 [volume] - to be used for z
                Vec::new(),    // vecs.5 [return] - to be used for z
                Vec::new(),    // vecs.6 [tr] - to be used for z
                Vec::new(),    // vecs.7 [upper_wick]
                Vec::new(),    // vecs.8 [body]
                Vec::new(),    // vecs.9 [lower_wick]
                Vec::new(),    // vecs.10 [volume_net]
                Vec::new(),    // vecs.11 [volume_pct]
                Vec::new(),    // vecs.12 [volume_liq]
                Vec::new(),    // vecs.13 [volume_liq_net]
                Vec::new(),    // vecs.14 [volume_liq_pct]
                Vec::new(),    // vecs.15 [value]
                Vec::new(),    // vecs.16 [Value_net]
                Vec::new(),    // vecs.17 [value_pct]
                Vec::new(),    // vecs.18 [value_liq]
                Vec::new(),    // vecs.19 [value_liq_net]
                Vec::new(),    // vecs.20 [value_liq_pct]
                Vec::new(),    // vecs.21 [trade_count]
                Vec::new(),    // vecs.22 [trade_count_net]
                Vec::new(),    // vecs.23 [trade_count_pct]
                Vec::new(),    // vecs.24 [liq_count]
                Vec::new(),    // vecs.25 [liq_count_net]
                Vec::new(),    // vecs.26 [liq_count_pct]
                Decimal::ZERO, // vecs.27 high
                Decimal::ZERO, // vecs.28 low
            ),
            |(
                c,
                mut vc,
                mut vh,
                mut vl,
                mut vv,
                mut vr,
                mut vtr,
                mut vuw,
                mut vbod,
                mut vlw,
                mut vvnet,
                mut vvpct,
                mut vvl,
                mut vvlnet,
                mut vvlpct,
                mut vva,
                mut vvanet,
                mut vvapct,
                mut vval,
                mut vvalnet,
                mut vvalpct,
                mut vtc,
                mut vtcnet,
                mut vtcpct,
                mut vlc,
                mut vlcnet,
                mut vlcpct,
                _h,
                _l,
            ),
             can| {
                // Map close, high low etc to their vecs
                vc.push(can.close);
                vh.push(can.high);
                vl.push(can.low);
                vv.push(can.volume);
                // Calc return as current candle close / prev candle close - 1
                let r = if !c.is_zero() {
                    can.close / c - Decimal::ONE
                } else {
                    c
                };
                vr.push(r);
                // Calc tr
                let hl = can.high - can.low;
                let tr = if !c.is_zero() {
                    let hpdc = (can.high - c).abs();
                    let pdcl = (c - can.low).abs();
                    hl.max(hpdc).max(pdcl)
                } else {
                    hl
                };
                vtr.push(tr);
                // Calc wicks
                vuw.push(
                    (can.high - can.open.max(can.close))
                        .checked_div(hl)
                        .unwrap_or(Decimal::ZERO),
                );
                vbod.push(
                    (can.open - can.close)
                        .abs()
                        .checked_div(hl)
                        .unwrap_or(Decimal::ZERO),
                );
                vlw.push(
                    (can.open.min(can.close) - can.low)
                        .checked_div(hl)
                        .unwrap_or(Decimal::ZERO),
                );
                // Volume
                vvnet.push(can.volume_buy - can.volume_sell);
                vvpct.push(
                    can.volume_buy
                        .checked_div(can.volume)
                        .unwrap_or(Decimal::ZERO),
                );
                // Volume Liq
                vvl.push(can.volume_liq);
                vvlnet.push(can.volume_liq_buy - can.volume_liq_sell);
                vvlpct.push(
                    can.volume_liq_buy
                        .checked_div(can.volume_liq)
                        .unwrap_or(Decimal::ZERO),
                );
                // Value
                vva.push(can.value);
                vvanet.push(can.value_buy - can.value_sell);
                vvapct.push(
                    can.value_buy
                        .checked_div(can.value)
                        .unwrap_or(Decimal::ZERO),
                );
                // Value Liq
                vval.push(can.value_liq);
                vvalnet.push(can.value_liq_buy - can.value_liq_sell);
                vvalpct.push(
                    can.value_liq_buy
                        .checked_div(can.value_liq)
                        .unwrap_or(Decimal::ZERO),
                );
                // Trade Count
                vtc.push(Decimal::from_i64(can.trade_count).unwrap());
                vtcnet.push(Decimal::from_i64(can.trade_count_buy - can.trade_count_sell).unwrap());
                vtcpct.push(
                    Decimal::from_i64(
                        can.trade_count_buy
                            .checked_div(can.trade_count)
                            .unwrap_or(0),
                    )
                    .unwrap(),
                );
                // Trade Count Liq
                vlc.push(Decimal::from_i64(can.liq_count).unwrap());
                vlcnet.push(Decimal::from_i64(can.liq_count_buy - can.liq_count_sell).unwrap());
                vlcpct.push(
                    Decimal::from_i64(can.liq_count_buy.checked_div(can.liq_count).unwrap_or(0))
                        .unwrap(),
                );
                (
                    can.close, vc, vh, vl, vv, vr, vtr, vuw, vbod, vlw, vvnet, vvpct, vvl, vvlnet,
                    vvlpct, vva, vvanet, vvapct, vval, vvalnet, vvalpct, vtc, vtcnet, vtcpct, vlc,
                    vlcnet, vlcpct, can.high, can.low,
                )
            },
        );
        // Set filters
        let direction = match vecs.7.last() {
            Some(r) => match r.cmp(&Decimal::ZERO) {
                Ordering::Less => MetricDirection::Down,
                Ordering::Equal => MetricDirection::NC,
                Ordering::Greater => MetricDirection::Up,
            },
            None => MetricDirection::NC,
        };
        let ma_l = Metric::ewma(&vecs.1, tf.lbp_l());
        let ma_s = Metric::ewma(&vecs.1, tf.lbp_s());
        let ma_filter = match ma_l.cmp(&ma_s) {
            Ordering::Greater => MetricFilter::LS,
            Ordering::Less => MetricFilter::SL,
            Ordering::Equal => MetricFilter::Equal,
        };
        // Set atrs
        let atr_l = Metric::ewma(&vecs.6, tf.lbp_l()).round_dp(8);
        let atr_s = Metric::ewma(&vecs.6, tf.lbp_s()).round_dp(8);
        let atr_filter = match atr_l.cmp(&atr_s) {
            Ordering::Greater => MetricFilter::LS,
            Ordering::Less => MetricFilter::SL,
            Ordering::Equal => MetricFilter::Equal,
        };
        // Set slice ranges
        let range_start_l = if n_i64.ge(&tf.lbp_l()) {
            n - tf.lbp_l() as usize
        } else {
            usize::MIN
        };
        let range_start_s = if n_i64.ge(&tf.lbp_s()) {
            n - tf.lbp_s() as usize
        } else {
            usize::MIN
        };
        let range_end = n;
        // Set z scores
        let return_z_l = Metric::z(&vecs.5, range_start_l, range_end).round_dp(4);
        let return_z_s = Metric::z(&vecs.5, range_start_s, range_end).round_dp(4);
        let tr_z_l = Metric::z(&vecs.6, range_start_l, range_end).round_dp(4);
        let tr_z_s = Metric::z(&vecs.6, range_start_s, range_end).round_dp(4);
        let upper_wick_z_l = Metric::z(&vecs.7, range_start_l, range_end).round_dp(4);
        let upper_wick_z_s = Metric::z(&vecs.7, range_start_s, range_end).round_dp(4);
        let body_z_l = Metric::z(&vecs.8, range_start_l, range_end).round_dp(4);
        let body_z_s = Metric::z(&vecs.8, range_start_s, range_end).round_dp(4);
        let lower_wick_z_l = Metric::z(&vecs.9, range_start_l, range_end).round_dp(4);
        let lower_wick_z_s = Metric::z(&vecs.9, range_start_s, range_end).round_dp(4);
        let volume_z_l = Metric::z(&vecs.4, range_start_l, range_end).round_dp(4);
        let volume_z_s = Metric::z(&vecs.4, range_start_s, range_end).round_dp(4);
        let volume_net_z_l = Metric::z(&vecs.10, range_start_l, range_end).round_dp(4);
        let volume_net_z_s = Metric::z(&vecs.10, range_start_s, range_end).round_dp(4);
        let volume_pct_z_l = Metric::z(&vecs.11, range_start_l, range_end).round_dp(4);
        let volume_pct_z_s = Metric::z(&vecs.11, range_start_s, range_end).round_dp(4);
        let volume_liq_z_l = Metric::z(&vecs.12, range_start_l, range_end).round_dp(4);
        let volume_liq_z_s = Metric::z(&vecs.12, range_start_s, range_end).round_dp(4);
        let volume_liq_net_z_l = Metric::z(&vecs.13, range_start_l, range_end).round_dp(4);
        let volume_liq_net_z_s = Metric::z(&vecs.13, range_start_s, range_end).round_dp(4);
        let volume_liq_pct_z_l = Metric::z(&vecs.14, range_start_l, range_end).round_dp(4);
        let volume_liq_pct_z_s = Metric::z(&vecs.14, range_start_s, range_end).round_dp(4);
        let value_z_l = Metric::z(&vecs.15, range_start_l, range_end).round_dp(4);
        let value_z_s = Metric::z(&vecs.15, range_start_s, range_end).round_dp(4);
        let value_net_z_l = Metric::z(&vecs.16, range_start_l, range_end).round_dp(4);
        let value_net_z_s = Metric::z(&vecs.16, range_start_s, range_end).round_dp(4);
        let value_pct_z_l = Metric::z(&vecs.17, range_start_l, range_end).round_dp(4);
        let value_pct_z_s = Metric::z(&vecs.17, range_start_s, range_end).round_dp(4);
        let value_liq_z_l = Metric::z(&vecs.18, range_start_l, range_end).round_dp(4);
        let value_liq_z_s = Metric::z(&vecs.18, range_start_s, range_end).round_dp(4);
        let value_liq_net_z_l = Metric::z(&vecs.19, range_start_l, range_end).round_dp(4);
        let value_liq_net_z_s = Metric::z(&vecs.19, range_start_s, range_end).round_dp(4);
        let value_liq_pct_z_l = Metric::z(&vecs.20, range_start_l, range_end).round_dp(4);
        let value_liq_pct_z_s = Metric::z(&vecs.20, range_start_s, range_end).round_dp(4);
        let trade_count_z_l = Metric::z(&vecs.21, range_start_l, range_end).round_dp(4);
        let trade_count_z_s = Metric::z(&vecs.21, range_start_s, range_end).round_dp(4);
        let trade_count_net_z_l = Metric::z(&vecs.22, range_start_l, range_end).round_dp(4);
        let trade_count_net_z_s = Metric::z(&vecs.22, range_start_s, range_end).round_dp(4);
        let trade_count_pct_z_l = Metric::z(&vecs.23, range_start_l, range_end).round_dp(4);
        let trade_count_pct_z_s = Metric::z(&vecs.23, range_start_s, range_end).round_dp(4);
        let liq_count_z_l = Metric::z(&vecs.24, range_start_l, range_end).round_dp(4);
        let liq_count_z_s = Metric::z(&vecs.24, range_start_s, range_end).round_dp(4);
        let liq_count_net_z_l = Metric::z(&vecs.25, range_start_l, range_end).round_dp(4);
        let liq_count_net_z_s = Metric::z(&vecs.25, range_start_s, range_end).round_dp(4);
        let liq_count_pct_z_l = Metric::z(&vecs.26, range_start_l, range_end).round_dp(4);
        let liq_count_pct_z_s = Metric::z(&vecs.26, range_start_s, range_end).round_dp(4);
        Self {
            market_id: market.market_id,
            tf,
            datetime,
            high: vecs.27,
            low: vecs.28,
            close: vecs.0,
            atr_l,
            atr_s,
            ma_filter,
            atr_filter,
            direction,
            return_z_l,
            return_z_s,
            tr_z_l,
            tr_z_s,
            upper_wick_z_l,
            upper_wick_z_s,
            body_z_l,
            body_z_s,
            lower_wick_z_l,
            lower_wick_z_s,
            volume_z_l,
            volume_z_s,
            volume_net_z_l,
            volume_net_z_s,
            volume_pct_z_l,
            volume_pct_z_s,
            volume_liq_z_l,
            volume_liq_z_s,
            volume_liq_net_z_l,
            volume_liq_net_z_s,
            volume_liq_pct_z_l,
            volume_liq_pct_z_s,
            value_z_l,
            value_z_s,
            value_net_z_l,
            value_net_z_s,
            value_pct_z_l,
            value_pct_z_s,
            value_liq_z_l,
            value_liq_z_s,
            value_liq_net_z_l,
            value_liq_net_z_s,
            value_liq_pct_z_l,
            value_liq_pct_z_s,
            trade_count_z_l,
            trade_count_z_s,
            trade_count_net_z_l,
            trade_count_net_z_s,
            trade_count_pct_z_l,
            trade_count_pct_z_s,
            liq_count_z_l,
            liq_count_z_s,
            liq_count_net_z_l,
            liq_count_net_z_s,
            liq_count_pct_z_l,
            liq_count_pct_z_s,
        }
    }

    pub fn from_file(pb: &PathBuf) -> Vec<Self> {
        let file = File::open(pb).expect("Failed to open file.");
        let mut metrics = Vec::new();
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: ResearchMetric = result.expect("Failed to deserialize record.");
            metrics.push(record);
        }
        metrics
    }

    async fn _create_table(pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            CREATE TABLE research_metrics (
                market_id uuid NOT NULL,
                tf TEXT NOT NULL,
                datetime timestamptz NOT NULL,
                high NUMERIC NOT NULL,
                low NUMERIC NOT NULL,
                close NUMERIC NOT NULL,
                atr_l NUMERIC NOT NULL,
                atr_s NUMERIC NOT NULL,
                ma_filter TEXT NOT NULL,
                atr_filter TEXT NOT NULL,
                direction TEXT NOT NULL,
                return_z_l NUMERIC NOT NULL,
                return_z_s NUMERIC NOT NULL,
                tr_z_l NUMERIC NOT NULL,
                tr_z_s NUMERIC NOT NULL,
                upper_wick_z_l NUMERIC NOT NULL,
                upper_wick_z_s NUMERIC NOT NULL,
                body_z_l NUMERIC NOT NULL,
                body_z_s NUMERIC NOT NULL,
                lower_wick_z_l NUMERIC NOT NULL,
                lower_wick_z_s NUMERIC NOT NULL,
                volume_z_l NUMERIC NOT NULL,
                volume_z_s NUMERIC NOT NULL,
                volume_net_z_l NUMERIC NOT NULL,
                volume_net_z_s NUMERIC NOT NULL,
                volume_pct_z_l NUMERIC NOT NULL,
                volume_pct_z_s NUMERIC NOT NULL,
                volume_liq_z_l NUMERIC NOT NULL,
                volume_liq_z_s NUMERIC NOT NULL,
                volume_liq_net_z_l NUMERIC NOT NULL,
                volume_liq_net_z_s NUMERIC NOT NULL,
                volume_liq_pct_z_l NUMERIC NOT NULL,
                volume_liq_pct_z_s NUMERIC NOT NULL,
                value_z_l NUMERIC NOT NULL,
                value_z_s NUMERIC NOT NULL,
                value_net_z_l NUMERIC NOT NULL,
                value_net_z_s NUMERIC NOT NULL,
                value_pct_z_l NUMERIC NOT NULL,
                value_pct_z_s NUMERIC NOT NULL,
                value_liq_z_l NUMERIC NOT NULL,
                value_liq_z_s NUMERIC NOT NULL,
                value_liq_net_z_l NUMERIC NOT NULL,
                value_liq_net_z_s NUMERIC NOT NULL,
                value_liq_pct_z_l NUMERIC NOT NULL,
                value_liq_pct_z_s NUMERIC NOT NULL,
                trade_count_z_l NUMERIC NOT NULL,
                trade_count_z_s NUMERIC NOT NULL,
                trade_count_net_z_l NUMERIC NOT NULL,
                trade_count_net_z_s NUMERIC NOT NULL,
                trade_count_pct_z_l NUMERIC NOT NULL,
                trade_count_pct_z_s NUMERIC NOT NULL,
                liq_count_z_l NUMERIC NOT NULL,
                liq_count_z_s NUMERIC NOT NULL,
                liq_count_net_z_l NUMERIC NOT NULL,
                liq_count_net_z_s NUMERIC NOT NULL,
                liq_count_pct_z_l NUMERIC NOT NULL,
                liq_count_pct_z_s NUMERIC NOT NULL,
                insert_dt timestamptz NOT NULL
            )
            "#;
        sqlx::query(sql).execute(pool).await?;
        Ok(())
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            INSERT INTO research_metrics (
                market_id, tf, datetime, high, low, close, atr_l, atr_s, ma_filter, atr_filter, direction,
                return_z_l, return_z_s, tr_z_l, tr_z_s, upper_wick_z_l, upper_wick_z_s, body_z_l,
                body_z_s, lower_wick_z_l, lower_wick_z_s, volume_z_l, volume_z_s, volume_net_z_l,
                volume_net_z_s, volume_pct_z_l, volume_pct_z_s, volume_liq_z_l, volume_liq_z_s,
                volume_liq_net_z_l, volume_liq_net_z_s, volume_liq_pct_z_l, volume_liq_pct_z_s,
                value_z_l, value_z_s, value_net_z_l, value_net_z_s, value_pct_z_l, value_pct_z_s,
                value_liq_z_l, value_liq_z_s, value_liq_net_z_l, value_liq_net_z_s,
                value_liq_pct_z_l, value_liq_pct_z_s, trade_count_z_l, trade_count_z_s,
                trade_count_net_z_l, trade_count_net_z_s, trade_count_pct_z_l, trade_count_pct_z_s,
                liq_count_z_l, liq_count_z_s, liq_count_net_z_l, liq_count_net_z_s,
                liq_count_pct_z_l, liq_count_pct_z_s, insert_dt
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
                $51, $52, $53, $54, $55, $56, $57, now()
            )
            "#;
        sqlx::query(sql)
            .bind(self.market_id)
            .bind(self.tf.as_str())
            .bind(self.datetime)
            .bind(self.high)
            .bind(self.low)
            .bind(self.close)
            .bind(self.atr_l)
            .bind(self.atr_s)
            .bind(self.ma_filter.as_str())
            .bind(self.atr_filter.as_str())
            .bind(self.direction.as_str())
            .bind(self.return_z_l)
            .bind(self.return_z_s)
            .bind(self.tr_z_l)
            .bind(self.tr_z_s)
            .bind(self.upper_wick_z_l)
            .bind(self.upper_wick_z_s)
            .bind(self.body_z_l)
            .bind(self.body_z_s)
            .bind(self.lower_wick_z_l)
            .bind(self.lower_wick_z_s)
            .bind(self.volume_z_l)
            .bind(self.volume_z_s)
            .bind(self.volume_net_z_l)
            .bind(self.volume_net_z_s)
            .bind(self.volume_pct_z_l)
            .bind(self.volume_pct_z_s)
            .bind(self.volume_liq_z_l)
            .bind(self.volume_liq_z_s)
            .bind(self.volume_liq_net_z_l)
            .bind(self.volume_liq_net_z_s)
            .bind(self.volume_liq_pct_z_l)
            .bind(self.volume_liq_pct_z_s)
            .bind(self.value_z_l)
            .bind(self.value_z_s)
            .bind(self.value_net_z_l)
            .bind(self.value_net_z_s)
            .bind(self.value_pct_z_l)
            .bind(self.value_pct_z_s)
            .bind(self.value_liq_z_l)
            .bind(self.value_liq_z_s)
            .bind(self.value_liq_net_z_l)
            .bind(self.value_liq_net_z_s)
            .bind(self.value_liq_pct_z_l)
            .bind(self.value_liq_pct_z_s)
            .bind(self.trade_count_z_l)
            .bind(self.trade_count_z_s)
            .bind(self.trade_count_net_z_l)
            .bind(self.trade_count_net_z_s)
            .bind(self.trade_count_pct_z_l)
            .bind(self.trade_count_pct_z_s)
            .bind(self.liq_count_z_l)
            .bind(self.liq_count_z_s)
            .bind(self.liq_count_net_z_l)
            .bind(self.liq_count_net_z_s)
            .bind(self.liq_count_pct_z_l)
            .bind(self.liq_count_pct_z_s)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn select_all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                tf as "tf: TimeFrame",
                datetime, high, low, close, atr_l, atr_s, 
                ma_filter as "ma_filter: MetricFilter",
                atr_filter as "atr_filter: MetricFilter", 
                direction as "direction: MetricDirection",
                return_z_l, return_z_s, tr_z_l, tr_z_s, upper_wick_z_l, upper_wick_z_s, body_z_l,
                body_z_s, lower_wick_z_l, lower_wick_z_s, volume_z_l, volume_z_s, volume_net_z_l,
                volume_net_z_s, volume_pct_z_l, volume_pct_z_s, volume_liq_z_l, volume_liq_z_s,
                volume_liq_net_z_l, volume_liq_net_z_s, volume_liq_pct_z_l, volume_liq_pct_z_s,
                value_z_l, value_z_s, value_net_z_l, value_net_z_s, value_pct_z_l, value_pct_z_s,
                value_liq_z_l, value_liq_z_s, value_liq_net_z_l, value_liq_net_z_s,
                value_liq_pct_z_l, value_liq_pct_z_s, trade_count_z_l, trade_count_z_s,
                trade_count_net_z_l, trade_count_net_z_s, trade_count_pct_z_l, trade_count_pct_z_s,
                liq_count_z_l, liq_count_z_s, liq_count_net_z_l, liq_count_net_z_s,
                liq_count_pct_z_l, liq_count_pct_z_s
            FROM research_metrics
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_id(pool: &PgPool, market_id: &Uuid) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                tf as "tf: TimeFrame",
                datetime, high, low, close, atr_l, atr_s, 
                ma_filter as "ma_filter: MetricFilter",
                atr_filter as "atr_filter: MetricFilter", 
                direction as "direction: MetricDirection",
                return_z_l, return_z_s, tr_z_l, tr_z_s, upper_wick_z_l, upper_wick_z_s, body_z_l,
                body_z_s, lower_wick_z_l, lower_wick_z_s, volume_z_l, volume_z_s, volume_net_z_l,
                volume_net_z_s, volume_pct_z_l, volume_pct_z_s, volume_liq_z_l, volume_liq_z_s,
                volume_liq_net_z_l, volume_liq_net_z_s, volume_liq_pct_z_l, volume_liq_pct_z_s,
                value_z_l, value_z_s, value_net_z_l, value_net_z_s, value_pct_z_l, value_pct_z_s,
                value_liq_z_l, value_liq_z_s, value_liq_net_z_l, value_liq_net_z_s,
                value_liq_pct_z_l, value_liq_pct_z_s, trade_count_z_l, trade_count_z_s,
                trade_count_net_z_l, trade_count_net_z_s, trade_count_pct_z_l, trade_count_pct_z_s,
                liq_count_z_l, liq_count_z_s, liq_count_net_z_l, liq_count_net_z_s,
                liq_count_pct_z_l, liq_count_pct_z_s
            FROM research_metrics
            WHERE market_id = $1
            "#,
            market_id
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_ids(
        pool: &PgPool,
        market_ids: &[Uuid],
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                tf as "tf: TimeFrame",
                datetime, high, low, close, atr_l, atr_s, 
                ma_filter as "ma_filter: MetricFilter",
                atr_filter as "atr_filter: MetricFilter", 
                direction as "direction: MetricDirection",
                return_z_l, return_z_s, tr_z_l, tr_z_s, upper_wick_z_l, upper_wick_z_s, body_z_l,
                body_z_s, lower_wick_z_l, lower_wick_z_s, volume_z_l, volume_z_s, volume_net_z_l,
                volume_net_z_s, volume_pct_z_l, volume_pct_z_s, volume_liq_z_l, volume_liq_z_s,
                volume_liq_net_z_l, volume_liq_net_z_s, volume_liq_pct_z_l, volume_liq_pct_z_s,
                value_z_l, value_z_s, value_net_z_l, value_net_z_s, value_pct_z_l, value_pct_z_s,
                value_liq_z_l, value_liq_z_s, value_liq_net_z_l, value_liq_net_z_s,
                value_liq_pct_z_l, value_liq_pct_z_s, trade_count_z_l, trade_count_z_s,
                trade_count_net_z_l, trade_count_net_z_s, trade_count_pct_z_l, trade_count_pct_z_s,
                liq_count_z_l, liq_count_z_s, liq_count_net_z_l, liq_count_net_z_s,
                liq_count_pct_z_l, liq_count_pct_z_s
            FROM research_metrics
            WHERE market_id = ANY($1)
            "#,
            market_ids
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn delete_by_market(pool: &PgPool, market: &MarketDetail) -> Result<(), sqlx::Error> {
        let sql = r#"
            DELETE FROM research_metrics
            WHERE market_id = $1
            "#;
        sqlx::query(sql)
            .bind(market.market_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub fn map_by_id_tf(metrics: &[Self]) -> HashMap<Uuid, HashMap<TimeFrame, Vec<Self>>> {
        let mut map: HashMap<Uuid, HashMap<TimeFrame, Vec<ResearchMetric>>> = HashMap::new();
        for metric in metrics.iter() {
            map.entry(metric.market_id)
                .and_modify(|hm| {
                    hm.entry(metric.tf)
                        .and_modify(|v| v.push(metric.clone()))
                        .or_insert_with(|| vec![metric.clone()]);
                })
                .or_insert_with(|| HashMap::from([(metric.tf, vec![metric.clone()])]));
        }
        map
    }

    pub fn z_l(&self, metric: &str) -> Decimal {
        match metric {
            "return" => self.return_z_l,
            "tr" => self.tr_z_l,
            "upper_wick" => self.upper_wick_z_l,
            "body" => self.body_z_l,
            "lower_wick" => self.lower_wick_z_l,
            "volume" => self.volume_z_l,
            "volume_net" => self.value_net_z_l,
            "volume_pct" => self.volume_pct_z_l,
            "volume_liq" => self.volume_liq_z_l,
            "volume_liq_net" => self.volume_liq_net_z_l,
            "volume_liq_pct" => self.volume_liq_pct_z_l,
            "value" => self.value_z_l,
            "value_net" => self.value_net_z_l,
            "value_pct" => self.value_pct_z_l,
            "value_liq" => self.value_liq_z_l,
            "value_liq_net" => self.value_liq_net_z_l,
            "value_liq_pct" => self.value_liq_pct_z_l,
            "trade_count" => self.trade_count_z_l,
            "trade_count_net" => self.trade_count_net_z_l,
            "trade_count_pct" => self.trade_count_pct_z_l,
            "liq_count" => self.liq_count_z_l,
            "liq_count_net" => self.liq_count_net_z_l,
            "liq_count_pct" => self.liq_count_pct_z_l,
            _ => panic!("{} not a valid metric.", metric),
        }
    }

    pub fn z_s(&self, metric: &str) -> Decimal {
        match metric {
            "return" => self.return_z_s,
            "tr" => self.tr_z_s,
            "upper_wick" => self.upper_wick_z_s,
            "body" => self.body_z_s,
            "lower_wick" => self.lower_wick_z_s,
            "volume" => self.volume_z_s,
            "volume_net" => self.value_net_z_s,
            "volume_pct" => self.volume_pct_z_s,
            "volume_liq" => self.volume_liq_z_s,
            "volume_liq_net" => self.volume_liq_net_z_s,
            "volume_liq_pct" => self.volume_liq_pct_z_s,
            "value" => self.value_z_s,
            "value_net" => self.value_net_z_s,
            "value_pct" => self.value_pct_z_s,
            "value_liq" => self.value_liq_z_s,
            "value_liq_net" => self.value_liq_net_z_s,
            "value_liq_pct" => self.value_liq_pct_z_s,
            "trade_count" => self.trade_count_z_s,
            "trade_count_net" => self.trade_count_net_z_s,
            "trade_count_pct" => self.trade_count_pct_z_s,
            "liq_count" => self.liq_count_z_s,
            "liq_count_net" => self.liq_count_net_z_s,
            "liq_count_pct" => self.liq_count_pct_z_s,
            _ => panic!("{} not a valid metric.", metric),
        }
    }
}

impl ElDorado {
    pub async fn insert_metrics(&self, metrics: &[ResearchMetric]) -> Result<(), ElDoradoError> {
        for metric in metrics.iter() {
            metric.insert(&self.pools[&Database::ElDorado]).await?;
        }
        Ok(())
    }

    pub fn calc_metrics_all_tfs(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) -> Vec<ResearchMetric> {
        let mut metrics = Vec::new();
        for tf in TimeFrame::tfs().iter() {
            println!("Calculating metrics for {} - {}", market.market_name, tf);
            let tf_metric = ResearchMetric::new(
                market,
                *tf,
                heartbeats
                    .get(&market.market_name)
                    .unwrap()
                    .candles
                    .get(tf)
                    .unwrap(),
            );
            metrics.push(tf_metric)
        }
        metrics
    }
}

#[cfg(test)]
mod tests {
    use crate::{configuration::Database, eldorado::ElDorado, metrics::ResearchMetric};
    use uuid::Uuid;

    #[tokio::test]
    pub async fn research_metric_etl() {
        // Using the sample research metrics file in the test directory:
        //  1) Read file into ResearchMetric struct vec
        //  2) Write vec to database
        //  3) Read rows from database into struc vec
        //  4) Read file into backtest struct
        //  5) Read file into live struct
        // 1)
        let fp = std::path::Path::new("tests").join("AVAXUSD_d01_202205.csv");
        let metrics = ResearchMetric::from_file(&fp);
        // 2)
        let eld = ElDorado::new().await.unwrap();
        let pool = &eld.pools[&Database::ElDorado];
        let drop = "DROP TABLE IF EXISTS research_metrics";
        sqlx::query(drop)
            .execute(pool)
            .await
            .expect("Failed to drop table.");
        ResearchMetric::_create_table(pool)
            .await
            .expect("Failed to insert.");
        for metric in metrics.iter() {
            metric.insert(pool).await.expect("Fialed to insert.");
        }
        // 3)
        let metrics = ResearchMetric::select_all(pool)
            .await
            .expect("Failed to select all.");
        println!("First Metric: {:?}", metrics.first());
        let market_id = Uuid::parse_str("2e6c07eb-2bef-42d1-af74-620ed5879a89").unwrap();
        let metrics = ResearchMetric::select_by_id(pool, &market_id)
            .await
            .expect("Failed to select metrrics.");
        println!("First Metric: {:?}", metrics.first());
        let market_ids = vec![market_id];
        let metrics = ResearchMetric::select_by_ids(pool, &market_ids)
            .await
            .expect("Failed to select metrics.");
        println!("First Metric: {:?}", metrics.first());
        let mapped_metrics = ResearchMetric::map_by_id_tf(&metrics);
        println!("Mapped metrics: {:?}", mapped_metrics);
    }

    // #[tokio::test]
    // pub async fn new_metrics_calculations_and_times() {
    //     // Select set of candles and calc metrics for them.
    //     // Expand to 91 days scenario and calculations for all time frame and lbps
    //     // Exclude time to fetch candles from database in this calc.
    //     // Load configuration
    //     let configuration = get_configuration().expect("Failed to read configuration.");
    //     println!("Configuration: {:?}", configuration);
    //     // Create db connection
    //     let pool = PgPool::connect_with(configuration.ftx_db.with_db())
    //         .await
    //         .expect("Failed to connect to Postgres.");
    //     // Get candles from db
    //     let start = Utc.ymd(2021, 09, 25).and_hms(0, 0, 0);
    //     let candles = select_candles_gte_datetime(
    //         &pool,
    //         &ExchangeName::FtxUs,
    //         &Uuid::parse_str("2246c870-769f-44a4-b989-ffa2de37f8b1").unwrap(),
    //         start,
    //     )
    //     .await
    //     .expect("Failed to select candles.");
    //     println!("Num Candles: {:?}", candles.len());
    //     let timer_start = Utc::now();
    //     let metrics = MetricAP::new("BTC-PERP", &ExchangeName::Ftx, TimeFrame::T15, &candles);
    //     let timer_end = Utc::now();
    //     let time_elapsed = timer_end - timer_start;
    //     println!("Time to calc: {:?}", time_elapsed);
    //     println!("Metrics: {:?}", metrics);
    //     for metric in metrics.iter() {
    //         insert_metric_ap(&pool, metric)
    //             .await
    //             .expect("Failed to insert metric ap.");
    //     }
    // }

    // #[tokio::test]
    // pub async fn calc_daily_ema() {
    //     // Get daily BTC-PERP candles from FTX
    //     let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
    //     let mut candles = client
    //         .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
    //         .await
    //         .expect("Failed to get candles.");
    //     // Sort candles and put close prices into vector
    //     candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
    //     let vc: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
    //     // Calc the EMA
    //     let period = 90 as i64;
    //     let ewma = Metric::ewma(&vc, period);
    //     println!("Candle Closes: {:?}", vc);
    //     println!("EWMA {}: {:?}", period, ewma);
    // }

    // #[tokio::test]
    // pub async fn calc_daily_ema_shift() {
    //     // Get daily BTC-PERP candles from FTX
    //     let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
    //     let mut candles = client
    //         .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
    //         .await
    //         .expect("Failed to get candles.");
    //     // Sort candles and put close prices into vector
    //     candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
    //     let vc: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
    //     // Calc the EMA
    //     let period = 90 as i64;
    //     let n = vc.len();
    //     let ewma = Metric::ewma(&vc[..n - 1], period);
    //     println!("Candle Closes: {:?}", vc);
    //     println!("EWMA {}: {:?}", period, ewma);
    // }

    // #[tokio::test]
    // pub async fn calc_atr() {
    //     // Get daily BTC-PERP candles from FTX
    //     let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
    //     let mut candles = client
    //         .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
    //         .await
    //         .expect("Failed to get candles.");
    //     // Sort candles and put tr values into vector
    //     candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
    //     let vtr = candles
    //         .iter()
    //         .fold((dec!(0), Vec::new()), |(c, mut vtr), can| {
    //             let hl = can.high - can.low;
    //             let tr = match c.is_zero() {
    //                 true => hl,
    //                 false => {
    //                     let hpdc = (can.high - c).abs();
    //                     let pdcl = (c - can.low).abs();
    //                     hl.max(hpdc).max(pdcl)
    //                 }
    //             };
    //             vtr.push(tr);
    //             (can.close, vtr)
    //         });
    //     // Calc the EMA
    //     let period = 14 as i64;
    //     let ewma = Metric::ewma(&vtr.1, period);
    //     println!("Candle TRs: {:?}", vtr.1);
    //     println!("ATR {}: {:?}", period, ewma);
    // }

    // #[tokio::test]
    // pub async fn calc_z_return() {
    //     // Get daily BTC-PERP candles from FTX
    //     let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
    //     let mut candles = client
    //         .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
    //         .await
    //         .expect("Failed to get candles.");
    //     // Sort candles and put tr values into vector
    //     candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
    //     let vr = candles
    //         .iter()
    //         .fold((dec!(0), Vec::new()), |(c, mut vr), can| {
    //             let r = match c.is_zero() {
    //                 true => dec!(0),
    //                 false => can.close / c - dec!(1),
    //             };
    //             vr.push(r);
    //             (can.close, vr)
    //         });
    //     // Calc the z-score of return
    //     let lbp = 90;
    //     let n = vr.1.len();
    //     let range_start = n - lbp as usize;
    //     let range_shift_start = range_start - 1;
    //     let range_shift_end = n - 1;
    //     let z = Metric::z(&vr.1, range_shift_start, range_shift_end);
    //     println!("vr: {:?}", vr.1);
    //     println!("z: {:?}", z);
    // }

    // #[tokio::test]
    // pub async fn calc_dons() {
    //     todo!("Use sample file and calc / compare");
    //     // Get daily BTC-PERP candles from FTX
    //     let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
    //     let mut candles = client
    //         .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
    //         .await
    //         .expect("Failed to get candles.");
    //     // Sort candles and put close prices into vector
    //     candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
    //     let vc: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
    //     // let slice = &vc[0..50];
    //     let dons = Metric::dons(&vc, &vc, &vc);
    //     println!("Closes: {:?}", vc);
    //     println!("Dons: {:?}", dons);
    // }

    // #[tokio::test]
    // pub async fn select_metrics_ap_by_exchange_market_maps_to_struct() {
    //     // Load configuration
    //     let configuration = get_configuration().expect("Failed to read configuration.");
    //     println!("Configuration: {:?}", configuration);
    //     // Create db connection
    //     let pool = PgPool::connect_with(configuration.ed_db.with_db())
    //         .await
    //         .expect("Failed to connect to Postgres.");
    //     // Create list of market names
    //     let markets = vec![
    //         "BTC-PERP".to_string(),
    //         "ETH-PERP".to_string(),
    //         "SOL-PERP".to_string(),
    //     ];
    //     // Select metrics for markets
    //     let metrics = MetricAP::select_by_exchange_market(&pool, &ExchangeName::Ftx, &markets)
    //         .await
    //         .expect("Failed to select metrics.");
    //     println!("{:?}", metrics);
    // }
}
