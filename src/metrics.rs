use crate::candles::{Candle, TimeFrame};
use crate::exchanges::ExchangeName;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
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

#[derive(Debug)]
pub struct Metric {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub datetime: DateTime<Utc>,
    pub time_frame: TimeFrame,
    pub lbp: i64,
    pub close: Decimal,
    pub r: Decimal,
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

impl Metric {
    // Takes vec of candles for a time frame and calculates metrics for the period
    pub fn new(exchange: &ExchangeName, tf: TimeFrame, candles: &[Candle]) -> Vec<Metric> {
        // Get look back periods for TimeFrame
        let lbps = tf.lbps();
        let n = candles.len();
        let datetime = candles[n - 1].datetime + Duration::seconds(tf.as_secs());
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
        // Create empty vec to hold metrics
        let mut metrics = Vec::new();
        // Calculate the metrics
        let dons = Metric::dons(&vecs.1, &vecs.2, &vecs.3);
        let ema1 = Metric::ewma(&vecs.1, 7);
        let ema2 = Metric::ewma(&vecs.1, 30);
        let ema3 = Metric::ewma(&vecs.1, 90);
        let mv1: Decimal =
            vecs.5[n - 7..].iter().sum::<Decimal>() / vecs.4[n - 7..].iter().sum::<Decimal>();
        let mv2: Decimal =
            vecs.5[n - 30..].iter().sum::<Decimal>() / vecs.4[n - 30..].iter().sum::<Decimal>();
        let mv3: Decimal =
            vecs.5[n - 90..].iter().sum::<Decimal>() / vecs.4[n - 90..].iter().sum::<Decimal>();
        // For each look back period, calc period specific metrics
        for lbp in &lbps[0..1] {
            // Set slice ranges
            let range_start = n - *lbp as usize;
            let range_shift_start = range_start - 1;
            let range_shift_end = n - 1;
            println!("Look Back Period: {}", lbp);
            // Calc metrics
            let atr = Metric::ewma(&vecs.8, *lbp);
            let vw = vecs.5[range_start..].iter().sum::<Decimal>()
                / vecs.4[range_start..].iter().sum::<Decimal>();
            let ma = Metric::ewma(&vecs.1[..range_shift_end], *lbp);
            let ofz = Metric::z(&vecs.6, range_shift_start, range_shift_end);
            let vz = Metric::z(&vecs.4, range_shift_start, range_shift_end);
            let rz = Metric::z(&vecs.7, range_shift_start, range_shift_end);
            let trz = Metric::z(&vecs.8, range_shift_start, range_shift_end);
            let uwz = Metric::z(&vecs.9, range_shift_start, range_shift_end);
            let bz = Metric::z(&vecs.10, range_shift_start, range_shift_end);
            let lwz = Metric::z(&vecs.11, range_shift_start, range_shift_end);
            let new_metric = Metric {
                market_id: candles[0].market_id,
                exchange_name: *exchange,
                datetime,
                time_frame: tf,
                lbp: *lbp,
                close: vecs.0,
                r: vecs.7[n - 1],
                ema1,
                ema2,
                ema3,
                mv1,
                mv2,
                mv3,
                atr,
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

    pub fn ewma(v: &[Decimal], lbp: i64) -> Decimal {
        // Set k = smoothing factor
        let k = dec!(2) / (Decimal::from_i64(lbp).unwrap() + dec!(1));
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
        let shift_mean = v[rs..re].iter().sum::<Decimal>() / n;
        let shift_sd = (v[rs..re]
            .iter()
            .fold(dec!(0), |s, x| s + (shift_mean - x).powi(2))
            / n)
            .sqrt()
            .unwrap();
        (v[v.len() - 1] - shift_mean) / shift_sd
    }

    pub fn dons(c: &[Decimal], _h: &[Decimal], _l: &[Decimal]) -> Vec<Decimal> {
        // For each of the ranges below, calc the higheset and lowest value
        let mut dons = Vec::new();
        let ranges = [4, 8, 12, 24, 48, 96, 192];
        // Set min and max to last elexment of vecs (first item to check)
        let mut i = 1;
        let mut min = c[c.len() - i];
        let mut max = c[c.len() - i];
        i += 1;
        // For each item in range (don window), check min and max until next range
        for range in ranges.iter() {
            while i <= *range as usize {
                // Compare current min/max to len()-i value
                min = min.min(c[c.len() - i]);
                max = max.max(c[c.len() - i]);
                i += 1;
            }
            dons.push(max);
            dons.push(min);
        }
        dons
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
        let start = Utc.ymd(2021, 09, 25).and_hms(0, 0, 0);
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
        let metrics = Metric::new(&ExchangeName::Ftx, TimeFrame::T15, &candles);
        let timer_end = Utc::now();
        let time_elapsed = timer_end - timer_start;
        println!("Time to calc: {:?}", time_elapsed);
        println!("Metrics: {:?}", metrics);
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
    pub async fn calc_daily_ema_shift() {
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
        let n = vc.len();
        let ewma = Metric::ewma(&vc[..n - 1], period);
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
        let vtr = candles
            .iter()
            .fold((dec!(0), Vec::new()), |(c, mut vtr), can| {
                let hl = can.high - can.low;
                let tr = match c.is_zero() {
                    true => hl,
                    false => {
                        let hpdc = (can.high - c).abs();
                        let pdcl = (c - can.low).abs();
                        hl.max(hpdc).max(pdcl)
                    }
                };
                vtr.push(tr);
                (can.close, vtr)
            });
        // Calc the EMA
        let period = 14 as i64;
        let ewma = Metric::ewma(&vtr.1, period);
        println!("Candle TRs: {:?}", vtr.1);
        println!("ATR {}: {:?}", period, ewma);
    }

    #[tokio::test]
    pub async fn calc_z_return() {
        // Get daily BTC-PERP candles from FTX
        let client = crate::exchanges::ftx::RestClient::new_intl();
        let mut candles = client
            .get_candles("BTC-PERP", Some(86400), None, None)
            .await
            .expect("Failed to get candles.");
        // Sort candles and put tr values into vector
        candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
        let vr = candles
            .iter()
            .fold((dec!(0), Vec::new()), |(c, mut vr), can| {
                let r = match c.is_zero() {
                    true => dec!(0),
                    false => can.close / c - dec!(1),
                };
                vr.push(r);
                (can.close, vr)
            });
        // Calc the z-score of return
        let lbp = 90;
        let n = vr.1.len();
        let range_start = n - lbp as usize;
        let range_shift_start = range_start - 1;
        let range_shift_end = n - 1;
        let z = Metric::z(&vr.1, range_shift_start, range_shift_end);
        println!("vr: {:?}", vr.1);
        println!("z: {:?}", z);
    }
}
