use crate::candles::Candle;
use crate::exchanges::ExchangeName;
use crate::markets::MarketDetail;
use crate::utilities::TimeFrame;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
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

    pub fn prev(&self) -> TimeFrame {
        match self {
            TimeFrame::T15 => TimeFrame::T15,
            TimeFrame::H01 => TimeFrame::T15,
            TimeFrame::H04 => TimeFrame::H01,
            TimeFrame::H12 => TimeFrame::H04,
            TimeFrame::D01 => TimeFrame::H12,
            _ => TimeFrame::T15, // To be defined
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

pub struct Metric {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub datetime: DateTime<Utc>,
    pub time_frame: TimeFrame,
    pub lbp: i64,
    pub close: Decimal,
    pub r: Decimal,
}

impl Metric {
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
        market: &str,
        exchange: &ExchangeName,
        tf: TimeFrame,
        candles: &[Candle],
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
                market_name: market.to_string(),
                exchange_name: *exchange,
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
}

pub async fn insert_metric_ap(pool: &PgPool, metric: &MetricAP) -> Result<(), sqlx::Error> {
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
        .bind(metric.exchange_name.as_str())
        .bind(&metric.market_name)
        .bind(metric.datetime)
        .bind(metric.time_frame.as_str())
        .bind(metric.lbp)
        .bind(metric.high)
        .bind(metric.low)
        .bind(metric.close)
        .bind(metric.r)
        .bind(metric.h004h)
        .bind(metric.h004c)
        .bind(metric.l004l)
        .bind(metric.l004c)
        .bind(metric.h008h)
        .bind(metric.h008c)
        .bind(metric.l008l)
        .bind(metric.l008c)
        .bind(metric.h012h)
        .bind(metric.h012c)
        .bind(metric.l012l)
        .bind(metric.l012c)
        .bind(metric.h024h)
        .bind(metric.h024c)
        .bind(metric.l024l)
        .bind(metric.l024c)
        .bind(metric.h048h)
        .bind(metric.h048c)
        .bind(metric.l048l)
        .bind(metric.l048c)
        .bind(metric.h096h)
        .bind(metric.h096c)
        .bind(metric.l096l)
        .bind(metric.l096c)
        .bind(metric.h192h)
        .bind(metric.h192c)
        .bind(metric.l192l)
        .bind(metric.l192c)
        .bind(metric.ema1)
        .bind(metric.ema2)
        .bind(metric.ema3)
        .bind(metric.mv1)
        .bind(metric.mv2)
        .bind(metric.mv3)
        .bind(metric.ofz)
        .bind(metric.vz)
        .bind(metric.rz)
        .bind(metric.atr)
        .bind(metric.trz)
        .bind(metric.uwz)
        .bind(metric.bz)
        .bind(metric.lwz)
        .bind(metric.ma)
        .bind(metric.vw)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_metrics_ap_by_exchange_market(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        DELETE FROM metrics_ap
        WHERE exchange_name = $1
        AND market_name = $2
        "#;
    sqlx::query(sql)
        .bind(exchange_name.as_str())
        .bind(&market.market_name)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_metrics_ap_by_exchange_market(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    markets: &[String],
) -> Result<Vec<MetricAP>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MetricAP,
        r#"
        SELECT exchange_name as "exchange_name: ExchangeName",
            market_name,
            datetime,
            time_frame as "time_frame: TimeFrame",
            lbp, high, low, close, r,
            h004c,
            l004c,
            h004r as h004h,
            l004r as l004l,
            h008c,
            l008c,
            h008r as h008h,
            l008r as l008l,
            h012c,
            l012c,
            h012r as h012h,
            l012r as l012l,
            h024c,
            l024c,
            h024r as h024h,
            l024r as l024l,
            h048c,
            l048c,
            h048r as h048h,
            l048r as l048l,
            h096c,
            l096c,
            h096r as h096h,
            l096r as l096l,
            h192c,
            l192c,
            h192r as h192h,
            l192r as l192l,
            ema1, ema2, ema3, mv1, mv2, mv3, n as atr, vw, ma,
            ofs as ofz, vs as vz, rs as rz, trs as trz, uws as uwz, mbs as bz, lws as lwz
        FROM metrics_ap
        WHERE exchange_name = $1
        AND market_name = ANY($2)
        "#,
        exchange_name.as_str(),
        markets
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
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
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Get candles from db
        let start = Utc.ymd(2021, 09, 25).and_hms(0, 0, 0);
        let candles = select_candles_gte_datetime(
            &pool,
            &ExchangeName::FtxUs,
            &Uuid::parse_str("2246c870-769f-44a4-b989-ffa2de37f8b1").unwrap(),
            start,
        )
        .await
        .expect("Failed to select candles.");
        println!("Num Candles: {:?}", candles.len());
        let timer_start = Utc::now();
        let metrics = MetricAP::new("BTC-PERP", &ExchangeName::Ftx, TimeFrame::T15, &candles);
        let timer_end = Utc::now();
        let time_elapsed = timer_end - timer_start;
        println!("Time to calc: {:?}", time_elapsed);
        println!("Metrics: {:?}", metrics);
        for metric in metrics.iter() {
            insert_metric_ap(&pool, metric)
                .await
                .expect("Failed to insert metric ap.");
        }
    }

    #[tokio::test]
    pub async fn calc_daily_ema() {
        // Get daily BTC-PERP candles from FTX
        let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
        let mut candles = client
            .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
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
        let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
        let mut candles = client
            .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
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
        let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
        let mut candles = client
            .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
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
        let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
        let mut candles = client
            .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
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

    #[tokio::test]
    pub async fn calc_dons() {
        // Get daily BTC-PERP candles from FTX
        let client = crate::exchanges::client::RestClient::new(&ExchangeName::Ftx);
        let mut candles = client
            .get_ftx_candles::<crate::exchanges::ftx::Candle>("BTC-PERP", Some(86400), None, None)
            .await
            .expect("Failed to get candles.");
        // Sort candles and put close prices into vector
        candles.sort_by(|c1, c2| c1.time.cmp(&c2.time));
        let vc: Vec<Decimal> = candles.iter().map(|c| c.close).collect();
        // let slice = &vc[0..50];
        let dons = Metric::dons(&vc, &vc, &vc);
        println!("Closes: {:?}", vc);
        println!("Dons: {:?}", dons);
    }

    #[tokio::test]
    pub async fn select_metrics_ap_by_exchange_market_maps_to_struct() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.ed_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Create list of market names
        let markets = vec![
            "BTC-PERP".to_string(),
            "ETH-PERP".to_string(),
            "SOL-PERP".to_string(),
        ];
        // Select metrics for markets
        let metrics = select_metrics_ap_by_exchange_market(&pool, &ExchangeName::Ftx, &markets)
            .await
            .expect("Failed to select metrics.");
        println!("{:?}", metrics);
    }
}
