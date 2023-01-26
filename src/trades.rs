use crate::configuration::Database;
use crate::eldorado::ElDorado;
use crate::exchanges::{
    error::RestError, ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade, ExchangeName,
};
use crate::markets::MarketDetail;
use crate::utilities::{DateRange, TimeFrame};
use async_trait::async_trait;
use chrono::{DateTime, Duration, DurationRound, Utc};
use csv::Reader;
use rust_decimal::prelude::*;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::path::PathBuf;

#[async_trait]
pub trait Trade {
    fn trade_id(&self) -> i64;
    fn price(&self) -> Decimal;
    fn size(&self) -> Decimal;
    fn side(&self) -> String;
    fn liquidation(&self) -> bool;
    fn time(&self) -> DateTime<Utc>;
    fn as_pridti(&self) -> PrIdTi;
    async fn create_table(
        pool: &PgPool,
        market: &MarketDetail,
        dt: DateTime<Utc>,
    ) -> Result<(), sqlx::Error>
    where
        Self: Sized;
    async fn insert(&self, pool: &PgPool, market: &MarketDetail) -> Result<(), sqlx::Error>;
    async fn drop_table(
        pool: &PgPool,
        market: &MarketDetail,
        dt: DateTime<Utc>,
    ) -> Result<(), sqlx::Error>
    where
        Self: Sized;
}

// Struct to pack information about last trade - typically used to create a candle from last trade
#[derive(Debug, Clone, Copy)]
pub struct PrIdTi {
    pub dt: DateTime<Utc>,
    pub id: i64,
    pub price: Decimal,
}

impl fmt::Display for PrIdTi {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dt: {}\tid: {}\tprice: {}", self.dt, self.id, self.price)
    }
}

impl ElDorado {
    pub async fn initialize_trade_schema_and_tables(&self) -> DateTime<Utc> {
        match self.instance.exchange_name.unwrap() {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.create_trades_schema(&self.pools[&Database::Ftx])
                    .await
                    .expect("Failed to create ftx/ftxus trade schema.");
            }
            ExchangeName::Gdax => {
                self.create_trades_schema(&self.pools[&Database::Gdax])
                    .await
                    .expect("Failed to create gdax trade schema.");
            }
        }
        // Create trade tables for each market for today if they don't exist
        let today = Utc::now().duration_trunc(Duration::days(1)).unwrap();
        self.create_trade_tables_all_markets(today).await;
        today
    }

    pub async fn create_trades_schema(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        let sql = r#"
            CREATE SCHEMA IF NOT EXISTS trades
            "#;
        sqlx::query(sql).execute(pool).await?;
        Ok(())
    }

    pub async fn create_trade_table(&self, market: &MarketDetail, dt: DateTime<Utc>) {
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                FtxTrade::create_table(&self.pools[&Database::Ftx], market, dt)
                    .await
                    .expect("Failed to create ftx/ftxus trade table.");
            }
            ExchangeName::Gdax => {
                GdaxTrade::create_table(&self.pools[&Database::Gdax], market, dt)
                    .await
                    .expect("Failed to create gdax trade table.");
            }
        }
    }

    pub async fn create_trade_tables_all_markets(&self, dt: DateTime<Utc>) {
        for market in self.markets.iter() {
            self.create_trade_table(market, dt).await;
        }
    }

    pub async fn create_future_trade_tables_all_markets(
        &self,
        mut dt: DateTime<Utc>,
    ) -> DateTime<Utc> {
        // If the date given is less than 2 days in the future, increment the date and
        // create tables for the date before returning it
        if dt < Utc::now().duration_trunc(Duration::days(1)).unwrap() + Duration::days(2) {
            dt = dt + Duration::days(1);
            self.create_trade_tables_all_markets(dt).await;
            dt
        } else {
            dt
        }
    }

    pub async fn trade_table_exists(&self, market: &MarketDetail, dt: &DateTime<Utc>) -> bool {
        // Get the full trade table name and then check self fn for table and schema
        let table = format!(
            "{}_{}_{}",
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        self.table_exists(&self.pools[&db], "trades", &table)
            .await
            .expect("Failed to check table.")
    }

    pub async fn select_first_ws_timeid(&self, market: &MarketDetail) -> Option<PrIdTi> {
        // Select the first trade from database table after the start of the instance
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                match FtxTrade::select_one_gt_dt(&self.pools[&Database::Ftx], market, self.start_dt)
                    .await
                {
                    Ok(t) => Some(t.as_pridti()),
                    Err(sqlx::Error::RowNotFound) => None,
                    Err(e) => panic!("Sqlx::Error: {:?}", e),
                }
            }
            ExchangeName::Gdax => {
                match GdaxTrade::select_one_gt_dt(
                    &self.pools[&Database::Gdax],
                    market,
                    self.start_dt,
                )
                .await
                {
                    Ok(t) => Some(t.as_pridti()),
                    Err(sqlx::Error::RowNotFound) => None,
                    Err(e) => panic!("Sqlx::Error: {:?}", e),
                }
            }
        }
    }

    pub async fn select_first_eld_trade_as_pridti(&self, market: &MarketDetail) -> Option<PrIdTi> {
        // Select the first trade for the market in the eldorado database. For <0.3 markets, this
        // will be in the 01d_candles table if it exists. For >=0.4 markets, this will be the first
        // full day production candle for the market.
        match self.select_first_daily_candle(market).await {
            Some(c) => Some(c.open_as_pridti()),
            None => self
                .select_first_production_candle_full_day(market)
                .await
                .map(|c| c.open_as_pridti()),
        }
    }

    // Return the last trade of the day for a given trade. For example: if the trade given was
    // placed on 14:23:11 on 12/23/2020, return the last trade on 12/23/2020
    pub async fn get_last_gdax_trade_for_day(
        &self,
        market: &MarketDetail,
        trade: &GdaxTrade,
    ) -> GdaxTrade {
        let end = trade.time.duration_trunc(Duration::days(1)).unwrap() + Duration::days(1);
        let mut t = trade.clone();
        let mut trades = Vec::new();
        while t.time < end {
            // Prevent 429 errors by only requesting 1 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match self.clients[&market.exchange_name]
                .get_gdax_trades(
                    &market.market_name,
                    Some(1000),
                    None,
                    Some(t.trade_id as i32 + 1001),
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Rest Error: {:?}", e);
                    }
                }
            };
            // Sort the new trades
            new_trades.sort_by_key(|t| t.trade_id);
            // Update the last trade
            t = new_trades.last().unwrap().clone();
            trades.append(&mut new_trades);
        }
        let vec_trades: Vec<GdaxTrade> = trades.iter().filter(|t| t.time < end).cloned().collect();
        vec_trades.last().unwrap().clone()
    }

    // Return the last trade of the prev day for a given trade. For example: if the trade given was
    // placed on 14:23:11 on 12/23/2020, return the last trade on 12/22/2020
    pub async fn get_last_gdax_trade_for_prev_day(
        &self,
        market: &MarketDetail,
        trade: &GdaxTrade,
    ) -> GdaxTrade {
        let end = trade.time.duration_trunc(Duration::days(1)).unwrap();
        let mut t = trade.clone();
        let mut trades = Vec::new();
        while t.time > end {
            // Prevent 429 errors by only requesting 1 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match self.clients[&market.exchange_name]
                .get_gdax_trades(
                    &market.market_name,
                    Some(1000),
                    None,
                    Some(t.trade_id as i32),
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Rest Error: {:?}", e);
                    }
                }
            };
            // Sort the new trades
            new_trades.sort_by_key(|t| t.trade_id);
            // Update the last trade
            t = new_trades.first().unwrap().clone();
            trades.append(&mut new_trades);
        }
        trades.sort_by_key(|t| t.trade_id);
        let vec_trades: Vec<GdaxTrade> = trades.iter().filter(|t| t.time < end).cloned().collect();
        vec_trades.last().unwrap().clone()
    }

    // This function relies on the trade tables to already be created. Do not call if there is no
    // trade table created for the market as it will fail when attempting to insert trades
    pub async fn get_ftx_trades_for_interval(
        &self,
        _market: &MarketDetail,
        _start: &DateTime<Utc>,
        _end: &DateTime<Utc>,
    ) -> Vec<FtxTrade> {
        Vec::new()
    }

    // This function relies on the trade tables to already be created. Do not call if there is no
    // trade table created for the market as it will fail when attempting to insert trades
    // From the start fill forward 1000 trades until you reach the end (which is either the first
    // streamed trade or the end of the next full day
    // Getting AAVE-PERP trades before and after trade id 13183395
    // Before trades: - Returns trades before that trade id in descending order. Since this
    // returns trades way beyond what we are seeking (those immediately before the trade id)
    // we need to use the after function to get trades.
    // Trade { trade_id: 17536192, side: "sell", size: 0.00400000, price: 101.57000000, time: 2022-05-24T20:23:05.836337Z }
    // Trade { trade_id: 17536191, side: "buy", size: 3.30000000, price: 101.55000000, time: 2022-05-24T20:23:01.506580Z }
    // Trade { trade_id: 17536190, side: "sell", size: 6.01100000, price: 101.56000000, time: 2022-05-24T20:23:00.273643Z }
    // Trade { trade_id: 17536189, side: "sell", size: 1.96800000, price: 101.55000000, time: 2022-05-24T20:23:00.273643Z }
    // Trade { trade_id: 17536188, side: "buy", size: 3.61100000, price: 101.48000000, time: 2022-05-24T20:22:55.061587Z }
    // After trades:
    // Trade { trade_id: 13183394, side: "buy", size: 0.21900000, price: 184.69100000, time: 2021-12-06T23:59:59.076214Z }
    // Trade { trade_id: 13183393, side: "buy", size: 2.37800000, price: 184.69200000, time: 2021-12-06T23:59:59.076214Z }
    // Trade { trade_id: 13183392, side: "buy", size: 0.00300000, price: 184.74100000, time: 2021-12-06T23:59:59.076214Z }
    // Trade { trade_id: 13183391, side: "buy", size: 0.01600000, price: 184.80200000, time: 2021-12-06T23:59:58.962743Z }
    // Trade { trade_id: 13183390, side: "buy", size: 0.01600000, price: 184.87100000, time: 2021-12-06T23:59:57.823784Z }
    pub async fn get_gdax_trades_for_interval_forward(
        &self,
        market: &MarketDetail,
        interval_start: &DateTime<Utc>,
        interval_end: &DateTime<Utc>,
        // last_trade: &PrIdTi,
        mut last_trade_id: i32,
        last_trade_dt: &DateTime<Utc>,
    ) -> Vec<GdaxTrade> {
        // Start with the last trade prior to the interval start. Get the next 1000 trades, move the
        // last trade to be equal to the last trade received and continue until the timestamp of
        // the last trade is greater than the interval end timestamp
        println!(
            "Getting trades from {} to {}.\tLast Trade Id: {}",
            interval_start, interval_end, last_trade_id
        );
        // let mut last = *last_trade;
        let mut last_dt = *last_trade_dt;
        let mut trades: Vec<GdaxTrade> = Vec::new();
        while last_dt < *interval_end {
            // Prevent 429 errors by only requesting 1 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match self.clients[&market.exchange_name]
                .get_gdax_trades(
                    &market.market_name,
                    Some(1000),
                    None,
                    Some(last_trade_id as i32 + 1001),
                )
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Rest Error: {:?}", e);
                    }
                }
            };
            // Sort the new trades
            new_trades.sort_by_key(|t| t.trade_id);
            // Update the last trade
            last_trade_id = new_trades.last().unwrap().trade_id as i32;
            last_dt = new_trades.last().unwrap().time;
            println!(
                "{} trades from API. New Last Trade: {}",
                new_trades.len(),
                last_dt
            );
            // Filter out trades that are beyond the end date
            let mut filtered_trades = if last_dt >= *interval_end {
                // There are trade to filter
                let ft: Vec<_> = new_trades
                    .iter()
                    .filter(|t| t.time < *interval_end)
                    .cloned()
                    .collect();
                println!(
                    "Last trade beyond interval end. Filter trades. New # {}",
                    ft.len()
                );
                if !ft.is_empty() {
                    // Edge case where all trades are filtered out and there is no trade to unwrap
                    println!("New Last Trade: {}", ft.last().unwrap().as_pridti());
                } else {
                    println!("All trades filtered.");
                }
                ft
            } else {
                new_trades
            };
            // Add new trades to trades vec
            trades.append(&mut filtered_trades);
        }
        trades
    }

    pub async fn get_gdax_trades_for_interval_backward(
        &self,
        market: &MarketDetail,
        interval_start: &DateTime<Utc>,
        interval_end: &DateTime<Utc>,
        mut first_trade_id: i32,
        first_trade_dt: &DateTime<Utc>,
    ) -> Vec<GdaxTrade> {
        // Start with the frist trade after the interval end. Get the 1000 trades after the first.
        // Move the first to be equal to the first that was received and continue until the time
        // of the first trade is less than the interval start time stamp or the trade id == 1
        println!(
            "Getting trades from {} to {}.\t First Trade Id: {}",
            interval_start, interval_end, first_trade_id
        );
        let mut first_dt = *first_trade_dt;
        let mut trades: Vec<GdaxTrade> = Vec::new();
        while first_dt >= *interval_start && first_trade_id != 1 {
            // Prevent 429 errors by only requesting 1 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match self.clients[&market.exchange_name]
                .get_gdax_trades(&market.market_name, Some(1000), None, Some(first_trade_id))
                .await
            {
                Ok(result) => result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Rest Error: {:?}", e);
                    }
                }
            };
            // Get the earliest trade from the new trades and add the trades to the trades vec
            new_trades.sort_by_key(|t| t.trade_id);
            first_trade_id = new_trades.first().unwrap().trade_id as i32;
            first_dt = new_trades.first().unwrap().time;
            println!(
                "{} trades from API. New First Trade: {}",
                new_trades.len(),
                first_dt
            );
            // Filter out trades that are before the start date
            let mut filtered_trades = if first_dt < *interval_start {
                // There are trade to filter out
                let ft: Vec<_> = new_trades
                    .iter()
                    .filter(|t| t.time >= *interval_start)
                    .cloned()
                    .collect();
                println!(
                    "First trade before interval start. Filter trades. New # {}",
                    ft.len()
                );
                println!("New First Trade: {}\t{}", first_trade_id, first_dt);
                ft
            } else {
                new_trades
            };
            trades.append(&mut filtered_trades);
        }
        trades
    }

    pub async fn get_gdax_next_trade(
        &self,
        market: &MarketDetail,
        trade: &GdaxTrade,
    ) -> Option<GdaxTrade> {
        // Get the next trade after the gdax trade provided
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            match self.clients[&ExchangeName::Gdax]
                .get_gdax_next_trade(market.market_name.as_str(), trade.trade_id as i32)
                .await
            {
                Ok(result) => return result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Unhandled Rest Error: {:?}", e);
                    }
                }
            }
        }
    }

    pub async fn get_gdax_previous_trade(
        &self,
        market: &MarketDetail,
        trade: &GdaxTrade,
    ) -> Option<GdaxTrade> {
        // Get the first trade before the gdax trade provided
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            match self.clients[&ExchangeName::Gdax]
                .get_gdax_previous_trade(market.market_name.as_str(), trade.trade_id as i32)
                .await
            {
                Ok(result) => return result,
                Err(e) => {
                    if self.handle_trade_rest_error(&e).await {
                        continue;
                    } else {
                        panic!("Unhandled Rest Error: {:?}", e);
                    }
                }
            }
        }
    }

    pub async fn handle_trade_rest_error(&self, e: &RestError) -> bool {
        match e {
            RestError::Reqwest(e) => {
                if e.is_timeout() || e.is_connect() || e.is_request() {
                    println!("Timeout/Connect/Request Error. Retry in 30 secs. {:?}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    true
                } else if e.is_status() {
                    match e.status() {
                        Some(s) => match s.as_u16() {
                            500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                // Server error, keep trying every 30 seconds
                                println!("{} status code. Retry in 30 secs. {:?}", s, e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                true
                            }
                            429 => {
                                // Too many requests, chill for 90 seconds
                                println!("{} status code. Retry in 90 secs. {:?}", s, e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(90)).await;
                                true
                            }
                            _ => {
                                println!("{} status code not handled. Panic.", s);
                                false
                            }
                        },
                        None => {
                            println!("No status code for request error.");
                            false
                        }
                    }
                } else {
                    println!("Other Reqwest Error. Panic.");
                    false
                }
            }
            _ => {
                println!("Other Rest Error, not Reqwest.");
                false
            }
        }
    }

    // Select all gdax trades from the gdax db for the market for the interval given. This may be
    // accross mulitple tables as the tables are broken up by day
    pub async fn select_gdax_trades_for_interval(
        &self,
        market: &MarketDetail,
        dr: &DateRange,
    ) -> Option<Vec<GdaxTrade>> {
        // Assert that the dr has dates and is not empty
        println!(
            "Selecting trades gte {} and lt {}",
            dr.first,
            dr.last + market.candle_timeframe.unwrap().as_dur()
        );
        let mut trades = Vec::new();
        match DateRange::new(
            &dr.first.duration_trunc(Duration::days(1)).unwrap(),
            &(dr.last.duration_trunc(Duration::days(1)).unwrap() + Duration::days(1)),
            &TimeFrame::D01,
        ) {
            Some(days) => {
                for d in days.dts.iter() {
                    // Select trades
                    println!(
                        "Selecting {} trades on day {} for interval.",
                        market.market_name, d
                    );
                    let mut db_trades = GdaxTrade::select_gte_and_lt_dts(
                        &self.pools[&Database::Gdax],
                        market,
                        d,
                        &dr.first,
                        &(dr.last + market.candle_timeframe.unwrap().as_dur()),
                    )
                    .await
                    .expect("Failed to select trades.");
                    trades.append(&mut db_trades);
                }
                if !trades.is_empty() {
                    Some(trades)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn read_gdax_trades_from_file_into_vec(&self, pb: &PathBuf) -> Vec<GdaxTrade> {
        // Read archived file for first and last trades to update
        let file = File::open(pb).expect("Failed to open file.");
        let mut trades = Vec::new();
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: GdaxTrade = result.expect("Failed to deserialize record.");
            trades.push(record)
        }
        trades
    }

    pub fn read_gdax_trades_from_file_into_hashmap(
        &self,
        pb: &PathBuf,
        tf: &TimeFrame,
    ) -> HashMap<DateTime<Utc>, Vec<GdaxTrade>> {
        // Read archived file and load trades into a hashmap with keys of the time frame buckets
        let mut trades: HashMap<DateTime<Utc>, Vec<GdaxTrade>> = HashMap::new();
        let file = File::open(pb).expect("Failed to open file.");
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: GdaxTrade = result.expect("Failed to deserialize record.");
            trades
                .entry(record.time.duration_trunc(tf.as_dur()).unwrap())
                .and_modify(|v| v.push(record.clone()))
                .or_insert_with(|| vec![record.clone()]);
        }
        trades
    }

    pub fn read_gdax_trades_from_file(
        &self,
        pb: &PathBuf,
        tf: &TimeFrame,
    ) -> (Vec<GdaxTrade>, HashMap<DateTime<Utc>, Vec<GdaxTrade>>) {
        let mut trades_hm: HashMap<DateTime<Utc>, Vec<GdaxTrade>> = HashMap::new();
        let mut trades_vec = Vec::new();
        let file = File::open(pb).expect("Failed to open file.");
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: GdaxTrade = result.expect("Failed to deserialize record.");
            trades_hm
                .entry(record.time.duration_trunc(tf.as_dur()).unwrap())
                .and_modify(|v| v.push(record.clone()))
                .or_insert_with(|| vec![record.clone()]);
            trades_vec.push(record)
        }
        (trades_vec, trades_hm)
    }

    pub fn read_ftx_trades_from_file(&self, pb: &PathBuf) -> Vec<FtxTrade> {
        // Read archived file for first and last trades to update
        let file = File::open(pb).expect("Failed to open file.");
        let mut trades = Vec::new();
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: FtxTrade = result.expect("Failed to deserialize record.");
            trades.push(record)
        }
        trades
    }
}

// impl Inquisidor {
//     // Takes in a start and end time to get trades from FTX REST API and inserts those trades in
//     // the trades table ftx.trades_{market_name}_{table_suf} one day at a time
//     // This is used in trade fill and sync.
//     // For trade sync, this function is called for each day in the daterange from the start
//     // ie: ftx.trades_btcperp_20221020
//     // For trade fill qc, this is used for one off trade day to qc with stored range
//     // ie: ftx.trades_btc_perp_20221020_qc
//     pub async fn get_ftx_trades_dr_into_table(
//         &self,
//         event: &Event,
//         table_suf: &str,
//         start: DateTime<Utc>,
//         end: DateTime<Utc>,
//     ) {
//         let market = self.market(&event.market_id);
//         create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, table_suf)
//             .await
//             .expect("Failed to create trade table.");
//         // Fill trade table with trades
//         let mut end_or_last_trade = end;
//         let mut total_trades: usize = 0;
//         while start < end_or_last_trade {
//             // Prevent 429 errors by only requesting 4 per second
//             tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
//             let mut new_trades = match self.clients[&event.exchange_name]
//                 .get_ftx_trades(
//                     market.market_name.as_str(),
//                     Some(5000),
//                     Some(start),
//                     Some(end_or_last_trade),
//                 )
//                 .await
//             {
//                 Err(RestError::Reqwest(e)) => {
//                     if e.is_timeout() || e.is_connect() || e.is_request() {
//                         println!("{:?} error. Waiting 30 seconds before retry.", e);
//                         tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
//                         continue;
//                     } else if e.is_status() {
//                         match e.status() {
//                             Some(s) => match s.as_u16() {
//                                 500 | 502 | 503 | 504 | 520 | 522 | 530 => {
//                                     println!(
//                                         "{} status code. Waiting 30 seconds before retry {:?}",
//                                         s, e
//                                     );
//                                     tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
//                                     continue;
//                                 }
//                                 _ => {
//                                     panic!("Status code not handled: {:?} {:?}", s, e)
//                                 }
//                             },
//                             None => panic!("No status code for request error: {:?}", e),
//                         }
//                     } else {
//                         panic!("Error (not timeout / connect / request): {:?}", e)
//                     }
//                 }
//                 Err(e) => panic!("Other RestError: {:?}", e),
//                 Ok(result) => result,
//             };
//             let num_trades = new_trades.len();
//             total_trades += num_trades; // Add to running total of trades
//             if num_trades > 0 {
//                 new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
//                 end_or_last_trade = new_trades.first().unwrap().time;
//                 let first_trade = new_trades.last().unwrap().time;
//                 println!(
//                     "{} trade returned. First: {}, Last: {}",
//                     num_trades, end_or_last_trade, first_trade
//                 );
//                 insert_ftx_trades(
//                     &self.ftx_pool,
//                     &event.exchange_name,
//                     market,
//                     table_suf,
//                     new_trades,
//                 )
//                 .await
//                 .expect("Failed to insert backfill ftx trades.");
//             };
//             if num_trades < 5000 && total_trades > 0 {
//                 // Trades returned less than REST API limit. No more trades to retreive.
//                 break;
//             };
//         }
//     }

//     pub async fn get_ftx_trades_dr_into_vec(
//         &self,
//         event: &Event,
//         start: DateTime<Utc>,
//         end: DateTime<Utc>,
//     ) -> Vec<FtxTrade> {
//         let market = self.market(&event.market_id);
//         // create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, table_suf)
//         //     .await
//         //     .expect("Failed to create trade table.");
//         let mut trades = Vec::new();
//         // Fill trade table with trades
//         let mut end_or_last_trade = end;
//         let mut total_trades: usize = 0;
//         while start < end_or_last_trade {
//             // Prevent 429 errors by only requesting 4 per second
//             tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
//             let mut new_trades = match self.clients[&event.exchange_name]
//                 .get_ftx_trades(
//                     market.market_name.as_str(),
//                     Some(5000),
//                     Some(start),
//                     Some(end_or_last_trade),
//                 )
//                 .await
//             {
//                 Err(RestError::Reqwest(e)) => {
//                     if e.is_timeout() || e.is_connect() || e.is_request() {
//                         println!("{:?} error. Waiting 30 seconds before retry.", e);
//                         tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
//                         continue;
//                     } else if e.is_status() {
//                         match e.status() {
//                             Some(s) => match s.as_u16() {
//                                 500 | 502 | 503 | 504 | 520 | 522 | 530 => {
//                                     println!(
//                                         "{} status code. Waiting 30 seconds before retry {:?}",
//                                         s, e
//                                     );
//                                     tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
//                                     continue;
//                                 }
//                                 _ => {
//                                     panic!("Status code not handled: {:?} {:?}", s, e)
//                                 }
//                             },
//                             None => panic!("No status code for request error: {:?}", e),
//                         }
//                     } else {
//                         panic!("Error (not timeout / connect / request): {:?}", e)
//                     }
//                 }
//                 Err(e) => panic!("Other RestError: {:?}", e),
//                 Ok(result) => result,
//             };
//             let num_trades = new_trades.len();
//             total_trades += num_trades; // Add to running total of trades
//             if num_trades > 0 {
//                 new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
//                 end_or_last_trade = new_trades.first().unwrap().time;
//                 let first_trade = new_trades.last().unwrap().time;
//                 println!(
//                     "{} trade returned. First: {}, Last: {}",
//                     num_trades, end_or_last_trade, first_trade
//                 );
//                 // insert_ftx_trades(
//                 //     &self.ftx_pool,
//                 //     &event.exchange_name,
//                 //     market,
//                 //     table_suf,
//                 //     new_trades,
//                 // )
//                 // .await
//                 // .expect("Failed to insert backfill ftx trades.");
//                 trades.append(&mut new_trades)
//             };
//             if num_trades < 5000 && total_trades > 0 {
//                 // Trades returned less than REST API limit. No more trades to retreive.
//                 break;
//             };
//         }
//         trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
//         trades.dedup_by(|c1, c2| c1.id == c2.id);
//         trades
//     }

//     pub async fn migrate_ftx_trades_for_date(
//         &self,
//         event: &Event,
//         to_table: &str,
//         from_table: &str,
//         date: DateTime<Utc>,
//     ) {
//         let market = self.market(&event.market_id);
//         // Select trades from the _processed and _validated tables and put in the new table
//         create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, to_table)
//             .await
//             .expect("Failed to create trade table.");
//         let mut trades = Vec::new();
//         let mut from_trades = select_ftx_trades_by_time(
//             &self.ftx_pool,
//             &event.exchange_name,
//             market,
//             from_table,
//             date,
//             date + Duration::days(1),
//         )
//         .await
//         .expect("Failed to select ftx trades.");
//         trades.append(&mut from_trades);
//         trades.sort_by_key(|t| t.trade_id());
//         insert_ftx_trades(
//             &self.ftx_pool,
//             &event.exchange_name,
//             market,
//             to_table,
//             trades,
//         )
//         .await
//         .expect("Failed to insert trades.");
//         // Delete trades from processed and validated tables
//         delete_trades_by_time(
//             &self.ftx_pool,
//             &event.exchange_name,
//             market,
//             from_table,
//             date,
//             date + Duration::days(1),
//         )
//         .await
//         .expect("Failed to delete trades.");
//     }

//     pub async fn select_ftx_trades_by_table(
//         &self,
//         table: &str,
//     ) -> Result<Vec<FtxTrade>, sqlx::Error> {
//         // Cannot user query_as! macro because table may not exist at compile time
//         let sql = format!(
//             r#"
//             SELECT trade_id as id, price, size, side, liquidation, time
//             FROM {}
//             ORDER BY trade_id
//             "#,
//             table
//         );
//         let rows = sqlx::query_as::<_, FtxTrade>(&sql)
//             .fetch_all(&self.ftx_pool)
//             .await?;
//         Ok(rows)
//     }

//     pub async fn load_trades_for_dr(
//         &self,
//         market: &MarketDetail,
//         dr: &[DateTime<Utc>],
//     ) -> HashMap<DateTime<Utc>, Vec<FtxTrade>> {
//         // For each date - load the trades and append to vec of trades
//         // Create hashmap of candle datetimes to store trades
//         let mut candle_dr_map: HashMap<DateTime<Utc>, Vec<FtxTrade>> = HashMap::new();
//         for d in dr.iter() {
//             // Load trades for day
//             println!("{:?} - Loading trades for {:?}", Utc::now(), d);
//             let archive_path = format!(
//                 "{}/trades/{}/{}/{}/{}",
//                 &self.settings.application.archive_path,
//                 &market.exchange_name.as_str(),
//                 &market.as_strip(),
//                 d.format("%Y"),
//                 d.format("%m")
//             );
//             let f = format!("{}_{}.csv", market.as_strip(), d.format("%F"));
//             let a_path = std::path::Path::new(&archive_path).join(f.clone());
//             // Set file
//             let file = File::open(a_path).expect("Failed to open file.");
//             let mut rdr = Reader::from_reader(file);
//             for result in rdr.deserialize() {
//                 let record: FtxTrade = result.expect("Faile to deserialize record.");
//                 candle_dr_map
//                     .entry(record.time.duration_trunc(TimeFrame::S15.as_dur()).unwrap())
//                     .and_modify(|v| v.push(record.clone()))
//                     .or_insert_with(|| vec![record.clone()]);
//             }
//         }
//         candle_dr_map
//     }
// }

// ALTER, DROP, MIGRATE actions are the same regardless of exchange
// CREATE, INSERT, SELECT actions are unique to each exchange trades struct

pub async fn select_insert_delete_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            // Select ftx trades according to params
            let trades =
                select_ftx_trades_by_time(pool, exchange_name, market, source, start, end).await?;
            // Insert ftx trades into destination table
            insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
        }
        ExchangeName::Gdax => {
            let trades = select_gdax_trades_by_time(pool, market, source, start, end).await?;
            insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
        }
    }
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

pub async fn select_insert_drop_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            // Select ftx trades according to params
            let trades =
                select_ftx_trades_by_time(pool, exchange_name, market, source, start, end).await?;
            // Insert ftx trades into destination table
            insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
        }
        ExchangeName::Gdax => {
            let trades = select_gdax_trades_by_time(pool, market, source, start, end).await?;
            insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
        }
    }
    // Drop source table
    drop_trade_table(pool, exchange_name, market, source).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_delete_ftx_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
    trades: Vec<FtxTrade>,
) -> Result<(), sqlx::Error> {
    // Insert ftx trades into destination table
    insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_delete_gdax_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
    trades: Vec<GdaxTrade>,
) -> Result<(), sqlx::Error> {
    // Insert ftx trades into destination table
    insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

pub async fn drop_create_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Drop the table if it exists
    drop_trade_table(pool, exchange_name, market, trade_table).await?;
    // Create the trade table to exchange specifications
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            create_ftx_trade_table(pool, exchange_name, market, trade_table).await?
        }
        ExchangeName::Gdax => {
            create_gdax_trade_table(pool, exchange_name, market, trade_table).await?
        }
    }
    Ok(())
}

pub async fn alter_create_migrate_drop_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Alter table to temp name
    alter_trade_table_to_temp(pool, exchange_name, market, trade_table).await?;
    // Create both the original trade table AND the temp table. There is a
    // scenario where the original table does not exists, thus the altered
    // table is not created. This will cause a panic in the migrate
    // function when it tries to select from a temp table that does not exists.
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            create_ftx_trade_table(pool, exchange_name, market, trade_table).await?;
            create_ftx_trade_table(
                pool,
                exchange_name,
                market,
                format!("{}_temp", trade_table).as_str(),
            )
            .await?;
        }
        ExchangeName::Gdax => {
            create_gdax_trade_table(pool, exchange_name, market, trade_table).await?;
            create_gdax_trade_table(
                pool,
                exchange_name,
                market,
                format!("{}_temp", trade_table).as_str(),
            )
            .await?;
        }
    }
    // Migrate trades fromm temp
    migrate_trades_from_temp(pool, exchange_name, market, trade_table).await?;
    // Finally drop the temp table
    drop_trade_table(
        pool,
        exchange_name,
        market,
        format!("{}_temp", trade_table).as_str(),
    )
    .await?;
    Ok(())
}

pub async fn create_ftx_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Create trade table
    // trades_EXCHANGE_MARKET_SOURCE
    // trades_ftx_btcperp_rest
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS trades_{}_{}_{} (
            market_id uuid NOT NULL,
            trade_id BIGINT NOT NULL,
            PRIMARY KEY (trade_id),
            price NUMERIC NOT NULL,
            size NUMERIC NOT NULL,
            side TEXT NOT NULL,
            liquidation BOOLEAN NOT NULL,
            time timestamptz NOT NULL
        )
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    // Create index on time
    let sql = format!(
        r#"
        CREATE INDEX IF NOT EXISTS trades_{e}_{m}_{t}_time_asc
        ON trades_{e}_{m}_{t} (time)
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn create_gdax_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Create trade table
    // trades_EXCHANGE_MARKET_SOURCE
    // trades_ftx_btcperp_rest
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS trades_{}_{}_{} (
            market_id uuid NOT NULL,
            trade_id BIGINT NOT NULL,
            PRIMARY KEY (trade_id),
            price NUMERIC NOT NULL,
            size NUMERIC NOT NULL,
            side TEXT NOT NULL,
            time timestamptz NOT NULL
        )
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    // Create index on time
    let sql = format!(
        r#"
        CREATE INDEX IF NOT EXISTS trades_{e}_{m}_{t}_time_asc
        ON trades_{e}_{m}_{t} (time)
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Drop the table if it exists
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS trades_{}_{}_{}
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_table(pool: &PgPool, table: &str) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS {}
        "#,
        table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn alter_trade_table_to_temp(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        ALTER TABLE IF EXISTS trades_{e}_{m}_{t}
        RENAME TO trades_{e}_{m}_{t}_temp
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn migrate_trades_from_temp(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        INSERT INTO trades_{e}_{m}_{t}
        SELECT * FROM trades_{e}_{m}_{t}_temp
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn insert_ftx_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trades: Vec<FtxTrade>,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, liquidation, time)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    for trade in trades.iter() {
        sqlx::query(&sql)
            .bind(market.market_id)
            .bind(trade.id)
            .bind(trade.price)
            .bind(trade.size)
            .bind(&trade.side)
            .bind(trade.liquidation)
            .bind(trade.time)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn insert_gdax_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trades: Vec<GdaxTrade>,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, time)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    for trade in trades.iter() {
        sqlx::query(&sql)
            .bind(market.market_id)
            .bind(trade.trade_id)
            .bind(trade.price)
            .bind(trade.size)
            .bind(&trade.side)
            .bind(trade.time)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn insert_gdax_trade(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trade: GdaxTrade,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, time)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql)
        .bind(market.market_id)
        .bind(trade.trade_id)
        .bind(trade.price)
        .bind(trade.size)
        .bind(&trade.side)
        .bind(trade.time)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_ftx_trade(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trade: FtxTrade,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, liquidation, time)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql)
        .bind(market.market_id)
        .bind(trade.id)
        .bind(trade.price)
        .bind(trade.size)
        .bind(&trade.side)
        .bind(trade.liquidation)
        .bind(trade.time)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_ftx_trades_by_time(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<FtxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        ORDER BY trade_id
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    let rows = sqlx::query_as::<_, FtxTrade>(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_gdax_trades_by_time(
    pool: &PgPool,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<GdaxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        ORDER BY trade_id
        "#,
        ExchangeName::Gdax.as_str(),
        market.as_strip(),
        trade_table,
    );
    let rows = sqlx::query_as::<_, GdaxTrade>(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_ftx_trades_by_table(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<FtxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM {}
        ORDER BY trade_id
        "#,
        table
    );
    let rows = sqlx::query_as::<_, FtxTrade>(&sql).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn select_gdax_trades_by_table(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<GdaxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM {}
        ORDER BY trade_id
        "#,
        table
    );
    let rows = sqlx::query_as::<_, GdaxTrade>(&sql).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn select_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<(DateTime<Utc>, Option<i64>), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            let ftx_trade = select_ftx_trade_first_stream(pool, exchange_name, market).await?;
            Ok((ftx_trade.time(), Some(ftx_trade.trade_id())))
        }
        ExchangeName::Gdax => {
            let gdax_trade = select_gdax_trade_first_stream(pool, exchange_name, market).await?;
            Ok((gdax_trade.time(), Some(gdax_trade.trade_id())))
        }
    }
}

pub async fn select_ftx_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<FtxTrade, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_{}_ws
        ORDER BY trade_id
        LIMIT 1
        "#,
        exchange_name.as_str(),
        market.as_strip()
    );
    let row = sqlx::query_as::<_, FtxTrade>(&sql).fetch_one(pool).await?;
    Ok(row)
}

pub async fn select_gdax_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<GdaxTrade, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM trades_{}_{}_ws
        ORDER BY trade_id
        LIMIT 1
        "#,
        exchange_name.as_str(),
        market.as_strip()
    );
    let row = sqlx::query_as::<_, GdaxTrade>(&sql).fetch_one(pool).await?;
    Ok(row)
}

pub async fn delete_trades_by_time(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DELETE FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    sqlx::query(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::configuration::get_configuration;
    use sqlx::PgPool;

    #[tokio::test]
    pub async fn insert_dup_trades_returns_error() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Create table
        let sql = r#"
            CREATE TABLE trades_test (
                trade_id BIGINT NOT NULL,
                PRIMARY KEY (trade_id)
            )    
        "#;
        sqlx::query(&sql)
            .execute(&pool)
            .await
            .expect("Could not create table.");

        // Create trade
        let sql_insert = r#"
            INSERT INTO trades_test (trade_id)
            VALUES ($1)
            ON CONFLICT (trade_id) DO NOTHING
        "#;
        let trade_id = 1;

        // Insert trade once
        sqlx::query(&sql_insert)
            .bind(trade_id)
            .execute(&pool)
            .await
            .expect("Could not insert trade first time.");
        // INsert trade a second time
        sqlx::query(&sql_insert)
            .bind(trade_id)
            .execute(&pool)
            .await
            .expect("Could not insert trade second time.");
        // match sqlx::query(&sql_insert).bind(trade_id).execute(&pool).await {
        //     Ok(_) => (),
        //     Err(e) => panic!(),
        // };
    }
}
