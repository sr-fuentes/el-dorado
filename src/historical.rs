use crate::candles::{
    get_ftx_candles_daterange, insert_candle, select_first_01d_candle, select_last_candle,
    select_previous_candle, Candle,
};
use crate::events::{Event, EventStatus, EventType};
use crate::exchanges::{
    client::RestClient, error::RestError, ftx::Trade as FtxTrade, Exchange, ExchangeName,
};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    update_market_data_status, MarketDataStatus, MarketDetail, MarketStatus, MarketTradeDetail,
};
use crate::mita::Mita;
use crate::trades::{
    create_ftx_trade_table, drop_create_trade_table, drop_table, drop_trade_table,
    insert_delete_ftx_trades, insert_delete_gdax_trades, insert_ftx_trades, insert_gdax_trades,
    select_ftx_trades_by_table, select_ftx_trades_by_time, select_gdax_trades_by_table,
    select_gdax_trades_by_time, select_trade_first_stream,
};
use crate::utilities::{get_input, TimeFrame, Trade};
use chrono::{DateTime, Datelike, Duration, DurationRound, TimeZone, Utc};
use csv::{Reader, Writer};
use rust_decimal_macros::dec;
use std::convert::TryInto;
use std::fs::File;

impl Mita {
    pub async fn historical(&self, end_source: &str) {
        // Get REST client for exchange
        let client = RestClient::new(&self.exchange.name);
        for market in self.markets.iter() {
            // Validate hb, create and validate 01 candles
            match &self.exchange.name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.validate_candles::<crate::exchanges::ftx::Candle>(&client, market)
                        .await
                }
                ExchangeName::Gdax => {
                    self.validate_candles::<crate::exchanges::gdax::Candle>(&client, market)
                        .await
                }
            };
            // Set start and end times for backfill
            let (start_ts, start_id) = match select_last_candle(
                &self.ed_pool,
                &self.exchange.name,
                &market.market_id,
                self.hbtf,
            )
            .await
            {
                Ok(c) => (
                    c.datetime + Duration::seconds(900),
                    Some(c.last_trade_id.parse::<i64>().unwrap()),
                ),
                Err(sqlx::Error::RowNotFound) => match &self.exchange.name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        (self.get_ftx_start(&client, market).await, None)
                    }
                    ExchangeName::Gdax => self.get_gdax_start(&client, market).await,
                },
                Err(e) => panic!("Sqlx Error getting start time: {:?}", e),
            };
            let (end_ts, end_id) = match end_source {
                "eod" => (
                    Utc::now().duration_trunc(Duration::days(1)).unwrap(),
                    Some(0_i64),
                ),
                // Loop until there is a trade in the ws table. If there is no trade, sleep
                // and check back until there is a trade.
                "stream" => {
                    println!(
                        "Fetching first {} streamed trade from ws.",
                        market.market_name
                    );
                    loop {
                        match select_trade_first_stream(
                            &self.trade_pool,
                            &self.exchange.name,
                            market,
                        )
                        .await
                        {
                            Ok((t, i)) => break (t + Duration::microseconds(1), i), // set trade time + 1 micro
                            Err(sqlx::Error::RowNotFound) => {
                                // There are no trades, sleep for 5 seconds
                                println!("Awaiting for ws trade for {}", market.market_name);
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            }
                            Err(e) => panic!("Sqlx Error: {:?}", e),
                        };
                    }
                }
                _ => panic!("Unsupported end time."),
            };
            // Update market data status to 'Syncing'
            update_market_data_status(&self.ed_pool, &market.market_id, &MarketStatus::Backfill)
                .await
                .expect("Could not update market status.");
            // Fill trades and candles from start to end
            match self.exchange.name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.fill_ftx(&client, &self.exchange, market, start_ts, end_ts)
                        .await
                }
                ExchangeName::Gdax => {
                    self.fill_gdax(
                        &client,
                        market,
                        start_ts,
                        start_id.unwrap() as i32,
                        end_ts,
                        end_id.unwrap() as i32,
                    )
                    .await
                }
            };
            // If `eod` end source then run validation on new backfill
            if end_source == "eod" {
                match &self.exchange.name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        self.validate_candles::<crate::exchanges::ftx::Candle>(&client, market)
                            .await
                    }
                    ExchangeName::Gdax => {
                        self.validate_candles::<crate::exchanges::gdax::Candle>(&client, market)
                            .await
                    }
                };
            }
        }
    }

    pub async fn fill_gdax(
        &self,
        client: &RestClient,
        market: &MarketDetail,
        start_ts: DateTime<Utc>,
        start_id: i32,
        end_ts: DateTime<Utc>,
        end_id: i32,
    ) {
        // From the start fill forward 1000 trades creating new 15m or HB candles as you go
        // until you reach the end which is either the first streamed trade or the end of the
        // previous full day
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
        let mut interval_start_id = start_id;
        let mut interval_start_ts = start_ts;
        let mut candle_ts = start_ts;
        while interval_start_id < end_id || interval_start_ts < end_ts {
            // Get next 1000 trades
            println!(
                "Filling {} trades from {} to {}.",
                market.market_name, interval_start_ts, end_ts
            );
            // Prevent 429 errors by only requesting 4 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match client
                .get_gdax_trades(
                    market.market_name.as_str(),
                    Some(1000),
                    None,
                    Some(interval_start_id + 1001),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 520 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    continue;
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            // Process sort and dedup trades
            if !new_trades.is_empty() {
                new_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                // Can unwrap first and last because there is at least one trade
                let oldest_trade = new_trades.first().unwrap();
                let newest_trade = new_trades.last().unwrap();
                // Update start id and ts
                interval_start_id = newest_trade.trade_id() as i32;
                interval_start_ts = newest_trade.time();
                println!(
                    "{} {} trades returned. First: {}, Last: {}",
                    new_trades.len(),
                    market.market_name,
                    oldest_trade.time(),
                    newest_trade.time()
                );
                println!(
                    "New interval_start id and ts: {} {}",
                    interval_start_id, interval_start_ts
                );
                // Insert trades into db
                insert_gdax_trades(
                    &self.trade_pool,
                    &self.exchange.name,
                    market,
                    "rest",
                    new_trades,
                )
                .await
                .expect("Failed to insert gdax rest trades.");
            }
            // Make new candles if trades moves to new interval
            let newest_floor = interval_start_ts
                .duration_trunc(Duration::minutes(15))
                .unwrap();
            if newest_floor > candle_ts {
                let mut interval_trades = select_gdax_trades_by_time(
                    &self.trade_pool,
                    market,
                    "rest",
                    candle_ts,
                    newest_floor,
                )
                .await
                .expect("Could not select trades from db.");
                // Sort and dedup
                interval_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                // interval_trades.dedup_by(|t1, t2| t1.trade_id == t2.trade_id);
                // Create daterange
                let date_range =
                    self.create_date_range(candle_ts, newest_floor, self.hbtf.as_dur());
                println!("Creating candles for dr: {:?}", date_range);
                // Create new candles from interval trades
                let candles = self
                    .create_interval_candles(market, date_range, &interval_trades)
                    .await;
                println!("Created {} candles to insert into db.", candles.len());
                // Insert candles to db
                self.insert_candles(market, candles).await;
                // Delete trades for market between start and end interval
                insert_delete_gdax_trades(
                    &self.trade_pool,
                    &self.exchange.name,
                    market,
                    candle_ts,
                    newest_floor,
                    "rest",
                    "processed",
                    interval_trades,
                )
                .await
                .expect("Failed to insert and delete ftx trades.");
                candle_ts = newest_floor;
                println!("New candle_ts: {:?}", candle_ts);
            };
        }
    }

    pub async fn get_gdax_start(
        &self,
        client: &RestClient,
        market: &MarketDetail,
    ) -> (DateTime<Utc>, Option<i64>) {
        // Get latest trade to establish current trade id and trade ts
        let exchange_trade = client
            .get_gdax_trades(&market.market_name, Some(1), None, None)
            .await
            .expect("Failed to get gdax trade.")
            .pop()
            .unwrap();
        let start_ts = Utc::now().duration_trunc(Duration::days(1)).unwrap() - Duration::days(91);
        let mut first_trade_id = 0_i64;
        let mut first_trade_ts = Utc.ymd(2017, 1, 1).and_hms(0, 0, 0);
        let mut last_trade_id = exchange_trade.trade_id();
        let mut mid = 1001_i64; // Once mid is < 1k, return that trade
        while first_trade_ts < start_ts - Duration::days(1) || first_trade_ts > start_ts {
            mid = (first_trade_id + last_trade_id) / 2;
            if mid < 1000 {
                break;
            };
            println!(
                "First / Mid / Last: {} {} {}",
                first_trade_id, mid, last_trade_id
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let exchange_trade = client
                .get_gdax_trades(
                    &market.market_name,
                    Some(1),
                    None,
                    Some(mid.try_into().unwrap()),
                )
                .await
                .expect("Failed to get gdax trade.")
                .pop()
                .unwrap();
            first_trade_ts = exchange_trade.time();
            if first_trade_ts < start_ts - Duration::days(1) {
                first_trade_id = mid;
            } else if first_trade_ts > start_ts {
                last_trade_id = mid;
            };
        }
        first_trade_ts = first_trade_ts.duration_trunc(Duration::days(1)).unwrap();
        println!("Final start: {} {}", mid, first_trade_ts);
        (first_trade_ts, Some(mid))
    }

    pub async fn fill_ftx(
        &self,
        client: &RestClient,
        exchange: &Exchange,
        market: &MarketDetail,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) {
        // From start to end fill forward trades in 15m buckets and create candle
        // Pagination returns trades in desc order from end timestamp in 100 trade batches
        // Use last trade out of 100 (earliest timestamp) to set as end time for next request for trades
        // Until trades returned is < 100 meaning there are no further trades for interval
        // Then advance interval start by interval length and set new end timestamp and begin
        // To reterive trades.
        let mut interval_start = start;
        while interval_start < end {
            // Set end of bucket to end of interval
            let interval_end = interval_start + Duration::seconds(900);
            let mut interval_end_or_last_trade =
                std::cmp::min(interval_start + Duration::seconds(900), end);
            println!(
                "Filling {} trades for interval from {} to {}.",
                market.market_name, interval_start, interval_end_or_last_trade
            );
            while interval_start < interval_end_or_last_trade {
                // Prevent 429 errors by only requesting 4 per second
                tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                let mut new_trades = match client
                    .get_ftx_trades(
                        market.market_name.as_str(),
                        Some(5000),
                        Some(interval_start),
                        Some(interval_end_or_last_trade),
                    )
                    .await
                {
                    Err(RestError::Reqwest(e)) => {
                        if e.is_timeout() || e.is_connect() || e.is_request() {
                            println!(
                                "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                e
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                            continue;
                        } else if e.is_status() {
                            match e.status() {
                                Some(s) => match s.as_u16() {
                                    500 | 502 | 503 | 520 | 530 => {
                                        println!(
                                            "{} status code. Waiting 30 seconds before retry {:?}",
                                            s, e
                                        );
                                        tokio::time::sleep(tokio::time::Duration::from_secs(30))
                                            .await;
                                        continue;
                                    }
                                    _ => {
                                        panic!("Status code not handled: {:?} {:?}", s, e)
                                    }
                                },
                                None => panic!("No status code for request error: {:?}", e),
                            }
                        } else {
                            panic!("Error (not timeout / connect / request): {:?}", e)
                        }
                    }
                    Err(e) => panic!("Other RestError: {:?}", e),
                    Ok(result) => result,
                };
                let num_trades = new_trades.len();
                if num_trades > 0 {
                    new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                    // Unwrap can be used because it will only be called
                    // if there is at least one element in new_trades vec
                    // Set end of interval to last trade returned for pagination
                    interval_end_or_last_trade = new_trades.first().unwrap().time;
                    let first_trade = new_trades.last().unwrap().time;
                    println!(
                        "{} {} trades returned. First: {}, Last: {}",
                        num_trades, market.market_name, interval_end_or_last_trade, first_trade
                    );
                    println!("New last trade ts: {}", interval_end_or_last_trade);
                    // FTX trades API takes time in microseconds. This could lead to a
                    // endless loop if there are more than 5000 trades in that microsecond.
                    // In that case move the end to last trade minus one microsecond.
                    if interval_end_or_last_trade == first_trade {
                        interval_end_or_last_trade =
                            interval_end_or_last_trade - Duration::microseconds(1);
                        println!(
                            "More than 100 trades in microsecond. Resetting to: {}",
                            interval_end_or_last_trade
                        );
                    }
                    // Save trades to db
                    insert_ftx_trades(&self.trade_pool, &exchange.name, market, "rest", new_trades)
                        .await
                        .expect("Failed to insert ftx trades.");
                };
                // If new trades returns less than 100 trades then there are no more trades
                // for that interval, create the candle and process the trades for that period
                // Do not create candle for last interval (when interval_end is greater than end time)
                // because the interval_end may not be complete (ie interval_end = 9:15 and current time
                // is 9:13, you dont want to create the candle because it is not closed yet).
                if num_trades < 5000 {
                    if interval_end <= end {
                        // Move trades from _rest to _processed and create candle
                        // Select trades for market between start and end interval
                        let mut interval_trades = select_ftx_trades_by_time(
                            &self.trade_pool,
                            &exchange.name,
                            market,
                            "rest",
                            interval_start,
                            interval_end,
                        )
                        .await
                        .expect("Could not fetch trades from db.");
                        // Check if there are interval trades, if there are: create candle and archive
                        // trades. If there are not, get previous trade details from previous candle
                        // and build candle from last
                        let new_candle = match interval_trades.len() {
                            0 => {
                                // Get previous candle
                                let previous_candle = select_previous_candle(
                                    &self.ed_pool,
                                    &exchange.name,
                                    &market.market_id,
                                    interval_start,
                                    TimeFrame::T15,
                                )
                                .await
                                .expect("No previous candle.");
                                // Create Candle
                                Candle::new_from_last(
                                    market.market_id,
                                    interval_start,
                                    previous_candle.close,
                                    previous_candle.last_trade_ts,
                                    &previous_candle.last_trade_id.to_string(),
                                )
                            }
                            _ => {
                                // Sort and dedup - dedup not needed as table has unique constraint
                                // on trade id and on insert conflict does nothing
                                interval_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                                // interval_trades.dedup_by(|t1, t2| t1.id == t2.id);
                                // Create Candle
                                Candle::new_from_trades(
                                    market.market_id,
                                    interval_start,
                                    &interval_trades,
                                )
                            }
                        };
                        // Insert into candles
                        insert_candle(
                            &self.ed_pool,
                            &exchange.name,
                            &market.market_id,
                            new_candle,
                            false,
                        )
                        .await
                        .expect("Could not insert new candle.");
                        println!("Candle {:?} inserted", interval_start);
                        // Delete trades for market between start and end interval
                        insert_delete_ftx_trades(
                            &self.trade_pool,
                            &exchange.name,
                            market,
                            interval_start,
                            interval_end,
                            "rest",
                            "processed",
                            interval_trades,
                        )
                        .await
                        .expect("Failed to insert and delete ftx trades.");
                    };
                    // Move to next interval start
                    interval_start = interval_start + Duration::seconds(900); // TODO! set to market heartbeat
                };
            }
        }
    }

    pub async fn get_ftx_start(&self, client: &RestClient, market: &MarketDetail) -> DateTime<Utc> {
        // Set end time to floor of today's date, start time to 90 days prior. Check each day if there
        // are trades and return the start date - 1 that first returns trades
        let end_time = Utc::now().duration_trunc(Duration::days(1)).unwrap();
        let mut start_time = end_time - Duration::days(90);
        while start_time < end_time {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let new_trades = client
                .get_ftx_trades(market.market_name.as_str(), Some(1), None, Some(start_time))
                .await
                .expect("Failed to get trades.");
            println!("New Trades: {:?}", new_trades);
            if !new_trades.is_empty() {
                return start_time;
            } else {
                start_time = start_time + Duration::days(1)
            };
        }
        start_time
    }
}

impl Inquisidor {
    pub async fn fill(&self) {
        // Fill trades to the beginning of the market open on exchange from the first candle/trade
        // day in el-dorado. Process will create a new trade detail table for each market working
        // from the first candle backwards until the first trade for the market. From there the
        // validation period will begin from the start through the current date.
        // Example:
        //      Market: BTC-PERP
        //      El-Dorado began with a 90 day sync starting 15-NOV-2021 to present. Existing trades
        //      are a mix of validated/unvalidated and archived trades.
        //      Process:
        //          1) Create Market Trade Details entry for BTC-PERP market
        //          2) Populate market_id, first_trade_ts, first_trade_id, prev_trade_day, prev_stat
        //          3) uuid | 2021-11-15 00:00:01 | 123456 | 2021-11-14 | get
        //          4) Event or process will create trades_ftx_btcperp_20211114 table
        //          5) Get trades for 14-Nov-2022 and put in that table
        //          6) Once complete, update first trade ts/id, prev day, create 01d candle
        //          7) Validate 01d can - if validated - archive trades, drop table
        //          7) Repeat for 13-Nov-2022 until you reach beginning of market
        //          8) At beginning set start_ts, set prev status to Completed
        //          9) Set next day to prev day, set next status to validate
        //          10) Validated next day, if validated - archive and move date forward
        //          11) If not validated, create manual validation event, send sms, exit
        // Get market to backfill
        let market = match self.get_fill_market().await {
            Some(m) => m,
            None => return,
        };
        // Validate market eligibility
        let market_eligible = self.validate_market_eligibility(&market);
        if !market_eligible {
            return;
        };
        // Get the ;market trade detail
        let mtd = self.get_market_trade_detail(&market).await;
        // Get fill event
        let event = match self.get_fill_event(&market, &mtd).await {
            Some(e) => e,
            None => {
                // No new events to create and process for the market. It is up to date in validated
                // trades from the start of the market through the last day of of tracking.
                println!("No fill event for market: {:?}.", market.market_name);
                println!(
                    "Prev Status: {:?}\nNext Status: {:?}",
                    mtd.previous_status, mtd.next_status
                );
                return;
            }
        };
        // Process backfill for market
        self.process_fill_event(&event).await;
    }

    pub async fn get_fill_market(&self) -> Option<MarketDetail> {
        let exchange: String = get_input("Enter Exchange to Backfill: ");
        let exchange: ExchangeName = match exchange.try_into() {
            Ok(exchange) => exchange,
            Err(err) => {
                // Exchange inputed is not part of existing exchanges
                println!("{:?} has not been added to El-Dorado.", err);
                println!("Available exchanges are: {:?}", self.list_exchanges());
                return None;
            }
        };
        let market_name: String = get_input("Enter Market Name to Backfill: ");
        let market = match self.markets.iter().find(|m| m.market_name == market_name) {
            Some(m) => m,
            None => {
                println!("{:?} not found in markets for {:?}", market_name, exchange);
                println!(
                    "Available markets are: {:?}",
                    self.list_markets(Some(&exchange))
                );
                return None;
            }
        };
        Some(market.clone())
    }

    pub fn validate_market_eligibility(&self, market: &MarketDetail) -> bool {
        // Validate market is eligible to fill:
        // Market detail contains a last candle - ie it has been live in El-Dorado
        if market.market_data_status != MarketStatus::Active || market.last_candle.is_none() {
            println!(
                "Market not eligible for backfill. \nStatus: {:?}\nLast Candle: {:?}",
                market.market_data_status, market.last_candle
            );
            return false;
        };
        true
    }

    pub async fn process_fill_event(&self, e: &Event) {
        // Process event
        match e.event_type {
            // Insert into db for processing during normal IG loop
            EventType::BackfillTrades => match e.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => e
                    .insert(&self.ig_pool)
                    .await
                    .expect("Failed to insert event."),
                ExchangeName::Gdax => self.process_event_backfill_trades(e).await,
            },
            // Do not insert into db. Process on separate instance. Manage API 429
            // calls through REST handling.
            EventType::ForwardFillTrades => self.process_event_forwardfill_trades(e).await,
            _ => (), // Unreachable
        }
    }

    pub async fn get_market_trade_detail(&self, market: &MarketDetail) -> MarketTradeDetail {
        let market_trade_details = MarketTradeDetail::select_all(&self.ig_pool)
            .await
            .expect("Failed to select market trade details.");
        match market_trade_details
            .iter()
            .find(|mtd| mtd.market_id == market.market_id)
        {
            Some(mtd) => mtd.clone(),
            None => {
                // No trade archive exists, create one
                let mtd = MarketTradeDetail::new(&self.ig_pool, market).await;
                mtd.insert(&self.ig_pool)
                    .await
                    .expect("Failed to insert market trade detail.");
                mtd
            }
        }
    }

    pub async fn get_fill_event(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
    ) -> Option<Event> {
        // Checks to see new or open events for a fill event for the given market. If there is no
        // event, it will try to create a new event. If there is no new event possible (the market
        // is fully synced from beginning to current day) it will return None.
        let events = Event::select_by_statuses_type(
            &self.ig_pool,
            &[EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        match events.iter().find(|e| e.market_id == market.market_id) {
            Some(e) =>
            // There exists an event, start there
            {
                Some(e.clone())
            }
            None => {
                // There is no event, create one then start here
                Event::new_fill_trades(market, mtd, &self.market(&mtd.market_id).exchange_name)
            }
        }
    }

    pub async fn is_backfill(&self, event: &Event) -> bool {
        // Determine is date of event is populated by fill process or by normal el-dorado trade /
        // candle processing.
        let market = self.market(&event.market_id);
        let start = event.start_ts.unwrap();
        let first_candle = select_first_01d_candle(&self.ig_pool, &market.market_id)
            .await
            .expect("Failed to select first 01d candle.");
        start < first_candle.datetime
    }

    pub async fn process_event_forwardfill_trades(&self, event: &Event) {
        // Get current market trade detail
        // Match the exchage and process the event
        let mut mtd = MarketTradeDetail::select(&self.ig_pool, &event.market_id)
            .await
            .expect("Failed to select market trade detail.");
        let mut current_event = Some(event.clone());
        println!("Current Event: {:?}", current_event);
        while current_event.is_some() {
            match event.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.process_ftx_forwardfill(event, &mtd).await;
                }
                ExchangeName::Gdax => {
                    self.process_gdax_forwardfill(event, &mtd).await;
                }
            };
            mtd = MarketTradeDetail::select(&self.ig_pool, &event.market_id)
                .await
                .expect("Failed to select market trade detail.");
            current_event =
                Event::new_fill_trades(self.market(&event.market_id), &mtd, &event.exchange_name);
        }
    }

    pub async fn process_ftx_forwardfill(&self, event: &Event, mtd: &MarketTradeDetail) {
        // Match the notes to a market data status
        let status = event.notes.as_ref().unwrap().clone();
        let status: MarketDataStatus = status.try_into().unwrap();
        let market = self.market(&event.market_id);
        let start = event.start_ts.unwrap();
        match status {
            MarketDataStatus::Completed => {} // completed, nothing to do
            MarketDataStatus::Get => {
                // TODO: Refactor into 3 sections - before input, input, after input
                // self.get_trades_for_day()
                // self.manual_evaluate_trades()
                // self.process_evaluation()
                // Only used to re-validate days processed via backfill, ongoing el-dorado days
                // are validated via the manage / manual ig processes
                // Re-download trades
                let new_trade_table = format!("ff_{}", start.format("%Y%m%d"));
                self.get_ftx_trades_dr_into_table(
                    event,
                    &new_trade_table,
                    start,
                    start + Duration::days(1),
                )
                .await;
                // Manually re-validate
                let is_valid = self.manual_evaluate_ftx_trades(event).await;
                if is_valid {
                    // If accepted, write to file and set status to Validate
                    // TODO: REFACTOR with Backfill and Archvive
                    // Check directory for exchange csv is created
                    let path = format!(
                        "{}/csv/{}",
                        &self.settings.application.archive_path,
                        &market.exchange_name.as_str()
                    );
                    std::fs::create_dir_all(&path).expect("Failed to create directories.");
                    let trade_table =
                        format!("trades_ftx_{}_{}", market.as_strip(), new_trade_table);
                    let trades_to_archive =
                        select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
                            .await
                            .expect("Failed to select backfill trades.");
                    // Define filename = TICKER_YYYYMMDD.csv
                    let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                    // Set filepath and file name
                    let fp = std::path::Path::new(&path).join(f);
                    // Write trades to file
                    let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
                    for trade in trades_to_archive.iter() {
                        wtr.serialize(trade).expect("Failed to serialize trade.");
                    }
                    wtr.flush().expect("Failed to flush wtr.");
                    // Drop tables ff and bf
                    let old_trade_table = format!("bf_{}", start.format("%Y%m%d"));
                    drop_trade_table(
                        &self.ig_pool,
                        &market.exchange_name,
                        market,
                        &new_trade_table,
                    )
                    .await
                    .expect("Failed to drop trade table.");
                    drop_trade_table(
                        &self.ig_pool,
                        &market.exchange_name,
                        market,
                        &old_trade_table,
                    )
                    .await
                    .expect("Failed to drop trade table.");
                    // Update mtd status to validated
                    mtd.update_next_status(&self.ig_pool, &MarketDataStatus::Validate)
                        .await
                        .expect("Failed to update mtd status.");
                };
            }
            MarketDataStatus::Validate => {
                println!("Validating forwardfill.");
                // Locate trade file for date
                let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                let archive_path = format!(
                    "{}/csv/{}",
                    &self.settings.application.archive_path,
                    &market.exchange_name.as_str()
                );
                let f_path = std::path::Path::new(&archive_path).join(f.clone());
                // Try location 3
                let bad_archive_path =
                    format!("{}/csv/gdax2", &self.settings.application.archive_path,);
                let bf_path = std::path::Path::new(&bad_archive_path).join(f.clone());
                if bf_path.exists() {
                    println!("File in location 3. Moving to correct location.");
                    // Create directories in the correct location
                    std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
                    // File exists but in wrong location - copy to correct location
                    std::fs::rename(bf_path, &f_path)
                        .expect("Failed to copy file from location 3.");
                }
                // Try location 2
                let bad_archive_path =
                    format!("{}/csv/gdax/gdax", &self.settings.application.archive_path,);
                let bf_path = std::path::Path::new(&bad_archive_path).join(f.clone());
                if bf_path.exists() {
                    println!("File in location 2. Moving to correct location.");
                    // Create directories in the correct location
                    std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
                    // File exists but in wrong location - copy to correct location
                    std::fs::rename(bf_path, &f_path)
                        .expect("Failed to copy file from location 2.");
                };
                // Try location 1 - Correct Location
                if f_path.exists() {
                    println!("File correct location. Updating next status.");
                    // File exists, day is validated. Update mtd next status to Archive
                    mtd.update_next_status(&self.ig_pool, &MarketDataStatus::Archive)
                        .await
                        .expect("Failed to update mtd status.");
                } else {
                    println!("File not located.");
                    // Determine if file creation is part of backfill or el-dorado
                    if self.is_backfill(event).await {
                        // If part of backfill process - update mtd status to GET
                        mtd.update_next_status(&self.ig_pool, &MarketDataStatus::Get)
                            .await
                            .expect("Failed to update mtd status.");
                    };
                };
            }
            MarketDataStatus::Archive => {
                // Get trade file path
                let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                let current_path = format!(
                    "{}/csv/{}",
                    &self.settings.application.archive_path,
                    &market.exchange_name.as_str()
                );
                let f_path = std::path::Path::new(&current_path).join(f.clone());
                // Set archive file path
                let archive_path = format!(
                    "{}/trades/{}/{}/{}/{}",
                    &self.settings.application.archive_path,
                    &market.exchange_name.as_str(),
                    &market.as_strip(),
                    start.format("%Y"),
                    start.format("%m")
                );
                // Create directories if not exists
                std::fs::create_dir_all(&archive_path).expect("Failed to create directories.");
                let a_path = std::path::Path::new(&archive_path).join(f.clone());
                // Move trade file to validated location
                std::fs::rename(f_path, &a_path).expect("Failed to copy file to trade folder.");
                // Update mtd next status and next day
                let mtd = mtd
                    .update_next_day_next_status(
                        &self.ig_pool,
                        &(start + Duration::days(1)),
                        &MarketDataStatus::Completed,
                    )
                    .await
                    .expect("Failed to update next day and next status.");
                // Set filepath and file name
                let file = File::open(a_path).expect("Failed to open file.");
                // Read file from new location
                let mut trades = Vec::new();
                let mut rdr = Reader::from_reader(file);
                for result in rdr.deserialize() {
                    let record: FtxTrade = result.expect("Faile to deserialize record.");
                    trades.push(record)
                }
                // Get first and last trades
                if !trades.is_empty() {
                    let first_trade = trades.first().unwrap();
                    let last_trade = trades.last().unwrap();
                    // Update first and last trade in the mtd
                    mtd.update_first_and_last_trades(&self.ig_pool, first_trade, last_trade)
                        .await
                        .expect("Failed to update first and last trade details.");
                }
            }
        };
    }

    pub async fn process_gdax_forwardfill(&self, _event: &Event, _mtd: &MarketTradeDetail) {}

    pub async fn process_event_backfill_trades(&self, event: &Event) {
        // Get current market trade detail
        // Match the exchange, if Ftx => Process the Event and mark as complete
        // if Gdax => Create loop to process event and the next until the next is None
        let mut mtd = MarketTradeDetail::select(&self.ig_pool, &event.market_id)
            .await
            .expect("Failed to select market trade detail.");
        match event.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => self.process_ftx_backfill(event, &mtd).await,
            ExchangeName::Gdax => {
                let mut current_event = Some(event.clone());
                println!("Current Event: {:?}", current_event);
                while current_event.is_some() {
                    self.process_gdax_backfill(&current_event.unwrap(), &mtd)
                        .await;
                    mtd = MarketTradeDetail::select(&self.ig_pool, &event.market_id)
                        .await
                        .expect("Failed to select market trade detail.");
                    current_event = Event::new_fill_trades(
                        self.market(&event.market_id),
                        &mtd,
                        &ExchangeName::Gdax,
                    );
                    println!("Current Event: {:?}", current_event);
                }
            }
        }
    }

    pub async fn process_ftx_backfill(&self, event: &Event, mtd: &MarketTradeDetail) {
        // Try to match the notes from the event to a market data status
        let status = event.notes.as_ref().unwrap().clone();
        let status: MarketDataStatus = status.try_into().unwrap();
        match status {
            MarketDataStatus::Completed => {} // Completed, nothing to do.
            MarketDataStatus::Get => {
                // Get market name to get strip name for table
                let market = self.market(&event.market_id);
                // Create trade table for market / exch / day
                let start = event.start_ts.unwrap();
                let trade_table = format!("bf_{}", start.format("%Y%m%d"));
                println!("Trade Table: {:?}", trade_table);
                create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, &trade_table)
                    .await
                    .expect("Failed to create trade table.");
                // Fill trade table with trades
                let end = event.start_ts.unwrap() + Duration::days(1);
                let mut end_or_last_trade = end;
                let mut total_trades: usize = 0;
                while start < end_or_last_trade {
                    // Prevent 429 errors by only requesting 4 per second
                    tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                    let mut new_trades = match self.clients[&event.exchange_name]
                        .get_ftx_trades(
                            market.market_name.as_str(),
                            Some(5000),
                            Some(start),
                            Some(end_or_last_trade),
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                continue;
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            continue;
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    let num_trades = new_trades.len();
                    total_trades += num_trades; // Add to running total of trades
                    if num_trades > 0 {
                        new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                        end_or_last_trade = new_trades.first().unwrap().time;
                        let first_trade = new_trades.last().unwrap().time;
                        println!(
                            "{} trade returned. First: {}, Last: {}",
                            num_trades, end_or_last_trade, first_trade
                        );
                        insert_ftx_trades(
                            &self.ftx_pool,
                            &event.exchange_name,
                            market,
                            &trade_table,
                            new_trades,
                        )
                        .await
                        .expect("Failed to insert backfill ftx trades.");
                    };
                    if num_trades < 5000 && total_trades > 0 {
                        // Trades returned less than REST API limit. No more trades to retreive.
                        break;
                    };
                    if total_trades == 0 {
                        // There are no trades on the day. End the backfill process
                        // Set prev status to complete
                        println!("No trades for day. Market backfill as complete.");
                        let _mtd = mtd
                            .update_prev_status(&self.ig_pool, &MarketDataStatus::Completed)
                            .await
                            .expect("Failed to update mtd.");
                        // Update current event to closed
                        event
                            .update_status(&self.ig_pool, &EventStatus::Done)
                            .await
                            .expect("Failed to update event status.");
                        return;
                        // TODO: Drop trade table for date
                    };
                }
                // Update mtd status to validate
                let mtd = mtd
                    .update_prev_status(&self.ig_pool, &MarketDataStatus::Validate)
                    .await
                    .expect("Failed to update mtd.");
                // Create new event and insert if it exists
                let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
                match new_event {
                    Some(e) => {
                        e.insert(&self.ig_pool)
                            .await
                            .expect("Failed to insert event.");
                    }
                    None => {
                        println!("Market complete, not further events.");
                        println!(
                            "Prev Status: {:?}\nNext Status: {:?}",
                            mtd.previous_status, mtd.next_status
                        );
                    }
                };
                // Update current event to closed
                event
                    .update_status(&self.ig_pool, &EventStatus::Done)
                    .await
                    .expect("Failed to update event status.");
            }
            MarketDataStatus::Validate => {
                // Validate trades - compare the value of all trades (price * qty) to the volume of
                // the ftx candle from exchange
                let market = self.market(&event.market_id);
                // Create trade table for market / exch / day
                let start = event.start_ts.unwrap();
                let trade_table = format!(
                    "trades_ftx_{}_bf_{}",
                    market.as_strip(),
                    start.format("%Y%m%d")
                );
                let trades = select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
                    .await
                    .expect("Failed to select ftx trades by table.");
                let value = trades
                    .iter()
                    .fold(dec!(0), |val, t| val + (t.price * t.size));
                let candle = get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
                    &self.clients[&event.exchange_name],
                    market,
                    start,
                    start,
                    86400,
                )
                .await
                .pop();
                // Update mtd status to archive if is validated, or move date back and update to get
                let mtd = match candle {
                    Some(c) => {
                        println!("Trade val: {:?}, Candle Vol: {:?}", value, c.volume);
                        // FTX candle volume is in USD for perps. Match volume to value calculated
                        if c.volume == value {
                            // The candle was validated, set status to archive
                            mtd.update_prev_status(&self.ig_pool, &MarketDataStatus::Archive)
                                .await
                                .expect("Failed to update mtd status.")
                        } else {
                            // The candle was not validated. Move prev day -1 and set status to get
                            mtd.update_prev_day_prev_status(
                                &self.ig_pool,
                                &(start - Duration::days(1)),
                                &MarketDataStatus::Get,
                            )
                            .await
                            .expect("Failed to update mtd.")
                        }
                    }
                    None => {
                        // No exchange candle to compare. Move prev day - 1. Set status to get
                        println!("No exchange candle.");
                        mtd.update_prev_day_prev_status(
                            &self.ig_pool,
                            &(start - Duration::days(1)),
                            &MarketDataStatus::Get,
                        )
                        .await
                        .expect("Failed to update mtd.")
                    }
                };
                // Create new event
                let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
                match new_event {
                    Some(e) => {
                        e.insert(&self.ig_pool)
                            .await
                            .expect("Failed to insert event.");
                    }
                    None => {
                        println!("Market complete, not further events.");
                        println!(
                            "Prev Status: {:?}\nNext Status: {:?}",
                            mtd.previous_status, mtd.next_status
                        );
                    }
                };
                // Update current event to closed
                event
                    .update_status(&self.ig_pool, &EventStatus::Done)
                    .await
                    .expect("Failed to update event status.");
            }
            MarketDataStatus::Archive => {
                // Save and archive trades
                let market = self.market(&event.market_id);
                let start = event.start_ts.unwrap();
                // Check directory for exchange csv is created
                let path = format!(
                    "{}/csv/{}",
                    &self.settings.application.archive_path,
                    &market.exchange_name.as_str()
                );
                std::fs::create_dir_all(&path).expect("Failed to create directories.");
                let trade_table = format!(
                    "trades_ftx_{}_bf_{}",
                    market.as_strip(),
                    start.format("%Y%m%d")
                );
                let trades_to_archive = select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
                    .await
                    .expect("Failed to select backfill trades.");
                let first_trade = trades_to_archive.first().unwrap();
                // Define filename = TICKER_YYYYMMDD.csv
                let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                // Set filepath and file name
                let fp = std::path::Path::new(&path).join(f);
                // Write trades to file
                let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
                for trade in trades_to_archive.iter() {
                    wtr.serialize(trade).expect("Failed to serialize trade.");
                }
                wtr.flush().expect("Failed to flush wtr.");
                // Drop trade table
                drop_table(&self.ftx_pool, &trade_table)
                    .await
                    .expect("Failed to drop backfill trade table.");
                // Update first trade id/ts. Move back prev day and set status to get
                let mtd = mtd
                    .update_prev_day_prev_status(
                        &self.ig_pool,
                        &(start - Duration::days(1)),
                        &MarketDataStatus::Get,
                    )
                    .await
                    .expect("Failed to update mtd.");
                // If first trade is ealier than mtd first trade, then update mtd
                let mtd = if mtd.first_trade_ts > trades_to_archive.first().unwrap().time {
                    let mtd = mtd
                        .update_first_trade(
                            &self.ig_pool,
                            &first_trade.time,
                            &first_trade.id.to_string(),
                        )
                        .await
                        .expect("Faile to update first trade details.");
                    mtd
                } else {
                    mtd
                };
                // Create new event
                let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
                match new_event {
                    Some(e) => {
                        e.insert(&self.ig_pool)
                            .await
                            .expect("Failed to insert event.");
                    }
                    None => {
                        println!("Market complete, not further events.");
                        println!(
                            "Prev Status: {:?}\nNext Status: {:?}",
                            mtd.previous_status, mtd.next_status
                        );
                    }
                };
                // Update current event to closed
                event
                    .update_status(&self.ig_pool, &EventStatus::Done)
                    .await
                    .expect("Failed to update event status.");
            }
        };
    }

    pub async fn process_gdax_backfill(&self, event: &Event, mtd: &MarketTradeDetail) {
        // Try to match the notes from the event to a market data status
        let status = event.notes.as_ref().unwrap().clone();
        let status: MarketDataStatus = status.try_into().unwrap();
        match status {
            MarketDataStatus::Completed => {} // Completed, nothing to do.
            MarketDataStatus::Get => {
                // Get market name to get strip name for table
                let market = self.market(&event.market_id);
                // Create trade table for market / exch / day
                let start = event.start_ts.unwrap();
                let trade_table = format!("bf_{}", start.format("%Y%m%d"));
                println!("Trade Table: {:?}", trade_table);
                drop_create_trade_table(
                    &self.gdax_pool,
                    &event.exchange_name,
                    market,
                    &trade_table,
                )
                .await
                .expect("Failed to drop/create trade table.");
                // Fill trade table with trades from start to end
                // Get trade id from the start of the candle prior
                let mut end_id = mtd.first_trade_id.parse::<i32>().unwrap();
                let mut earliest_trade_ts = mtd.first_trade_ts;
                while earliest_trade_ts > start {
                    // Get trades after the first trade of the previous candle in increments of 1000
                    // until the timestamp of the earliest trade returned is earlier than the start
                    // timestamp of the day that is being backfilled.
                    // Example:
                    // Backfill 12/22
                    // Get all trades from 2021:12:22 00:00:00 to 2021:12:23 00:00:00
                    // The first trade details for 12/13 are 12334567 and 2021:12:23 00:00:43
                    // start = 12/22
                    // end = 12/23
                    // end_id = 12334567
                    // earliest_trade_ts = 2021:12:23 00:00:43
                    // while 2021:12:23 00:00:43 > 2021:12:22 00:00:00 get the next 1000 trades.
                    // with those 1000 trades, reset earliest_trade_ts to the new earliest and
                    // continue until all trades are received
                    // Note that this is different logic than fill_gdax or recreate_gdax_candle
                    // In fill_gdax the loop is moving forward 1000 trades at a time to build
                    // candles as it gets them. The benefit is if there are any issues it can
                    // restart and pick up where it left off.
                    // In recreate_gdax_candle we are moving forward while checking back 1000 and
                    // forward 1000 extra candles to make sure it is all covered
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let mut new_trades = match self.clients[&event.exchange_name]
                        .get_gdax_trades(
                            market.market_name.as_str(),
                            Some(1000),
                            None,
                            Some(end_id),
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                continue;
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            continue;
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    if new_trades.is_empty() {
                        // No new trades were returned. This should only happen when an attempt is
                        // made to reterive trades after id = 1. You have reached the end of the
                        // backfill and the beginning of the market. Mark as completed.
                        // Update the first trade info - This will need to be validated
                        let mtd = mtd
                            .update_prev_day_prev_status(
                                &self.ig_pool,
                                &(start - Duration::days(1)),
                                &MarketDataStatus::Completed,
                            )
                            .await
                            .expect("Failed to update mtd.");
                        // If first trade is ealier than mtd first trade, then update mtd
                        let _mtd = if mtd.first_trade_id.parse::<i32>().unwrap() > end_id {
                            let mtd = mtd
                                .update_first_trade(
                                    &self.ig_pool,
                                    &earliest_trade_ts,
                                    &end_id.to_string(),
                                )
                                .await
                                .expect("Faile to update first trade details.");
                            mtd
                        } else {
                            mtd
                        };
                        // Update current event to closed
                        event
                            .update_status(&self.ig_pool, &EventStatus::Done)
                            .await
                            .expect("Failed to update event status.");
                        return;
                    } else {
                        // Add the returned trade the the trade table, update the earliest trade
                        // timestamp and id
                        new_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                        let earliest_trade = new_trades.first().unwrap();
                        let newest_trade = new_trades.last().unwrap();
                        earliest_trade_ts = earliest_trade.time;
                        end_id = earliest_trade.trade_id as i32;
                        println!(
                            "{} {} trades returned. First: {} Last: {}",
                            new_trades.len(),
                            market.market_name,
                            earliest_trade.time(),
                            newest_trade.time()
                        );
                        insert_gdax_trades(
                            &self.gdax_pool,
                            &event.exchange_name,
                            market,
                            &trade_table,
                            new_trades,
                        )
                        .await
                        .expect("Failed to insert trades.");
                    };
                }
                // Update mtd status to validate
                let _mtd = mtd
                    .update_prev_status(&self.ig_pool, &MarketDataStatus::Validate)
                    .await
                    .expect("Failed to update mtd.");
            }
            MarketDataStatus::Validate => {
                // Validate the trades - GDAX trade ids are sequential per product. Validate:
                // 1) There are no gaps in trade ids. Highest ID - Lowest ID + 1 = Num Trades
                // ie 1001 - 94 + 1 = 908 = 908 trades
                // 2) The next trade id in sequence falls on the next day
                // 3) The prev trade id in the sequence falls on the previous day
                let market = self.market(&event.market_id);
                // Create trade table for market / exch / day
                let start = event.start_ts.unwrap();
                let trade_table = format!(
                    "trades_gdax_{}_bf_{}",
                    market.as_strip(),
                    start.format("%Y%m%d")
                );
                let trades_in_table = select_gdax_trades_by_table(&self.gdax_pool, &trade_table)
                    .await
                    .expect("Failed to select backfill trades.");
                println!("{} trades in table.", trades_in_table.len());
                // Filter trades
                let mut trades_to_validate: Vec<crate::exchanges::gdax::Trade> = trades_in_table
                    .iter()
                    .filter(|t| t.time() >= start && t.time() < start + Duration::days(1))
                    .cloned()
                    .collect();
                println!("{} trades to validate.", trades_to_validate.len());
                // Sort trades by id
                trades_to_validate.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                // Perform validations
                println!("First: {:?}", trades_to_validate.first());
                println!("Last: {:?}", trades_to_validate.last());
                let validation_1 = if trades_to_validate.is_empty() {
                    false
                } else {
                    trades_to_validate.last().unwrap().trade_id
                        - trades_to_validate.first().unwrap().trade_id
                        + 1
                        == trades_to_validate.len() as i64
                };
                println!("Validation 1: {}", validation_1);
                let validation_2 = if trades_to_validate.is_empty() {
                    false
                } else {
                    let next_trade = match self.clients[&event.exchange_name]
                        .get_gdax_next_trade(
                            market.market_name.as_str(),
                            trades_to_validate.last().unwrap().trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                        "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                        e
                                    );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return event is imcomplete. Try to process again
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                    "{} status code. Waiting 30 seconds before retry {:?}",
                                                    s, e
                                                );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    println!("Next trade: {:?}", next_trade);
                    if !next_trade.is_empty() {
                        // Pop off the trade
                        let next_trade = next_trade.first().unwrap();
                        // Compare the day of the next trade to the last trade day of the trades
                        // to validated
                        let last_trade = trades_to_validate.last().unwrap();
                        next_trade.time().day() > last_trade.time().day()
                    } else {
                        // If next trade is empty, return false. There should always be a next trade
                        false
                    }
                };
                println!("Validation 2: {}", validation_2);
                let validation_3 = if trades_to_validate.is_empty() {
                    false
                } else {
                    let previous_trade = match self.clients[&event.exchange_name]
                        .get_gdax_previous_trade(
                            market.market_name.as_str(),
                            trades_to_validate.first().unwrap().trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                        "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                        e
                                    );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return event is imcomplete. Try to process again
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                    "{} status code. Waiting 30 seconds before retry {:?}",
                                                    s, e
                                                );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    println!("Previous trade: {:?}", previous_trade);
                    if !previous_trade.is_empty() {
                        // Pop off the trade
                        let previous_trade = previous_trade.first().unwrap();
                        // Compare the day of the next trade to the last trade day of the trades
                        // to validated
                        let first_trade = trades_to_validate.first().unwrap();
                        previous_trade.time().day() < first_trade.time().day()
                    } else {
                        // If previous trade is empty, check if first trade id = 1.
                        // If so there is no previous day and it is valid.
                        let first_trade = trades_to_validate.first().unwrap();
                        first_trade.trade_id == 1
                    }
                };
                println!("Validation 3: {}", validation_3);
                let _mtd = if validation_1 && validation_2 && validation_3 {
                    // All three validations met, market it as validated
                    mtd.update_prev_status(&self.ig_pool, &MarketDataStatus::Archive)
                        .await
                        .expect("Failed to update mtd status.")
                } else {
                    let mtd = mtd
                        .update_prev_day_prev_status(
                            &self.ig_pool,
                            &(start - Duration::days(1)),
                            &MarketDataStatus::Get,
                        )
                        .await
                        .expect("Failed to update mtd.");
                    // Update first id
                    if !trades_to_validate.is_empty() {
                        let earliest_trade = trades_to_validate.first().unwrap();
                        if mtd.first_trade_id.parse::<i32>().unwrap()
                            > earliest_trade.trade_id as i32
                        {
                            mtd.update_first_trade(
                                &self.ig_pool,
                                &earliest_trade.time,
                                &earliest_trade.trade_id.to_string(),
                            )
                            .await
                            .expect("Failed to update previous trade.")
                        } else {
                            mtd
                        }
                    } else {
                        mtd
                    }
                };
                // // Create new event
                // let new_event = Event::new_backfill_trades(&mtd, &event.exchange_name);
                // match new_event {
                //     Some(e) => {
                //         e.insert(&self.ig_pool)
                //             .await
                //             .expect("Failed to insert event.");
                //     }
                //     None => {
                //         println!("Market complete, not further events.");
                //         println!(
                //             "Prev Status: {:?}\nNext Status: {:?}",
                //             mtd.previous_status, mtd.next_status
                //         );
                //     }
                // };
                // // Update current event to closed
                // event
                //     .update_status(&self.ig_pool, &EventStatus::Done)
                //     .await
                //     .expect("Failed to update event status.");
            }
            MarketDataStatus::Archive => {
                // Save and archive trades
                let market = self.market(&event.market_id);
                let start = event.start_ts.unwrap();
                // Check directory for exchange csv is created
                let path = format!(
                    "{}/csv/{}",
                    &self.settings.application.archive_path,
                    &market.exchange_name.as_str()
                );
                std::fs::create_dir_all(&path).expect("Failed to create directories.");
                let trade_table = format!(
                    "trades_gdax_{}_bf_{}",
                    market.as_strip(),
                    start.format("%Y%m%d")
                );
                let trades_in_table = select_gdax_trades_by_table(&self.gdax_pool, &trade_table)
                    .await
                    .expect("Failed to select backfill trades.");
                // Filter trades
                let trades_to_archive: Vec<crate::exchanges::gdax::Trade> = trades_in_table
                    .iter()
                    .filter(|t| t.time() >= start && t.time() < start + Duration::days(1))
                    .cloned()
                    .collect();
                // Define filename = TICKER_YYYYMMDD.csv
                let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                // Set filepath and file name
                let fp = std::path::Path::new(&path).join(f);
                // Write trades to file
                let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
                for trade in trades_to_archive.iter() {
                    wtr.serialize(trade).expect("Failed to serialize trade.");
                }
                wtr.flush().expect("Failed to flush wtr.");
                // Drop trade table
                drop_table(&self.gdax_pool, &trade_table)
                    .await
                    .expect("Failed to drop backfill trade table.");
                // Update first trade id/ts. Move back prev day and set status to get
                let mtd = mtd
                    .update_prev_day_prev_status(
                        &self.ig_pool,
                        &(start - Duration::days(1)),
                        &MarketDataStatus::Get,
                    )
                    .await
                    .expect("Failed to update mtd.");
                // If first trade is ealier than mtd first trade, then update mtd
                let _mtd = if !trades_to_archive.is_empty() {
                    let first_trade = trades_to_archive.first().unwrap();
                    let mtd = if mtd.first_trade_ts > trades_to_archive.first().unwrap().time {
                        let mtd = mtd
                            .update_first_trade(
                                &self.ig_pool,
                                &first_trade.time,
                                &first_trade.trade_id.to_string(),
                            )
                            .await
                            .expect("Faile to update first trade details.");
                        mtd
                    } else {
                        mtd
                    };
                    mtd
                } else {
                    mtd
                };
                // // Create new event
                // let new_event = Event::new_backfill_trades(&mtd, &event.exchange_name);
                // match new_event {
                //     Some(e) => {
                //         e.insert(&self.ig_pool)
                //             .await
                //             .expect("Failed to insert event.");
                //     }
                //     None => {
                //         println!("Market complete, not further events.");
                //         println!(
                //             "Prev Status: {:?}\nNext Status: {:?}",
                //             mtd.previous_status, mtd.next_status
                //         );
                //     }
                // };
                // // Update current event to closed
                // event
                //     .update_status(&self.ig_pool, &EventStatus::Done)
                //     .await
                //     .expect("Failed to update event status.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::get_configuration;
    use crate::events::{Event, EventStatus, EventType};
    use crate::exchanges::ftx::Trade as FtxTrade;
    use crate::exchanges::{client::RestClient, ExchangeName};
    use crate::inquisidor::Inquisidor;
    use crate::markets::{
        select_market_detail_by_name, select_market_details, MarketDataStatus, MarketDetail,
    };
    use crate::mita::Mita;
    use crate::trades::{create_ftx_trade_table, drop_trade_table, insert_ftx_trade};
    use chrono::{Duration, TimeZone, Utc};
    use csv::Writer;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use sqlx::PgPool;
    use uuid::Uuid;

    pub async fn prep_ftx_market(pool: &PgPool) {
        // Update market to active with valid timestamp
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(pool)
            .await
            .expect("Failed to update last candle to null.");
        // Clear market trade details and daily candles
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(pool)
            .await
            .expect("Failed to update market trade details.");
        let sql = r#"
            DELETE FROM events
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(pool)
            .await
            .expect("Failed to update market trade details.");
        let sql = r#"
            DELETE FROM candles_01d
            WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
            "#;
        sqlx::query(sql)
            .execute(pool)
            .await
            .expect("Failed to update market trade details.");
        let sql = r#"
            INSERT INTO candles_01d (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
                market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19)
            "#;
        sqlx::query(sql)
            .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(100))
            .bind(dec!(0))
            .bind(dec!(10))
            .bind(dec!(5000))
            .bind(20)
            .bind(10)
            .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
            .bind("1234")
            .bind(true)
            .bind(Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap())
            .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
            .bind("1234")
            .bind(true)
            .bind(false)
            .execute(pool)
            .await
            .expect("Failed to insert candle for test.");
    }

    pub async fn prep_ftx_table_with_trade(
        pool: &PgPool,
        market: &MarketDetail,
        price: Decimal,
        quantity: Decimal,
    ) {
        // Create trade table for market / exch / day
        let trade_table = "bf_20211130";
        drop_trade_table(pool, &ExchangeName::Ftx, market, trade_table)
            .await
            .expect("Failed to drop trade table.");
        create_ftx_trade_table(pool, &ExchangeName::Ftx, market, trade_table)
            .await
            .expect("Failed to create trade table.");
        // Create trade
        let trade = create_ftx_trade(price, quantity);
        // Insert trade into table
        insert_ftx_trade(pool, &ExchangeName::Ftx, market, trade_table, trade)
            .await
            .expect("Failed to insert trade.");
    }

    pub fn prep_ftx_file(path: &str) {
        // Create trade
        let trade = create_ftx_trade(dec!(886644021.83975), dec!(1));
        let trades = vec![trade];
        // Check directory is created
        std::fs::create_dir_all(&path).expect("Failed to create directories.");
        // Set filepath
        let fp = std::path::Path::new(&path).join("SOLPERP_2021-11-30.csv");
        // Write trades to file
        let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
        for trade in trades.iter() {
            wtr.serialize(trade).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
    }

    pub fn create_ftx_trade(price: Decimal, quantity: Decimal) -> FtxTrade {
        FtxTrade {
            id: 123,
            price,
            size: quantity,
            side: "buy".to_string(),
            liquidation: false,
            time: Utc.ymd(2021, 11, 30).and_hms(0, 1, 0),
        }
    }

    #[tokio::test]
    pub async fn get_gdax_start_old_asset_returns_90d() {
        // Create mita
        let m = Mita::new().await;
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Create rest client
        let client = RestClient::new(&ExchangeName::Gdax);
        // Select old asset (BTC or ETH) and run get gdax start
        let market_details = select_market_details(&pool)
            .await
            .expect("Failed to select market details.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "BTC-USD")
            .unwrap();
        // Get gdax start
        println!("Getting GDAX start for BTC-USD");
        let (id, ts) = m.get_gdax_start(&client, market).await;
        println!("ID / TS: {:?} {:?}", id, ts);
    }

    #[tokio::test]
    pub async fn get_gdax_start_new_asset_returns_first_day() {
        // Create mita
        let m = Mita::new().await;
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Create rest client
        let client = RestClient::new(&ExchangeName::Gdax);
        // Select old asset (BTC or ETH) and run get gdax start
        let market_details = select_market_details(&pool)
            .await
            .expect("Failed to select market details.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "ORCA-USD")
            .unwrap();
        // Get gdax start
        println!("Getting GDAX start for ORCA-USD");
        let (id, ts) = m.get_gdax_start(&client, market).await;
        println!("ID / TS: {:?} {:?}", id, ts);
    }

    #[tokio::test]
    pub async fn backfill_ftx_with_no_candle_fails() {
        // Setup failing market scenario
        let ig = Inquisidor::new().await;
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', Null)
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Select the market
        let market = select_market_detail_by_name(&ig.ig_pool, "SOL-PERP")
            .await
            .expect("Failed to select market detail.");
        // Test the eligibility fails
        assert!(!ig.validate_market_eligibility(&market));
    }

    #[tokio::test]
    pub async fn backfill_ftx_with_non_active_market_fails() {
        // Setup
        let ig = Inquisidor::new().await;
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('terminated', '2022-09-21 18:30:00+00')
            WHERE market_name = 'SOL-PERP'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        // Select the market
        let market = select_market_detail_by_name(&ig.ig_pool, "SOL-PERP")
            .await
            .expect("Failed to select market detail.");
        // Test the eligibility fails
        assert!(!ig.validate_market_eligibility(&market));
    }

    #[tokio::test]
    pub async fn backfill_ftx_with_no_mtd_creates_new() {
        // Setup
        // Get pool and prep market
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig instance will pick up new data items
        let ig = Inquisidor::new().await;
        // Get test market
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        // Test
        let mtd = ig.get_market_trade_detail(&market).await;
        // Validate
        assert_eq!(mtd.previous_status, MarketDataStatus::Get);
    }

    #[tokio::test]
    pub async fn backfill_ftx_with_completed_does_nothing() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd and event
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mtd = ig.get_market_trade_detail(&market).await;
        let mut event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Change event notes to 'completed'
        event.notes = Some("completed".to_string());
        // Process the completed backfill event -> It should do nothing
        ig.process_ftx_backfill(&event, &mtd).await;
    }

    #[tokio::test]
    pub async fn backfill_ftx_get_gets_trades_updates_mtd_creates_new_event() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd and event
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mtd = ig.get_market_trade_detail(&market).await;
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Process the event to get the trades for the first day
        ig.process_event_backfill_trades(&event).await;
        // Assert mtd status is now validated
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.previous_status, MarketDataStatus::Validate);
        // Assert new event was created to validate trades
        let mi = Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap();
        let events = Event::select_by_statuses_type(
            &ig.ig_pool,
            &vec![EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = events.iter().find(|e| e.market_id == mi).unwrap();
        assert_eq!(event.notes, Some("validate".to_string()));
    }

    #[tokio::test]
    pub async fn backfill_ftx_get_gets_trades_zero_trades_completes_backfill() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        let sql = r#"
            UPDATE candles_01d
            SET datetime = '2017-12-31 00:00:00'
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update market trade details.");
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Run backfill to create mtd and event
        // Get mtd and event
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mtd = ig.get_market_trade_detail(&market).await;
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Process the event to get the trades for the first day
        ig.process_event_backfill_trades(&event).await;
        // Assert mtd status is now completed
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.previous_status, MarketDataStatus::Completed);
        // Assert new event was NOT created and there is no new event
        let mi = Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap();
        let events = Event::select_by_statuses_type(
            &ig.ig_pool,
            &vec![EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = events.iter().find(|e| e.market_id == mi);
        assert!(event.is_none());
    }

    #[tokio::test]
    pub async fn backfill_ftx_validate_success_updates_mtd_to_archive_creates_new_event() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to validate
        mtd.previous_status = MarketDataStatus::Validate;
        let original_prev_date = mtd.previous_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Add correct trade to trade table
        prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(886644021.83975), dec!(1)).await;
        // Process the event
        ig.process_event_backfill_trades(&event).await;
        // Assert mtd is now archive
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.previous_status, MarketDataStatus::Archive);
        // Assert the new event was created to archive trades
        let events = Event::select_by_statuses_type(
            &ig.ig_pool,
            &vec![EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = events
            .iter()
            .find(|e| e.market_id == market.market_id)
            .unwrap();
        assert_eq!(event.notes, Some("archive".to_string()));
        // Assert new mtd next date is not changed
        assert_eq!(original_prev_date, mtd.previous_trade_day);
    }

    #[tokio::test]
    pub async fn backfill_ftx_validate_failure_updates_mtd_to_next_get_creates_new_event() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to validate
        mtd.previous_status = MarketDataStatus::Validate;
        let original_prev_date = mtd.previous_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Add incorrect trade to trade table
        prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(123.321), dec!(1)).await;
        // Process the event
        ig.process_event_backfill_trades(&event).await;
        // Assert mtd is now archive
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.previous_status, MarketDataStatus::Get);
        // Assert the new event was created to archive trades
        let events = Event::select_by_statuses_type(
            &ig.ig_pool,
            &vec![EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = events
            .iter()
            .find(|e| e.market_id == market.market_id)
            .unwrap();
        assert_eq!(event.notes, Some("archive".to_string()));
        // Assert new mtd next date is not changed
        assert_eq!(
            original_prev_date,
            mtd.previous_trade_day + Duration::days(1)
        );
    }

    #[tokio::test]
    pub async fn backfill_ftx_archive_updates_mtd_creates_new_event() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to validate
        mtd.previous_status = MarketDataStatus::Archive;
        let original_prev_date = mtd.previous_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Add correct trade to trade table
        prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(886644021.83975), dec!(1)).await;
        // Process the event
        ig.process_event_backfill_trades(&event).await;
        // Assert mtd is now get
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.previous_status, MarketDataStatus::Get);
        // Assert the new event was created to archive trades
        let events = Event::select_by_statuses_type(
            &ig.ig_pool,
            &vec![EventStatus::New, EventStatus::Open],
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = events
            .iter()
            .find(|e| e.market_id == market.market_id)
            .unwrap();
        assert_eq!(event.notes, Some("get".to_string()));
        // Assert new mtd next date is not changed
        assert_eq!(
            original_prev_date,
            mtd.previous_trade_day + Duration::days(1)
        );
        // Assert file exists
        let f = format!(
            "{}_{}.csv",
            market.as_strip(),
            original_prev_date.format("%F")
        );
        let archive_path = format!(
            "{}/csv/{}",
            &ig.settings.application.archive_path,
            &market.exchange_name.as_str()
        );
        let f_path = std::path::Path::new(&archive_path).join(f.clone());
        assert!(f_path.exists());
    }

    #[tokio::test]
    pub async fn backfill_ftx_completed_creates_first_forwardfill_event() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Get mtd and event
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // Assert event is forwardfill
        assert_eq!(event.event_type, EventType::ForwardFillTrades);
        // Assert event is validate
        assert_eq!(event.notes, Some("validate".to_string()));
    }

    #[tokio::test]
    pub async fn forwardfill_ftx_get_accept_writes_file() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Add correct trade to trade table
        prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(1234021.83975), dec!(1)).await;
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        // Modify mtd next status to some(get)
        mtd.next_status = Some(MarketDataStatus::Get);
        // Process forwardfill event   
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;     
    }

    // pub async fn forwardfill_ftx_get_reject_does_nothing() {}

    #[tokio::test]
    pub async fn forwardfill_ftx_validate_loc3_moves_and_creates_archive() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Set up trade file in location 3
        let path = format!("{}/csv/gdax2", &ig.settings.application.archive_path,);
        prep_ftx_file(&path);
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        let original_next_date = mtd.next_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;
        // Assert mtd is now archive
        let mut mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
        // Assert new mtd next date is not changed
        assert_eq!(original_next_date, mtd.next_trade_day);
        // Assert next event it correct
        // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        mtd.previous_status = MarketDataStatus::Completed;
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        assert_eq!(
            event.start_ts,
            Some(mtd.previous_trade_day + Duration::days(1))
        );
        assert_eq!(event.notes.unwrap(), "archive".to_string());
    }

    #[tokio::test]
    pub async fn forwardfill_ftx_validate_loc2_moves_and_creates_archive() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Set up trade file in location 3
        let path = format!("{}/csv/gdax/gdax", &ig.settings.application.archive_path,);
        prep_ftx_file(&path);
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        let original_next_date = mtd.next_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;
        // Assert mtd is now archive
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
        // Assert new mtd next date is not changed
        assert_eq!(original_next_date, mtd.next_trade_day);
    }

    #[tokio::test]
    pub async fn fowardfill_ftx_validate_loc1_creates_archive() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Set up trade file in proper location
        let path = format!(
            "{}/csv/{}",
            &ig.settings.application.archive_path,
            &market.exchange_name.as_str(),
        );
        prep_ftx_file(&path);
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        let original_next_date = mtd.next_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;
        // Assert mtd is now archive
        let mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
        // Assert new mtd next date is not changed
        assert_eq!(original_next_date, mtd.next_trade_day);
    }

    #[tokio::test]
    pub async fn forwardfill_ftx_validate_noloc_creates_get() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        let original_next_date = mtd.next_trade_day;
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;
        // Assert mtd is now archive
        let mut mtd = ig.get_market_trade_detail(&market).await;
        assert_eq!(mtd.next_status, Some(MarketDataStatus::Get));
        // Assert new mtd next date is not changed
        assert_eq!(original_next_date, mtd.next_trade_day);
        // Assert next event it correct
        // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        mtd.previous_status = MarketDataStatus::Completed;
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        assert_eq!(
            event.start_ts,
            Some(mtd.previous_trade_day + Duration::days(1))
        );
        assert_eq!(event.notes.unwrap(), "get".to_string());
    }

    #[tokio::test]
    pub async fn forwardfill_ftx_archive_moves_file_creates_completed() {
        // Setup
        let ig = Inquisidor::new().await;
        prep_ftx_market(&ig.ig_pool).await;
        // Get mtd and modify for test
        let market = ig
            .markets
            .iter()
            .find(|m| m.market_name == "SOL-PERP")
            .unwrap();
        // Set up trade file in proper location
        let path = format!(
            "{}/csv/{}",
            &ig.settings.application.archive_path,
            &market.exchange_name.as_str(),
        );
        prep_ftx_file(&path);
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // Modify mtd prev status to completed
        mtd.previous_status = MarketDataStatus::Completed;
        // Modify mtd next status to archive
        mtd.next_status = Some(MarketDataStatus::Archive);
        // Modify mt prev date to 11/29 (as 11/30 is our test day)
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        let original_next_date = mtd.previous_trade_day + Duration::days(1);
        // Get event
        let event = ig.get_fill_event(&market, &mtd).await.unwrap();
        // println!("{:?}", event);
        // Process event
        ig.process_ftx_forwardfill(&event, &mtd).await;
        // Assert mtd is now archive
        let mut mtd = ig.get_market_trade_detail(&market).await;
        // println!("mtd: {:?}", mtd);
        assert_eq!(mtd.next_status, Some(MarketDataStatus::Completed));
        // Assert new mtd next date incremented
        assert_eq!(
            Some(original_next_date + Duration::days(1)),
            mtd.next_trade_day
        );
        // Assert next event it correct
        // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
        mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
        mtd.previous_status = MarketDataStatus::Completed;
        // println!("{:?}", mtd);
        let event = ig.get_fill_event(&market, &mtd).await;
        assert!(event.is_none());
    }

    #[tokio::test]
    pub async fn backfill_gdax_with_completed_does_nothing() {
        // Setup
        let ig = Inquisidor::new().await;
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', $1)
            WHERE market_name = 'AAVE-USD'
            "#;
        sqlx::query(sql)
            .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update market trade details.");
        let sql = r#"
            DELETE FROM candles_01d
            WHERE market_id = $1
            "#;
        sqlx::query(sql)
            .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to delete 01 candles.");
        let sql = r#"
            DELETE FROM events
            WHERE event_type = 'backfilltrades'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to delete events.");
        let sql = r#"
            INSERT INTO candles_01d (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
                market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19)
            "#;
        sqlx::query(sql)
            .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(100))
            .bind(dec!(0))
            .bind(dec!(10))
            .bind(dec!(5000))
            .bind(20)
            .bind(10)
            .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
            .bind("1234")
            .bind(true)
            .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
            .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
            .bind("1234")
            .bind(true)
            .bind(false)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to insert candle for test.");
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Run backfill to create mtd and event and run backfill loop
        ig.fill().await;
    }

    #[tokio::test]
    pub async fn backfill_gdax_get_validate_archive() {
        // TODO: Refactor into get / validate / archive tests with assertions
        // Setup
        let ig = Inquisidor::new().await;
        let sql = r#"
            UPDATE markets
            SET (market_data_status, last_candle) = ('active', $1)
            WHERE market_name = 'AAVE-USD'
            "#;
        sqlx::query(sql)
            .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update last candle to null.");
        let sql = r#"
            DELETE FROM market_trade_details
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to update market trade details.");
        let sql = r#"
            DELETE FROM candles_01d
            WHERE market_id = $1
            "#;
        sqlx::query(sql)
            .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to delete 01 candles.");
        let sql = r#"
            DELETE FROM events
            WHERE event_type = 'backfilltrades'
            "#;
        sqlx::query(sql)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to delete events.");
        let sql = r#"
            INSERT INTO candles_01d (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
                market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19)
            "#;
        sqlx::query(sql)
            .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(50))
            .bind(dec!(100))
            .bind(dec!(0))
            .bind(dec!(10))
            .bind(dec!(5000))
            .bind(20)
            .bind(10)
            .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
            .bind("13183391")
            .bind(true)
            .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
            .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
            .bind("13183391")
            .bind(true)
            .bind(false)
            .execute(&ig.ig_pool)
            .await
            .expect("Failed to insert candle for test.");
        // New ig will pick up new data items
        let ig = Inquisidor::new().await;
        // Run backfill to create mtd and event and run backfill loop
        ig.fill().await;
    }
}
