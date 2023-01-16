use crate::candles::{select_first_01d_candle, CandleType, ProductionCandle, ResearchCandle};
use crate::configuration::Database;
use crate::eldorado::ElDorado;
use crate::events::{Event, EventStatus, EventType};
use crate::exchanges::{ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    select_market_detail, MarketCandleDetail, MarketDataStatus, MarketDetail, MarketStatus,
    MarketTradeDetail,
};
use crate::mita::Heartbeat;
use crate::trades::{drop_trade_table, PrIdTi, Trade};
use crate::utilities::{get_input, TimeFrame};
use chrono::{DateTime, Duration, DurationRound, Utc};
use csv::{Reader, Writer};
use rust_decimal_macros::dec;
use std::{collections::HashMap, convert::TryInto, fs::File, path::Path, path::PathBuf};

impl ElDorado {
    pub async fn sync(&self) -> HashMap<String, Heartbeat> {
        let mut heartbeats: HashMap<String, Heartbeat> = HashMap::new();
        for market in self.markets.iter() {
            // For each market in eldorado instnace, determine the start time and id for the sync
            let (sync_start, last_trade, mut candles) = self.calculate_sync_start(market).await;
            println!("Sync Start: {}\tLast Trade: {:?}", sync_start, last_trade);
            println!("Candles Loaded: {:?}", candles.len());
            let first_trade: PrIdTi = self.calculate_sync_end(market).await;
            println!("Sync End: {:?}", first_trade.dt);
            // Fill the trades and aggregate to candles for start to end period
            let mut fill_candles = self
                .fill_trades_and_make_candles(market, sync_start, last_trade, first_trade)
                .await;
            candles.append(&mut fill_candles);
            // Create the heartbeat for the market given the sync'd candles
            let heartbeat = self.create_heartbeat(market, candles).await;
            heartbeats.insert(market.market_name.clone(), heartbeat);
        }
        for hb in heartbeats.iter() {
            println!(
                "Heartbeat for {}:\tDT: {}\tLast: {}",
                hb.0, hb.1.ts, hb.1.last
            );
            for c in hb.1.candles.iter() {
                println!(
                    "TF: {}\tFirst: {}\tLast: {}\tCount: {}",
                    c.0,
                    c.1.first().unwrap().datetime,
                    c.1.last().unwrap().datetime,
                    c.1.len()
                );
            }
            if hb.1.metrics.is_none() {
                println!("No metrics.");
            } else {
                for m in hb.1.metrics.as_ref().unwrap().iter() {
                    println!("Metric: {}\t{}\t{}", m.time_frame, m.lbp, m.datetime);
                }
            }
        }
        heartbeats
    }

    // The start of the sync is the determined by the market status and data. If it is a brand
    // new market, determined by there being no MarketTradeDetail or MarketCandleDetail (validated
    // and archived trade and candle data), then the start is 92 days prior to current day or the
    // openeing of the given market on the exchange. If however, there is a MarketTradeDetail or
    // MarketCandleDetail, the start begins at the end of the last candle from these sources. The
    // system will also consider existing candles that were created but not validated/archived
    // by checking the production candle table, this is to speed up the sync using data that was
    // previously used in the last run of the system.
    async fn calculate_sync_start(
        &self,
        market: &MarketDetail,
    ) -> (DateTime<Utc>, Option<PrIdTi>, Vec<ProductionCandle>) {
        // Set the min date that is needed for start - 92 days prior
        let mut sync_start =
            self.start_dt.duration_trunc(Duration::days(1)).unwrap() - Duration::days(92);
        let mut last_trade: Option<PrIdTi> = None;
        let mut candles = Vec::new();
        println!("Initialize start to 92 days prior.");
        println!("Sync Start: {}", sync_start);
        // Try checking the mcd record for last candle and confirming candles are in candles table
        let mcd_end = self.try_mcd_start(market, &sync_start).await;
        (sync_start, last_trade) = match mcd_end {
            Some(mut r) => {
                println!("MCD start succesful. Adding {} candles.", r.2.len());
                candles.append(&mut r.2);
                // Start now equals the last trade captured in the last candle. Try mtd and try eld
                // Will need to check if the id field is Some or None to determine start dr for
                // trades or candles
                println!("New Sync Start: {}\tLast Trade: {}", r.0, r.1);
                (r.0, Some(r.1))
            }
            None => (sync_start, last_trade),
        };
        // Try checking the mtd record for last trades and confirming trade table/trades exist
        let mtd_end = self.try_mtd_start(market, &sync_start, &last_trade).await;
        (sync_start, last_trade) = match mtd_end {
            Some(mut r) => {
                println!("MTD start successful. Adding {} candles.", r.2.len());
                candles.append(&mut r.2);
                // Start now equals the last trade captured in candles made from the trade tables.
                println!("New Sync Start: {}\tLast Trade: {}", r.0, r.1);
                (r.0, Some(r.1))
            }
            // No candles from trade tables were added, start could still be the original 90 day
            // sync window or smaller from the addition of the mcd archived research candles.
            None => (sync_start, last_trade),
        };
        // Try checking system candles table and last candle
        let candles_end = self.try_eld_start(market, &sync_start).await;
        (sync_start, last_trade) = match candles_end {
            Some(mut r) => {
                println!("Eld start successful. Adding {} candles.", r.2.len());
                candles.append(&mut r.2);
                // Start now equals the last trade captured in candles from the production table.
                println!("New Sync Start: {}\tLast Trade: {}", r.0, r.1);
                (r.0, Some(r.1))
            }
            None => (sync_start, last_trade),
        };
        // If at this point there are no candles, calc the start based on exchange logic
        if candles.is_empty() {
            match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    sync_start = self.use_ftx_start(market, &sync_start).await;
                    (sync_start, last_trade, candles)
                }
                ExchangeName::Gdax => {
                    (sync_start, last_trade) = self.use_gdax_start(market, &sync_start).await;
                    (sync_start, last_trade, candles)
                }
            }
        } else {
            (sync_start, last_trade, candles)
        }
    }

    // Try to determine the start dt for sync by checking the mcd record for the market. The mcd
    // will list the valid candle end dt if it exists. If this dt is greater than the given start dt
    // return the mcd last dt and the candles from start to last resampled in the market time frame
    // as these candles have been validated and archived.
    async fn try_mcd_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, PrIdTi, Vec<ProductionCandle>)> {
        println!("Trying MCD start.");
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        // Check for mcd
        let mcd = MarketCandleDetail::select(&self.pools[&Database::ElDorado], market).await;
        match mcd {
            Ok(m) => {
                if m.last_trade_ts > *sync_start {
                    // Validate the data from the mcd record
                    Some(self.use_mcd_start(&db, market, &m, sync_start).await)
                } else {
                    // The archive market candles end before the request start and are of no use
                    println!("MCD ends before sync start");
                    println!("MCD end: {}\tSync start: {}", m.last_trade_ts, sync_start);
                    None
                }
            }
            // No mcd exists, so no archive candles to use
            Err(sqlx::Error::RowNotFound) => {
                println!("No MCD exists.");
                None
            }
            // Other SQLX Error - TODO! - Add Error Handling
            Err(e) => panic!("SQLX Error: {:?}", e),
        }
    }

    // Get all candles from start to mcd last, resample to market time frame and convert to prod
    async fn use_mcd_start(
        &self,
        db: &Database,
        market: &MarketDetail,
        mcd: &MarketCandleDetail,
        sync_start: &DateTime<Utc>,
    ) -> (DateTime<Utc>, PrIdTi, Vec<ProductionCandle>) {
        println!("Using MCD start.");
        // Select candles
        let archive_candles = ResearchCandle::select_dr(
            &self.pools[db],
            market,
            &mcd.time_frame,
            sync_start,
            &(mcd.last_candle + mcd.time_frame.as_dur()),
        )
        .await
        .expect("Failed to select research candles by dr.");
        // Validate & Resample candles if the market time frame is longer than research base of S15
        let candles: Vec<ProductionCandle> = if archive_candles.is_empty() {
            // Could not select any candles, panic as there is a data integrity issue
            panic!(
                "Excpected mcd candles for sync. Select dr of {:?} to {:?} returned None.",
                sync_start,
                mcd.last_candle + mcd.time_frame.as_dur()
            );
        } else if &archive_candles.first().unwrap().datetime > sync_start.max(&mcd.first_candle) {
            // Research candles are not correctly stored and cannot be used. Panic and fix the data
            // integrity issue. Research candles returned should be at min the sync start or the
            // first mcd candle.
            // Err 1: Sync start on 8/1/22 and MCD First Candle for market is 10/1/22. If the first
            //        research candle returned is from 12/1/22 then we are missing the research
            //        candles from 10/1 through 12/1 and the metrics will be flawed.
            // Err 2: Sync start on 11/1/22 and MCD First Candle for market is 1/1/22. If the first
            //        research candle returned is from 12/1/11 then we are missing the research
            //        candles from 11/1 through 12/1 and the metrics will be flawed.
            panic!(
                "Expected research candles from {} through {}. First research candle is {}",
                sync_start.max(&mcd.first_candle),
                &(mcd.last_candle + mcd.time_frame.as_dur()),
                archive_candles.first().unwrap().datetime
            );
        } else if market.candle_timeframe.unwrap() == mcd.time_frame {
            // Candles are already in time frame. Convert the candles to prod candles.
            archive_candles
                .iter()
                .map(|c| c.as_production_candle())
                .collect()
        } else {
            // Resample candles to market time frame and convert to prod candles.
            println!(
                "Resampling MCD candles from {} to {}.",
                mcd.time_frame,
                market.candle_timeframe.unwrap()
            );
            let resampled_candles =
                self.resample_research_candles(&archive_candles, &market.candle_timeframe.unwrap());
            resampled_candles
                .iter()
                .map(|c| c.as_production_candle())
                .collect()
        };
        // Return the prepped candles ready for sync and new sync start and pridti
        let last = candles.last().unwrap();
        (
            last.datetime + market.candle_timeframe.unwrap().as_dur(),
            last.close_as_pridti(),
            candles,
        )
    }

    // Try to determine start dt for sync by checking the mtd record for the market. The mtd will
    // list the last trade date that was validated with a trade file archived. If this date is
    // greater than the give start date, load all the trades for the days starting at the start dt
    // and ending at the last valid trade date from the mtd as these trades have been validated.
    // Create production candles from the trades and return with the updated start
    async fn try_mtd_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
        last_trade: &Option<PrIdTi>,
    ) -> Option<(DateTime<Utc>, PrIdTi, Vec<ProductionCandle>)> {
        println!("Trying MTD start.");
        let mtd = MarketTradeDetail::select(&self.pools[&Database::ElDorado], market).await;
        match mtd {
            Ok(m) => {
                match m.next_trade_day {
                    // There is a next trade day, check if relevant to start date. ie if sync start
                    // is 08/12/2020 00:00 (it is a even date as it is only either a MCD or 92 day)
                    Some(ntd) => {
                        if ntd > *sync_start {
                            // Validated trades available, convert to candles and use them
                            self.use_mtd_start(market, &m, sync_start, *last_trade)
                                .await
                        } else {
                            // The last validated trade date is less than current start, not useful
                            println!("MTD ends before sync start.");
                            println!("MTD next trade day: {}\tSync start: {}", ntd, sync_start);
                            None
                        }
                    }
                    // No next trade day, no archive trades to use
                    None => None,
                }
            }
            // No mtd exists, so no archive trades to use
            Err(sqlx::Error::RowNotFound) => {
                println!("No MTD exists.");
                None
            }
            // Other SQLX Error - TODO! - Add Error Handling
            Err(e) => panic!("SQLX Error: {:?}", e),
        }
    }

    // Get all trades from start to mtd next - 1, convert to production candles for market timeframe
    async fn use_mtd_start(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        sync_start: &DateTime<Utc>,
        mut last_trade: Option<PrIdTi>,
    ) -> Option<(DateTime<Utc>, PrIdTi, Vec<ProductionCandle>)> {
        println!("Using MTD start.");
        // Create date range for days to load and make candles from
        let dr = self.create_date_range(sync_start, &mtd.next_trade_day.unwrap(), &TimeFrame::D01);
        let mut candles = Vec::new();
        println!(
            "MTD Date Range: {} to {}",
            dr.first().unwrap(),
            dr.last().unwrap()
        );
        // For each day, check for table
        for d in dr.iter() {
            // Does the trade table exist for the day?
            println!("Checking trade table exists for {}", d);
            if self.trade_table_exists(market, d).await {
                // Select trades and make candles
                let new_candles = self
                    .make_production_candles_for_dt_from_table(market, d, last_trade)
                    .await;
                match new_candles {
                    Some(mut c) => {
                        last_trade = Some(c.last().unwrap().close_as_pridti());
                        candles.append(&mut c);
                    }
                    None => break,
                }
            } else {
                println!("Trade table does not exist. Exiting MTD start.");
                break;
            };
        }
        if candles.is_empty() {
            None
        } else {
            // Return the prepped candles ready for sync and new sync start and pridti
            let last = candles.last().unwrap();
            Some((
                last.datetime + market.candle_timeframe.unwrap().as_dur(),
                last.close_as_pridti(),
                candles,
            ))
        }
    }

    // Try to determine the dt for sync by checking for candles that were created from running the
    // instance. The start will have already checked for validated candles and made candles from any
    // validated trades. At this point check for the unvalidated candles in the system from the last
    // time it was run.
    async fn try_eld_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, PrIdTi, Vec<ProductionCandle>)> {
        println!("Trying ElD start.");
        // Check if production candle table exists for market
        if self
            .candle_table_exists(
                market,
                &market.candle_timeframe.unwrap(),
                &CandleType::Production,
            )
            .await
        {
            self.use_eld_start(market, sync_start).await
        } else {
            // No production candles, create the candle table and return None
            println!("ElD production candles table does not exist. Creating table.");
            let db = match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
                ExchangeName::Gdax => Database::Gdax,
            };
            ProductionCandle::create_table(
                &self.pools[&db],
                market,
                &market.candle_timeframe.unwrap(),
            )
            .await
            .expect("Failed to create candle table.");
            None
        }
    }

    // Select all the production candles greater than the start date and return with updated PrIdTi
    async fn use_eld_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
    ) -> Option<(DateTime<Utc>, PrIdTi, Vec<ProductionCandle>)> {
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        let candles = ProductionCandle::select_gte_dt(&self.pools[&db], market, sync_start)
            .await
            .expect("Failed to select candles.");
        if candles.is_empty() {
            println!("No ElD production candles greater than current start.");
            None
        } else {
            let last = candles.last().unwrap();
            Some((
                last.datetime + market.candle_timeframe.unwrap().as_dur(),
                last.close_as_pridti(),
                candles,
            ))
        }
    }

    // Calculate the time and id for the ftx trade that corresponds to 92 days prior to today
    // as there are no research candles, validated trades or production candles to start from.
    // FTX API uses date time as field so the first day that has trades is the first day to use
    async fn use_ftx_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
    ) -> DateTime<Utc> {
        let end_dt = Utc::now().duration_trunc(Duration::days(1)).unwrap();
        let mut start_dt = *sync_start;
        while start_dt < end_dt {
            // Only request 1 per second to avoid 429 errors
            println!("Checking Ftx for trades on {}", start_dt);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let trades = self.clients[&market.exchange_name]
                .get_ftx_trades(market.market_name.as_str(), Some(1), None, Some(start_dt))
                .await
                .expect("Failed to get ftx trades.");
            if trades.is_empty() {
                start_dt = start_dt + Duration::days(1)
            } else {
                println!("Trades exist. Use start of day.");
                break;
            };
        }
        start_dt
    }

    // GDAX trade API uses trade id for pagination instead of date. In order to determine the trade
    // id that started 92 days ago this function uses a binary search algorithm to find a trade on
    // the given day 92 days ago, then refines it further to find the last trade of the previous
    // say to give the exact trade id to start syncing from
    async fn use_gdax_start(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
    ) -> (DateTime<Utc>, Option<PrIdTi>) {
        // Get the latest exchange trade to get the last trade id for the market and timestamp
        println!("Getting Gdax start from exchange.");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let mut trade = self.clients[&ExchangeName::Gdax]
            .get_gdax_trades(&market.market_name, Some(1), None, None)
            .await
            .expect("Failed to get gdax trade.")
            .pop()
            .unwrap();
        // Continue to check for trades until the trade timestamp day matches the start day or until
        // the trade id is less than 1000, in which case use that day
        let mut low = 0_i64;
        let mut high = trade.trade_id;
        while trade.time.duration_trunc(Duration::days(1)).unwrap() != *sync_start
            && trade.trade_id > 1000
        {
            let mid = (high + low) / 2;
            println!("Low: {}\tMid: {}\tHigh: {}", low, mid, high);
            println!("Getting next trade before mid {}", mid);
            // Get trade for mid id
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            trade = self.clients[&ExchangeName::Gdax]
                .get_gdax_trades(&market.market_name, Some(1), None, Some(mid as i32))
                .await
                .expect("Failed to get gdax trade.")
                .pop()
                .unwrap();
            println!("Mid trade id and ts: {}\t{:?}", trade.trade_id, trade.time);
            if trade.time < *sync_start {
                // Too far in the past, set low = trade id, high remains the same
                low = trade.trade_id;
            } else {
                // Not far enough in past, set high = trade id, low remains the same
                high = trade.trade_id
            };
        }
        // Take the last trade from the binary search and then query sequentially to get the last
        // trade of the day or last trade of the previous day depending on total number of trades
        let last_trade = if trade.trade_id > 1000 {
            self.get_last_gdax_trade_for_prev_day(market, &trade)
                .await
                .as_pridti()
        } else {
            // Get end of current day as there a < 1000 trades and it may not make it to previous
            // day to set start at 92 days
            self.get_last_gdax_trade_for_day(market, &trade)
                .await
                .as_pridti()
        };
        (
            last_trade
                .dt
                .duration_trunc(market.candle_timeframe.unwrap().as_dur())
                .unwrap()
                + market.candle_timeframe.unwrap().as_dur(),
            Some(last_trade),
        )
    }

    // The end of the sync is the first trade from the websocket stream. Check the table for the
    // trade and return the id and timestamp. If there is no trade, sleep for interval and check
    // again until a trade is found.
    async fn calculate_sync_end(&self, market: &MarketDetail) -> PrIdTi {
        println!(
            "Fetching first {} trade from ws after {:?}.",
            market.market_name, self.start_dt
        );
        loop {
            match self.select_first_ws_timeid(market).await {
                // There is a first trade, return it in TimeId format
                Some(t) => break t,
                // There are no trades in the db, sleep and check again in 5 seconds
                None => {
                    println!("Awaiting first ws trade for {}.", market.market_name);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }

    // Get trades from start to end and write to trades table for market / day. Aggregrate trades
    // into production candles to be returned
    async fn fill_trades_and_make_candles(
        &self,
        market: &MarketDetail,
        sync_start: DateTime<Utc>,
        mut last_trade: Option<PrIdTi>,
        first_trade: PrIdTi,
    ) -> Vec<ProductionCandle> {
        let mut fill_candles: Vec<ProductionCandle> = Vec::new();
        // Create date range for days to fill
        let dr_start = sync_start.duration_trunc(Duration::days(1)).unwrap();
        let dr = self.create_date_range(
            &dr_start,
            &(first_trade.dt.duration_trunc(Duration::days(1)).unwrap() + Duration::days(1)),
            &TimeFrame::D01,
        );
        // Fill trades for each day
        for d in dr.iter() {
            println!("Filling trades and making candles for date: {}", d);
            let new_candles = self
                .fill_trades_and_make_candles_for_dt(
                    market,
                    &sync_start,
                    &last_trade,
                    &first_trade,
                    d,
                )
                .await;
            last_trade = match new_candles {
                Some(mut ncs) => {
                    let lt = ncs.last().unwrap().close_as_pridti();
                    fill_candles.append(&mut ncs);
                    Some(lt)
                }
                None => last_trade,
            };
        }
        fill_candles
    }

    async fn fill_trades_and_make_candles_for_dt(
        &self,
        market: &MarketDetail,
        sync_start: &DateTime<Utc>,
        last_trade: &Option<PrIdTi>,
        first_trade: &PrIdTi,
        dt: &DateTime<Utc>,
    ) -> Option<Vec<ProductionCandle>> {
        // Create trade table
        self.create_trade_table(market, *dt).await;
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                // The interval end is the min of the start of the next day or the end PrIdTi given
                // The use case where it is the end PrIdTi is when the sync is filling trades for
                // the final day to sync to the first ws streamed trade. For example: if the first
                // streamed trades is at 12:01:34 for a given day, the trades should be filled to
                // that time, otherwise to 00:00:00 the following day.
                let interval_start = sync_start.max(dt);
                let interval_end = first_trade.dt.min(*dt + Duration::days(1));
                // Fill trades for day to interval end
                let trades: Vec<FtxTrade> = self
                    .get_ftx_trades_for_interval(market, dt, &interval_end)
                    .await;
                // Make candles for day and insert to db
                let candles = self.make_production_candles_for_dt_from_vec(
                    market,
                    dt,
                    last_trade,
                    interval_start,
                    &interval_end,
                    &trades,
                );
                match candles {
                    Some(cs) => {
                        for candle in cs.iter() {
                            candle
                                .insert(
                                    &self.pools[&Database::Ftx],
                                    market,
                                    &market.candle_timeframe.unwrap(),
                                )
                                .await
                                .expect("Failed to insert candle.");
                        }
                        Some(cs)
                    }
                    None => None,
                }
            }
            ExchangeName::Gdax => {
                // Fill trades for day
                let interval_start = sync_start.max(dt);
                let interval_end = first_trade.dt.min(*dt + Duration::days(1));
                let trades: Vec<GdaxTrade> = self
                    .get_gdax_trades_for_interval_forward(
                        market,
                        interval_start,
                        &interval_end,
                        last_trade.unwrap().id as i32,
                        &last_trade.unwrap().dt,
                    )
                    .await;
                // Write trades to table
                println!("Writing {} trades to table.", trades.len());
                for trade in trades.iter() {
                    trade
                        .insert(&self.pools[&Database::Gdax], market)
                        .await
                        .expect("Failed to insert trade.");
                }
                // Make candles for day
                let candles = self.make_production_candles_for_dt_from_vec(
                    market,
                    dt,
                    last_trade,
                    interval_start,
                    &interval_end,
                    &trades,
                );
                match candles {
                    Some(cs) => {
                        for candle in cs.iter() {
                            candle
                                .insert(
                                    &self.pools[&Database::Gdax],
                                    market,
                                    &market.candle_timeframe.unwrap(),
                                )
                                .await
                                .expect("Failed to insert candle.");
                        }
                        Some(cs)
                    }
                    None => None,
                }
            }
        }
    }

    // Take the candles created in the fill and sync functions and create the current heartbeat
    // for the market
    async fn create_heartbeat(
        &self,
        market: &MarketDetail,
        candles: Vec<ProductionCandle>,
    ) -> Heartbeat {
        // Create hashmap for timeframes and their candles
        let last = candles.last().expect("Expected at least one candle.");
        let last_ts = last.datetime;
        let last_pridti = last.close_as_pridti();
        let mut candles_map = HashMap::new();
        let base_tf = market
            .candle_timeframe
            .expect("Expected timeframe for market.");
        candles_map.insert(base_tf, candles);
        for tf in TimeFrame::time_frames().iter().skip(1) {
            // Filter candles from flor of new timeframe
            let filtered_candles: Vec<_> = candles_map[&tf.prev()]
                .iter()
                .filter(|c| c.datetime < last_ts.duration_trunc(tf.as_dur()).unwrap())
                .cloned()
                .collect();
            let resampled_candles = self.resample_production_candles(&filtered_candles, tf);
            candles_map.insert(*tf, resampled_candles);
        }
        Heartbeat {
            ts: last_ts,
            last: last_pridti,
            candles: candles_map,
            metrics: None,
        }
    }

    // Fill trades to the beginning of the market open on exchange from the first candle/trade
    // day in el-dorado. Process will create a market trade detail record for each market working
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
    //          7) Validate 01d can - if validated - archive trades to csv file and drop table
    //          7) Repeat for 13-Nov-2022 until you reach beginning of market
    //          8) At beginning set start_ts, set prev status to Completed
    //          9) Set next day to prev day, set next status to validate
    //          10) Validated next day, if validated - archive and move date forward
    //          11) If not validated, create manual validation event, send sms, exit
    pub async fn fill(&self, market: &Option<MarketDetail>, automate: Option<bool>) {
        // Eval params to determine to run and whether to automate or get manual inputs
        let markets = match market {
            Some(m) => {
                // Validate that market given is eligible for fill
                if self.validate_market_eligible_for_fill(m) {
                    Some([m.clone()].to_vec())
                } else {
                    None
                }
            }
            None => self.select_markets_eligible_for_fill().await,
        };
        // Check if there are markets to fill
        match markets {
            Some(m) => {
                let automate = automate.unwrap_or(true);
                // For each market: fill back than forward until either fully synced or manual
                // input to stop
                for market in m.iter() {
                    // Fill the market backward than forward until either the current date or
                    // there is a manual rejection or if automated - a validation fails
                    self.fill_market(market, &automate).await;
                }
            }
            None => (),
        }
    }

    fn validate_market_eligible_for_fill(&self, market: &MarketDetail) -> bool {
        // Market detail must contain a last candle value - meaning that it has been initialized
        // and run. The market must also be active.
        if market.market_data_status != MarketStatus::Active || market.last_candle.is_none() {
            println!(
                "{} not eligible for fill. Status: {:?}\tLast Candle: {:?}",
                market.market_name, market.market_data_status, market.last_candle
            );
            false
        } else {
            true
        }
    }

    async fn fill_market(&self, market: &MarketDetail, automate: &bool) {
        // Route based on if it is a forward or backward fill event and the mtd status,
        // continue processing events until there are no more events to process or validation fail
        let mut continue_fill = true;
        while continue_fill {
            // Get the mtd
            let mtd = self.select_market_trade_detail(market).await;
            // Determine fill branch and process next fill step
            continue_fill = match mtd.previous_status {
                MarketDataStatus::Completed => {
                    self.route_fill_forward(market, &mtd, automate).await
                }
                MarketDataStatus::Get => {
                    self.fill_backward_get(market, &mtd).await;
                    continue_fill
                }
                MarketDataStatus::Validate => {
                    self.fill_backward_validate(market, &mtd).await;
                    continue_fill
                }
                MarketDataStatus::Archive => {
                    self.fill_backward_archive(market, &mtd).await;
                    continue_fill
                }
            };
        }
    }

    async fn route_fill_forward(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        automate: &bool,
    ) -> bool {
        // Backfill is completed, this function will look at the mtd record to determine how to
        // route the next forward fill event
        match mtd.next_status {
            Some(MarketDataStatus::Completed) => {
                // The forward fill has begun - determine next action
                match mtd.next_trade_day {
                    Some(d) => {
                        // Check that the date is valid - ie it is less than the market last candle
                        if d < market
                            .last_candle
                            .expect("Expected last candle with earlier validation.")
                            .duration_trunc(Duration::days(1))
                            .expect("Expected duration trunc for day.")
                        {
                            self.fill_forward_validate(market, mtd, d).await;
                            true
                        } else {
                            println!("Market fill complete. Market Last Candle: {:?}\tNext Trade Day: {}",market.last_candle, d);
                            false
                        }
                    }
                    None => false, // There should be a next trade day if there is a next status
                }
            }
            Some(MarketDataStatus::Get) => self.fill_forward_get(market, mtd, automate).await,
            Some(MarketDataStatus::Archive) => {
                self.fill_forward_archive(market, mtd).await;
                true
            }
            Some(MarketDataStatus::Validate) => {
                self.fill_forward_validate(
                    market,
                    mtd,
                    mtd.next_trade_day.expect("Expected next trade day."),
                )
                .await;
                true
            }
            None => {
                // Next status is null in the db. This occurs when the the backfill is first
                // completed. The first foward fill is to validate the first trade day
                self.fill_forward_validate(market, mtd, mtd.previous_trade_day + Duration::days(1))
                    .await;
                true
            }
        }
    }

    async fn fill_forward_get(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        automate: &bool,
    ) -> bool {
        // Get the trades for the day and write to a vec. Then evaluate those trades with the
        // exchange candle, the trade table for the day, and the _processed _rest tables if they
        // exist. In addition do the trade id numerical validations if needed by exchange.
        // If automate is false - get user input and wait for input. If automate - stop fill
        // process on validation failure. Finally, process the trades if the evaluation is true
        // by saving the trades, cleaning the tables, writing the file and updating the next status
        // to validate so that it can move the file to the archive location and move forward in the
        // fill process
        let dt = mtd.next_trade_day.expect("Expected next trade day.");
        println!(
            "Forward Fill {} for {}. Get: Re-download trades.",
            market.market_name, dt
        );
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                todo!("Refactor ftx get.")
            }
            ExchangeName::Gdax => {
                // Get the trades for the day
                let trades = self
                    .get_gdax_trades_for_interval_forward(
                        market,
                        &dt,
                        &(dt + Duration::days(1)),
                        mtd.last_trade_id
                            .parse::<i32>()
                            .expect("Failed to parse trade id."),
                        &mtd.last_trade_ts,
                    )
                    .await;
                // Eval auto or manual to existing trades (day table or _rest/_processed)
                if self
                    .evaluate_fill_forward_get_gdax(market, &dt, automate, &trades)
                    .await
                {
                    // If acccepted - write trades to file and update mtd
                    self.process_fill_forward_get_gdax(market, mtd, &dt, &trades)
                        .await;
                    true
                } else {
                    false
                }
            }
        }
    }

    // Evaluate the trades retrieved during the Forward Fill Get event.
    // 1) Check for normal validation of the trades - trade id has no gaps and next/prev trade
    // 2) If that fails, then display for the user the volume and trade count for:
    //   a) The trades received from forward fill
    //   b) The trades stored in the daily trade table (may not exists from legacy process)
    //   c) The trades stored in the _rest/_processed table (from legacy process)
    // Volume needs exact match - if automate is True and not matched - fail. if automate is false
    // get the input from the user to accept the new trades or reject.
    async fn evaluate_fill_forward_get_gdax(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        automate: &bool,
        trades: &[GdaxTrade],
    ) -> bool {
        // Run GDAX validation on trades
        let validated = self.validate_gdax_trades_for_interval(market, trades).await;
        if validated {
            // Return true - new trade are validated
            println!("GDAX validation sucessful.");
            true
        } else {
            // Calc volume for new trades
            let forward_fill_volume = trades.iter().fold(dec!(0), |v, t| v + t.size());
            println!(
                "Forward Volume & Count:\t{}\t\t{}",
                forward_fill_volume,
                trades.len()
            );
            // Check if there is a trade table for the date and load those trades
            if self.trade_table_exists(market, dt).await {
                let trade_date_trades =
                    GdaxTrade::select_all(&self.pools[&Database::Gdax], market, dt)
                        .await
                        .expect("Failed to select trades.");
                let trade_date_volume = trade_date_trades.iter().fold(dec!(0), |v, t| v + t.size());
                println!(
                    "Daily Volume & Count:\t{}\t\t{}",
                    trade_date_volume,
                    trade_date_trades.len()
                );
            } else {
                // Trade table does not exist for the day
                println!("Daily Trade Table Does Not Exist.");
            }
            // Get the exchange candle from exchange api to compare volume
            let validated = match self.get_gdax_daily_candle(market, dt).await {
                Some(ec) => {
                    let delta = forward_fill_volume - ec.volume;
                    println!("Gdax Exchange Volume: {}", ec.volume);
                    println!(
                        "Delta: {}\t (%) {:?}%",
                        delta,
                        delta / ec.volume * dec!(100)
                    );
                    forward_fill_volume == ec.volume
                }
                None => {
                    println!(
                        "No Gdax exchange candle for {} on {}",
                        market.market_name, dt
                    );
                    false
                }
            };
            // Check for auto approve it automate = true or get manual input to validate
            if *automate && validated {
                println!("Volume matches, approve qc.");
                true
            } else if *automate && !validated {
                println!("Volume does not match, fail qc and move to next market.");
                false
            } else {
                let input: Option<String> = self.get_input_with_timer(15, "Volume does not match, what do you want to do? Enter 'Y' to use qc trades:").await;
                match input {
                    Some(s) => match s.to_lowercase().as_str() {
                        "y" | "yes" => {
                            println!("Accepting QC trades.");
                            true
                        }
                        _ => {
                            println!("Failing QC and moving to next market.");
                            false
                        }
                    },
                    None => {
                        println!(
                            "No response provided in time. Failing QC and moving to next market."
                        );
                        false
                    }
                }
            }
        }
    }

    // Write the trades to file and update the mtd forward status to Validate so that the next round
    // of fill picks up the file as validated and moves forward.
    async fn process_fill_forward_get_gdax(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        dt: &DateTime<Utc>,
        trades: &[GdaxTrade],
    ) {
        let fp = self.prep_trade_archive_path_initial(market, dt);
        self.write_trades_to_archive_path_gdax(trades, &fp);
        mtd.update_next_day_next_status(
            &self.pools[&Database::ElDorado],
            dt,
            &MarketDataStatus::Validate,
        )
        .await
        .expect("Failed to update db status.");
    }

    async fn fill_forward_validate(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        dt: DateTime<Utc>,
    ) {
        // Locate a trades file if there is one. They can be located in main archive - or in two
        // other directories. If there is no trade file - update to get
        println!(
            "Forward Fill {} for {}. Validate: Check file or trade table.",
            market.market_name, dt
        );
        let f = format!("{}_{}.csv", market.as_strip(), dt.format("%F"));
        let archive_path = format!(
            "{}/csv/{}",
            &self.storage_path,
            &market.exchange_name.as_str()
        );
        let f_path = self.prep_trade_archive_path_initial(market, &dt);
        // Move files from bad locations if they are there - can remove after cleanup
        self.fill_forward_validate_file_locations(&archive_path, &f, &f_path);
        // Check if the file exists - which means the day has been validated
        if f_path.exists() {
            // File exists - update mtd to archive and the date to the given data param - it is not
            // guaranteed to be there prior
            println!(
                "Trade file for {} on {} exists in the expected location. Archive it.",
                market.market_name, dt
            );
            mtd.update_next_day_next_status(
                &self.pools[&Database::ElDorado],
                &dt,
                &MarketDataStatus::Archive,
            )
            .await
            .expect("Expected db update.");
        } else if self.trade_table_exists(market, &dt).await {
            // Check the trade table for the day if the trades are validated. They would be checked
            // already in the backfill process if the date is prior to the date when the market
            // started OR they will not have been checked if the date was created during normal
            // running of the price feed.
            println!("Trade file does not exist. Checking trade table.");
            match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => todo!(),
                ExchangeName::Gdax => {
                    let trades = GdaxTrade::select_all(&self.pools[&Database::Gdax], market, &dt)
                        .await
                        .expect("Failed to select GDAX trades.");
                    if self
                        .validate_gdax_trades_for_interval(market, &trades)
                        .await
                    {
                        // Trades from trade table are valid - write to file. Do not update the mtd
                        // As the next iteration will pick up the file and move the status to
                        // archive.
                        self.write_trades_to_archive_path_gdax(&trades, &f_path);
                    } else {
                        // Trades from trade table are not valid - get qc trades
                        println!("Trades from Trade table are not valid. Get trades for qc.");
                        mtd.update_next_day_next_status(
                            &self.pools[&Database::ElDorado],
                            &dt,
                            &MarketDataStatus::Get,
                        )
                        .await
                        .expect("Failed to update db status.");
                    }
                }
            }
        } else {
            // File does not exists - and trade table does not exist with validated trades.
            // update mtd to get and the date to the give date parame - it is
            // not guaranteed to be there prior
            println!(
                "Trade file for {} on {} does not exists. Get trades for day to qc.",
                market.market_name, dt
            );
            mtd.update_next_day_next_status(
                &self.pools[&Database::ElDorado],
                &dt,
                &MarketDataStatus::Get,
            )
            .await
            .expect("Expected db update.");
        }
    }

    async fn fill_forward_archive(&self, market: &MarketDetail, mtd: &MarketTradeDetail) -> bool {
        // Check that the mtd next day is popluated, otherwise return false
        match mtd.next_trade_day {
            Some(d) => {
                println!(
                    "Forwardfill {} for {}. ARCHIVE: Move trade file to archives.",
                    market.market_name, mtd.previous_trade_day
                );
                // Move file to archive
                let path = self.fill_forward_archive_move_file(market, &d);
                // Update mtd to the next day with the last trade price and id
                self.fill_forward_archive_update_mtd(market, mtd, &d, &path)
                    .await;
                // Update archive candles table if within 100 days (only keep 100 days in db, any
                // more stored in flat files for research and backtesting. 100 days needed for
                // current price metric calculations. reduces db size)
                self.fill_forward_archive_process_candles(market, &d, &path)
                    .await;
                // Cleanup tables
                self.fill_forward_archive_cleaup_tables(market, &d).await;
                true
            }
            None => {
                println!("Expected next trade day for Fill Forward Archive.");
                false
            }
        }
    }

    fn fill_forward_archive_move_file(&self, market: &MarketDetail, dt: &DateTime<Utc>) -> PathBuf {
        // Get the current trade file path
        let f_path = self.prep_trade_archive_path_initial(market, dt);
        // Set archive file path
        let a_path = self.prep_trade_archive_path_final(market, dt);
        // Move trade file to validated location
        std::fs::rename(f_path, &a_path).expect("Failed to copy file to trade folder.");
        a_path
    }

    async fn fill_forward_archive_update_mtd(
        &self,
        market: &MarketDetail,
        mtd: &MarketTradeDetail,
        d: &DateTime<Utc>,
        pb: &PathBuf,
    ) {
        // Update mtd status and next day
        mtd.update_next_day_next_status(
            &self.pools[&Database::ElDorado],
            &(*d + Duration::days(1)),
            &MarketDataStatus::Completed,
        )
        .await
        .expect("Failed to update the mtd.");
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                // Read file from new location
                let trades = self.read_ftx_trades_from_file(pb);
                // Get first and last trades
                if !trades.is_empty() {
                    let first_trade = trades.first().unwrap();
                    let last_trade = trades.last().unwrap();
                    // Update first and last trade in the mtd
                    mtd.update_first_and_last_trades(
                        &self.pools[&Database::ElDorado],
                        first_trade,
                        last_trade,
                    )
                    .await
                    .expect("Failed to update first and last trade details.");
                } else {
                    println!("Trade file empty. No need to update first and last trades.")
                }
            }
            ExchangeName::Gdax => {
                // Read file from new location
                let trades = self.read_gdax_trades_from_file(pb);
                // Get first and last trades
                if !trades.is_empty() {
                    let first_trade = trades.first().unwrap();
                    let last_trade = trades.last().unwrap();
                    // Update first and last trade in the mtd
                    mtd.update_first_and_last_trades(
                        &self.pools[&Database::ElDorado],
                        first_trade,
                        last_trade,
                    )
                    .await
                    .expect("Failed to update first and last trade details.");
                } else {
                    println!("Trade file empty. No need to update first and last trades.")
                }
            }
        }
    }

    // Cleanup the research candles table for the market by
    // 1) Deleting an candles older than 100 days from now
    // 2) Creating and Inserting research candles for the market and dt as it has been
    //    validated and archived
    // 3) Updated (or creating if it does not exist) the mcd for market with candle details
    async fn fill_forward_archive_process_candles(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
        pb: &PathBuf,
    ) {
        // Delete candles greater than 100 days in the db
        let cutoff = Utc::now()
            .duration_trunc(Duration::days(1))
            .expect("Failed to trunc date.")
            - Duration::days(100);
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        ResearchCandle::delete_lt_dt(
            &self.pools[&db],
            market,
            &market.candle_timeframe.expect("Expected candle timeframe."),
            &cutoff,
        )
        .await
        .expect("Failed to delete candles.");
        // Get MCD and create if it does not exist
        let mcd = MarketCandleDetail::select(&self.pools[&Database::ElDorado], market).await;
        let candles = match mcd {
            Ok(m) => {
                // Assert that the mcd last is the previous day and create candles for current day
                assert_eq!(m.last_candle + TimeFrame::S15.as_dur(), *dt);
                // Make candles for the dt and update mcd
                let candles = self
                    .make_research_candles_for_dt_from_file(market, &Some(m.clone()), dt, pb)
                    .await;
                match candles {
                    Some(c) => {
                        m.update_last(
                            &self.pools[&Database::ElDorado],
                            c.last().expect("Expected last candle."),
                        )
                        .await
                        .expect("Failed to update mcd.");
                        c
                    }
                    None => panic!("Execpted research candles."),
                }
            }
            Err(sqlx::Error::RowNotFound) => {
                // MCD does not exist for market, create record with todays trades
                // as the start and with the end of day candle as end with last trade
                let candles = self
                    .make_research_candles_for_dt_from_file(market, &None, dt, pb)
                    .await
                    .unwrap();
                let mcd = MarketCandleDetail::new(market, &TimeFrame::S15, &candles);
                mcd.insert(&self.pools[&Database::ElDorado])
                    .await
                    .expect("Failed to insert mcd.");
                candles
            }
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };
        // If the dt is within 100 days - add to research table to use when loading market to run
        if *dt >= cutoff {
            // Insert candles into research candles table
            let db = match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
                ExchangeName::Gdax => Database::Gdax,
            };
            for candle in candles.iter() {
                candle
                    .insert(&self.pools[&db], market, &TimeFrame::S15)
                    .await
                    .expect("Failed to insert candle.");
            }
        }
    }

    async fn fill_forward_archive_cleaup_tables(&self, market: &MarketDetail, dt: &DateTime<Utc>) {
        // Clean up all the trade tables for the market for the day
        // Drop the official trade date table.
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => {
                GdaxTrade::drop_table(&self.pools[&Database::Gdax], market, *dt)
                    .await
                    .expect("Failed to drop table.");
                Database::Gdax
            }
        };
        // Drop the legacy trade and qc tables
        let table_pre = format!(
            "public.trade_{}_{}",
            market.exchange_name.as_str(),
            market.as_strip()
        );
        let tables = ["_rest", "_ws", "_validated", "_archived", "_qc", "_qc_auto"];
        for table in tables.iter() {
            let sql = format!(
                r#"
                DROP TABLE IF EXISTS {}{}
                "#,
                table_pre, table
            );
            sqlx::query(&sql)
                .execute(&self.pools[&db])
                .await
                .expect("Failed to drop table.");
        }
    }

    fn fill_forward_validate_file_locations(&self, path: &str, f: &str, fb: &PathBuf) {
        // Try bad location 1 in a second gdax folder - once cleaned up this can be removed
        let bad_archive_path = format!("{}/csv/gdax2", &self.storage_path);
        let bf_path = Path::new(&bad_archive_path).join(f);
        if bf_path.exists() {
            println!("File in location 3. Moving to correct location.");
            // Create directories in the correct location
            std::fs::create_dir_all(path).expect("Failed to create directories.");
            // File exists but in wrong location - copy to correct location
            std::fs::rename(bf_path, &fb).expect("Failed to copy file from location 3.");
        };
        // Try location 2
        let bad_archive_path = format!("{}/csv/gdax/gdax", &self.storage_path,);
        let bf_path = Path::new(&bad_archive_path).join(f);
        if bf_path.exists() {
            println!("File in location 2. Moving to correct location.");
            // Create directories in the correct location
            std::fs::create_dir_all(&path).expect("Failed to create directories.");
            // File exists but in wrong location - copy to correct location
            std::fs::rename(bf_path, &fb).expect("Failed to copy file from location 2.");
        };
    }

    // Fill backward get - Get the trades for the market on the previous trade date. Create the
    // table if needed and fill with trades from the rest api for the exchange.
    async fn fill_backward_get(&self, market: &MarketDetail, mtd: &MarketTradeDetail) {
        println!(
            "Backfill {} for {}. Get: Create table and get trades.",
            market.market_name, mtd.previous_trade_day
        );
        // Create trade table for market / exchange / date
        self.create_trade_table(market, mtd.previous_trade_day)
            .await;
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                todo!("Migrate FTX get from Inqui.")
            }
            ExchangeName::Gdax => {
                // Fill trades for day
                let trades: Vec<GdaxTrade> = self
                    .get_gdax_trades_for_interval_backward(
                        market,
                        &mtd.previous_trade_day,
                        &(mtd.previous_trade_day + Duration::days(1)),
                        mtd.first_trade_id.parse::<i32>().unwrap(),
                        &mtd.first_trade_ts,
                    )
                    .await;
                // Write trades to table
                println!("Writing {} trades to table.", trades.len());
                for trade in trades.iter() {
                    trade
                        .insert(&self.pools[&Database::Gdax], market)
                        .await
                        .expect("Failed to insert trade.");
                }
            }
        };
        // Update mtd status to validate
        mtd.update_prev_status(
            &self.pools[&Database::ElDorado],
            &MarketDataStatus::Validate,
        )
        .await
        .expect("Failed to update mtd status.");
    }

    // Fill Backward Validate - Validate the trades in the table for the date. If validated - update
    // status to Archive, if not validated - update status to Get and update next date.
    async fn fill_backward_validate(&self, market: &MarketDetail, mtd: &MarketTradeDetail) {
        println!(
            "Backfill {} for {}. VALIDATE: Match trade ids and volume against exchange data.",
            market.market_name, mtd.previous_trade_day
        );
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                todo!("Migrate FTX validate from Inqui.")
            }
            ExchangeName::Gdax => {
                // Get trades from table
                let trades = GdaxTrade::select_all(
                    &self.pools[&Database::Gdax],
                    market,
                    &mtd.previous_trade_day,
                )
                .await
                .expect("Failed to select trades.");
                // Perform GDAX specific validations
                let validated = self
                    .validate_gdax_trades_for_interval(market, &trades)
                    .await;
                // Process validation result
                self.process_fill_backward_validation_gdax(mtd, validated, &trades)
                    .await;
            }
        }
    }

    // Fill Backward Archive - Archive the trades in the trade table for the day and update the mtd
    async fn fill_backward_archive(&self, market: &MarketDetail, mtd: &MarketTradeDetail) {
        println!(
            "Backfill {} for {}. ARCHIVE: Save trades to csv file.",
            market.market_name, mtd.previous_trade_day
        );
        let fp = self.prep_trade_archive_path_initial(market, &mtd.previous_trade_day);
        // Load trades from table and write to file but do not drop table. This will increase db
        // storage but will simplify process by only dropping tables and cleaning up during the
        // forward fill as each day is validated and the research candles are created
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                todo!("Migrate FTX archive from Inqui.")
            }
            ExchangeName::Gdax => {
                let trades = GdaxTrade::select_all(
                    &self.pools[&Database::Gdax],
                    market,
                    &mtd.previous_trade_day,
                )
                .await
                .expect("Failed to select trades.");
                self.write_trades_to_archive_path_gdax(&trades, &fp);
                self.process_fill_backward_archive_gdax(mtd, &trades).await;
            }
        }
    }

    async fn process_fill_backward_archive_gdax(
        &self,
        mtd: &MarketTradeDetail,
        trades: &[GdaxTrade],
    ) {
        // Update mtd
        if !trades.is_empty() {
            // Safely unwrap the first trade since it is not empty
            let first_trade = trades.first().expect("Expected first trade.");
            // Check if the first trade is trade_id = 1 - then mark backfill compelte or move to
            // previous day and get
            if first_trade.trade_id == 1 {
                // First trade, make backfill as completed
                mtd.update_prev_day_prev_status(
                    &self.pools[&Database::ElDorado],
                    &(mtd.previous_trade_day - Duration::days(1)),
                    &MarketDataStatus::Completed,
                )
                .await
                .expect("Failed to update mtd status.");
            } else {
                // Move to next day
                mtd.update_prev_day_prev_status(
                    &self.pools[&Database::ElDorado],
                    &(mtd.previous_trade_day - Duration::days(1)),
                    &MarketDataStatus::Get,
                )
                .await
                .expect("Failed to update mtd status.");
            };
            // Update the first ftrade in the mtd
            mtd.update_first_trade(
                &self.pools[&Database::ElDorado],
                &first_trade.time,
                &first_trade.trade_id.to_string(),
            )
            .await
            .expect("Failed to update mtd.");
        } else {
            // No trades for the day but update the previous day and status
            mtd.update_prev_day_prev_status(
                &self.pools[&Database::ElDorado],
                &(mtd.previous_trade_day - Duration::days(1)),
                &MarketDataStatus::Get,
            )
            .await
            .expect("Failed to update mtd status.");
        }
    }

    async fn process_fill_backward_validation_gdax(
        &self,
        mtd: &MarketTradeDetail,
        validated: bool,
        trades: &[GdaxTrade],
    ) {
        // Based on validation - update mtd to archive or to validate the next day
        if validated {
            mtd.update_prev_status(&self.pools[&Database::ElDorado], &MarketDataStatus::Archive)
                .await
                .expect("Failed to update mtd status.");
        } else if !trades.is_empty() {
            // Safely unwrap the first trade since it is not empty
            let first_trade = trades.first().expect("Expected first trade.");
            // Check if the first trade is trade_id 1 - then either move to next day or mark
            // the backfill as complete so the forward fill can start
            if first_trade.trade_id == 1 {
                // First trade, make backfill as completed
                mtd.update_prev_day_prev_status(
                    &self.pools[&Database::ElDorado],
                    &(mtd.previous_trade_day - Duration::days(1)),
                    &MarketDataStatus::Completed,
                )
                .await
                .expect("Failed to update mtd status.");
            } else {
                // Day not validated, move on to getting the next day
                mtd.update_prev_day_prev_status(
                    &self.pools[&Database::ElDorado],
                    &(mtd.previous_trade_day - Duration::days(1)),
                    &MarketDataStatus::Get,
                )
                .await
                .expect("Failed to update mtd status.");
            };
            // Update the first trade in the mtd
            mtd.update_first_trade(
                &self.pools[&Database::ElDorado],
                &first_trade.time,
                &first_trade.trade_id.to_string(),
            )
            .await
            .expect("Failed to update mtd.");
        } else {
            // Failed validation and there are no trades for the day - move to next day
            mtd.update_prev_day_prev_status(
                &self.pools[&Database::ElDorado],
                &(mtd.previous_trade_day - Duration::days(1)),
                &MarketDataStatus::Get,
            )
            .await
            .expect("Failed to update mtd status.");
        }
    }

    async fn validate_gdax_trades_for_interval(
        &self,
        market: &MarketDetail,
        trades: &[GdaxTrade],
    ) -> bool {
        // Check if there are trades, if there are no trades than mark as not validated
        if !trades.is_empty() {
            // Unwrap first and last trades given that trades is not empty
            let first = trades.first().expect("Expected first trade.");
            let last = trades.last().expect("Expected last trade.");
            // 3 validations
            // Validation 1: Trade count matches number of trades
            let validation_1 = last.trade_id - first.trade_id + 1 == trades.len() as i64;
            println!(
                "Trade Count Validation: {}. {} - {} + 1 = {}.",
                validation_1,
                last.trade_id,
                first.trade_id,
                trades.len()
            );
            // Validation 2: Next trade from last is outside interval end
            let validation_2 = self.validate_next_gdax_trade(market, last).await;
            println!("Next Trade Validation: {}", validation_2);
            // Validation 3: Previous trade from first is outside interval start
            let validation_3 = self.validate_previous_gdax_trade(market, first).await;
            println!("Previous Trade Validation: {}", validation_3);
            validation_1 && validation_2 && validation_3
        } else {
            false
        }
    }

    async fn validate_next_gdax_trade(&self, market: &MarketDetail, trade: &GdaxTrade) -> bool {
        // Get the next trade for the trade id given
        let next_trade = self.get_gdax_next_trade(market, trade).await;
        match next_trade {
            Some(t) => {
                // Check that the next trade is greater than the date of the last trade
                println!("Last Trade: {}\tNext Trade {}", trade.time, t.time);
                t.time.duration_trunc(Duration::days(1)).unwrap()
                    > trade.time.duration_trunc(Duration::days(1)).unwrap()
            }
            None => {
                println!("Next trade validation failed. No next trade.");
                false
            }
        }
    }

    async fn validate_previous_gdax_trade(&self, market: &MarketDetail, trade: &GdaxTrade) -> bool {
        // Check if trade id == 1 as there would be no next trade and the validation is correct
        if trade.trade_id == 1 {
            true
        } else {
            // Get the previous trade for the trade id given
            let previous_trade = self.get_gdax_previous_trade(market, trade).await;
            match previous_trade {
                Some(t) => {
                    // Check taht the previous trade is less than the date of the first trade
                    println!("First Trade: {}\tPrevious Trade {}", trade.time, t.time);
                    t.time.duration_trunc(Duration::days(1)).unwrap()
                        < trade.time.duration_trunc(Duration::days(1)).unwrap()
                }
                None => {
                    println!("Previous trade validation failed. No previous trade.");
                    false
                }
            }
        }
    }

    // Takes the trade market and trade day and creates any directories needed creates the path
    // to where the file will be archived. This is the csv directory where the file is first written
    // as files are processed forward - the files are moved to the final location in a different
    // function
    fn prep_trade_archive_path_initial(
        &self,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> PathBuf {
        println!(
            "Creating trade archive path for {} on {}",
            market.market_name, dt
        );
        // Check directory for csv file is created
        let path = format!(
            "{}/csv/{}",
            &self.storage_path,
            &market.exchange_name.as_str()
        );
        std::fs::create_dir_all(&path).expect("Failed to create directories.");
        let f = format!("{}-{}.csv", market.as_strip(), dt.format("%F"));
        std::path::Path::new(&path).join(f)
    }

    fn prep_trade_archive_path_final(&self, market: &MarketDetail, dt: &DateTime<Utc>) -> PathBuf {
        println!(
            "Creating trade archive path final for {} on {}",
            market.market_name, dt
        );
        let path = format!(
            "{}/trades/{}/{}/{}/{}",
            &self.storage_path,
            &market.exchange_name.as_str(),
            &market.as_strip(),
            dt.format("%Y"),
            dt.format("%m"),
        );
        std::fs::create_dir_all(&path).expect("Failed to create directories.");
        let f = format!("{}-{}.csv", market.as_strip(), dt.format("%F"));
        std::path::Path::new(&path).join(f)
    }

    // Takes Gdax trades and writes to the file path
    fn write_trades_to_archive_path_gdax(&self, trades: &[GdaxTrade], fp: &PathBuf) {
        // Write trades to file
        let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
        for trade in trades.iter() {
            wtr.serialize(trade).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
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
        let market = match self.get_valid_market().await {
            Some(m) => m,
            None => return,
        };
        // Validate market eligibility
        let market_eligible = self.validate_market_eligibility_for_fill(&market);
        if !market_eligible {
            return;
        };
        // Get the market trade detail
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

    pub async fn get_valid_market(&self) -> Option<MarketDetail> {
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

    pub fn validate_market_eligibility_for_fill(&self, market: &MarketDetail) -> bool {
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
                ExchangeName::Gdax => (), //self.process_event_backfill_trades(e).await,
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
                let first_candle = select_first_01d_candle(&self.ig_pool, &market.market_id)
                    .await
                    .expect("Failed to select first candle.");
                let mtd = MarketTradeDetail::new(market, &first_candle.open_as_pridti()).await;
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
        let market = select_market_detail(&self.ig_pool, &event.market_id)
            .await
            .expect("Failed to select market.");
        let mut mtd = MarketTradeDetail::select(&self.ig_pool, &market)
            .await
            .expect("Failed to select market trade detail.");
        let mut current_event = Some(event.clone());
        println!("First Current Event: {:?}", current_event);
        while current_event.is_some() {
            match event.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.process_ftx_forwardfill(&current_event.unwrap(), &mtd)
                        .await;
                }
                ExchangeName::Gdax => {
                    self.process_gdax_forwardfill(&current_event.unwrap(), &mtd)
                        .await;
                }
            };
            mtd = MarketTradeDetail::select(&self.ig_pool, &market)
                .await
                .expect("Failed to select market trade detail.");
            current_event =
                Event::new_fill_trades(self.market(&event.market_id), &mtd, &event.exchange_name);
            println!("Next Current Event: {:?}", current_event);
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
                // let new_trade_table = format!("{}_qc", start.format("%Y%m%d"));
                // self.get_ftx_trades_dr_into_table(
                //     event,
                //     &new_trade_table,
                //     start,
                //     start + Duration::days(1),
                // )
                // .await;
                let new_trades = self
                    .get_ftx_trades_dr_into_vec(event, start, start + Duration::days(1))
                    .await;
                // Manually re-validate
                let is_valid = self.manual_evaluate_ftx_trades(event, &new_trades).await;
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
                    // let trade_table =
                    //     format!("trades_ftx_{}_{}", market.as_strip(), new_trade_table);
                    // let trades_to_archive =
                    //     select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
                    //         .await
                    //         .expect("Failed to select backfill trades.");
                    // Define filename = TICKER_YYYYMMDD.csv
                    let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
                    // Set filepath and file name
                    let fp = std::path::Path::new(&path).join(f);
                    // Write trades to file
                    let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
                    for trade in new_trades.iter() {
                        wtr.serialize(trade).expect("Failed to serialize trade.");
                    }
                    wtr.flush().expect("Failed to flush wtr.");
                    // Drop tables ff and bf
                    let old_trade_table = format!("bf_{}", start.format("%Y%m%d"));
                    // drop_trade_table(
                    //     &self.ig_pool,
                    //     &market.exchange_name,
                    //     market,
                    //     &new_trade_table,
                    // )
                    // .await
                    // .expect("Failed to drop trade table.");
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
                    } else {
                        // Not part of backfill process. Check that 01d candle exists, if so move
                        println!("Creating new bf trades table.");
                        let new_trade_table = format!("bf_{}", start.format("%Y%m%d"));
                        // trades to 'bf_YYYYMMDD' table from _processed and _archived
                        println!("Moving processed trades.");
                        self.migrate_ftx_trades_for_date(
                            event,
                            &new_trade_table,
                            "processed",
                            start,
                        )
                        .await;
                        println!("Moving validated trades.");
                        self.migrate_ftx_trades_for_date(
                            event,
                            &new_trade_table,
                            "validated",
                            start,
                        )
                        .await;
                        // Delete any validations for that market and date
                        println!("Deleting validations for date.");
                        self.delete_validations_for_date(event, start)
                            .await
                            .expect("Failed to purge candle validations.");
                        // Update status to get to next event will reprocess
                        println!("Updating newxt marketdatastatus to get.");
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

    // pub async fn process_event_backfill_trades(&self, event: &Event) {
    //     // Get current market trade detail
    //     // Match the exchange, if Ftx => Process the Event and mark as complete
    //     // if Gdax => Create loop to process event and the next until the next is None
    //     let market = select_market_detail(&self.ig_pool, &event.market_id)
    //         .await
    //         .expect("Faile to select market.");
    //     let mut mtd = MarketTradeDetail::select(&self.ig_pool, &market)
    //         .await
    //         .expect("Failed to select market trade detail.");
    //     let mut current_event = Some(event.clone());
    //     println!("First Current Event: {:?}", current_event);
    //     while current_event.is_some() && current_event.unwrap().event_type == EventType::BackfillTrades {

    //         match event.exchange_name {
    //             ExchangeName::Ftx | ExchangeName::FtxUs => self.process_ftx_backfill(event, &mtd).await,
    //             ExchangeName::Gdax => {
    //                 println!("Current Event: {:?}", current_event);
    //                 while current_event.is_some() {
    //                     self.process_gdax_backfill(&current_event.unwrap(), &mtd)
    //                         .await;

    //                 }
    //             }
    //         }
    //         mtd = MarketTradeDetail::select(&self.ig_pool, &market)
    //         .await
    //         .expect("Failed to select market trade detail.");
    //     current_event = Event::new_fill_trades(
    //         self.market(&event.market_id),
    //         &mtd,
    //         &ExchangeName::Gdax,
    //     );
    //     println!("Current Event: {:?}", current_event);
    //     }
    // }

    //     pub async fn process_ftx_backfill(&self, event: &Event, mtd: &MarketTradeDetail) {
    //         // Try to match the notes from the event to a market data status
    //         let status = event.notes.as_ref().unwrap().clone();
    //         let status: MarketDataStatus = status.try_into().unwrap();
    //         match status {
    //             MarketDataStatus::Completed => {} // Completed, nothing to do.
    //             MarketDataStatus::Get => {
    //                 // Get market name to get strip name for table
    //                 let market = self.market(&event.market_id);
    //                 // Create trade table for market / exch / day
    //                 let start = event.start_ts.unwrap();
    //                 let trade_table = format!("bf_{}", start.format("%Y%m%d"));
    //                 println!("Trade Table: {:?}", trade_table);
    //                 create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, &trade_table)
    //                     .await
    //                     .expect("Failed to create trade table.");
    //                 // Fill trade table with trades
    //                 let end = event.start_ts.unwrap() + Duration::days(1);
    //                 let mut end_or_last_trade = end;
    //                 let mut total_trades: usize = 0;
    //                 while start < end_or_last_trade {
    //                     // Prevent 429 errors by only requesting 4 per second
    //                     tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
    //                     let mut new_trades = match self.clients[&event.exchange_name]
    //                         .get_ftx_trades(
    //                             market.market_name.as_str(),
    //                             Some(5000),
    //                             Some(start),
    //                             Some(end_or_last_trade),
    //                         )
    //                         .await
    //                     {
    //                         Err(RestError::Reqwest(e)) => {
    //                             if e.is_timeout() || e.is_connect() || e.is_request() {
    //                                 println!(
    //                                     "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
    //                                     e
    //                                 );
    //                                 tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    //                                 continue;
    //                             } else if e.is_status() {
    //                                 match e.status() {
    //                                     Some(s) => match s.as_u16() {
    //                                         500 | 502 | 503 | 504 | 520 | 522 | 530 => {
    //                                             println!(
    //                                                 "{} status code. Waiting 30 seconds before retry {:?}",
    //                                                 s, e
    //                                             );
    //                                             tokio::time::sleep(tokio::time::Duration::from_secs(
    //                                                 30,
    //                                             ))
    //                                             .await;
    //                                             continue;
    //                                         }
    //                                         _ => {
    //                                             panic!("Status code not handled: {:?} {:?}", s, e)
    //                                         }
    //                                     },
    //                                     None => panic!("No status code for request error: {:?}", e),
    //                                 }
    //                             } else {
    //                                 panic!("Error (not timeout / connect / request): {:?}", e)
    //                             }
    //                         }
    //                         Err(e) => panic!("Other RestError: {:?}", e),
    //                         Ok(result) => result,
    //                     };
    //                     let num_trades = new_trades.len();
    //                     total_trades += num_trades; // Add to running total of trades
    //                     if num_trades > 0 {
    //                         new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
    //                         end_or_last_trade = new_trades.first().unwrap().time;
    //                         let first_trade = new_trades.last().unwrap().time;
    //                         println!(
    //                             "{} trade returned. First: {}, Last: {}",
    //                             num_trades, end_or_last_trade, first_trade
    //                         );
    //                         insert_ftx_trades(
    //                             &self.ftx_pool,
    //                             &event.exchange_name,
    //                             market,
    //                             &trade_table,
    //                             new_trades,
    //                         )
    //                         .await
    //                         .expect("Failed to insert backfill ftx trades.");
    //                     };
    //                     if num_trades < 5000 && total_trades > 0 {
    //                         // Trades returned less than REST API limit. No more trades to retreive.
    //                         break;
    //                     };
    //                     if total_trades == 0 {
    //                         // There are no trades on the day. End the backfill process
    //                         // Set prev status to complete
    //                         println!("No trades for day. Market backfill as complete.");
    //                         let _mtd = mtd
    //                             .update_prev_status(&self.ig_pool, &MarketDataStatus::Completed)
    //                             .await
    //                             .expect("Failed to update mtd.");
    //                         // Update current event to closed
    //                         event
    //                             .update_status(&self.ig_pool, &EventStatus::Done)
    //                             .await
    //                             .expect("Failed to update event status.");
    //                         return;
    //                         // TODO: Drop trade table for date
    //                     };
    //                 }
    //                 // Update mtd status to validate
    //                 let mtd = mtd
    //                     .update_prev_status(&self.ig_pool, &MarketDataStatus::Validate)
    //                     .await
    //                     .expect("Failed to update mtd.");
    //                 // Create new event and insert if it exists
    //                 let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
    //                 match new_event {
    //                     Some(e) => {
    //                         e.insert(&self.ig_pool)
    //                             .await
    //                             .expect("Failed to insert event.");
    //                     }
    //                     None => {
    //                         println!("Market complete, not further events.");
    //                         println!(
    //                             "Prev Status: {:?}\nNext Status: {:?}",
    //                             mtd.previous_status, mtd.next_status
    //                         );
    //                     }
    //                 };
    //                 // Update current event to closed
    //                 event
    //                     .update_status(&self.ig_pool, &EventStatus::Done)
    //                     .await
    //                     .expect("Failed to update event status.");
    //             }
    //             MarketDataStatus::Validate => {
    //                 // Validate trades - compare the value of all trades (price * qty) to the volume of
    //                 // the ftx candle from exchange
    //                 let market = self.market(&event.market_id);
    //                 // Create trade table for market / exch / day
    //                 let start = event.start_ts.unwrap();
    //                 let trade_table = format!(
    //                     "trades_ftx_{}_bf_{}",
    //                     market.as_strip(),
    //                     start.format("%Y%m%d")
    //                 );
    //                 let trades = select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to select ftx trades by table.");
    //                 let value = trades
    //                     .iter()
    //                     .fold(dec!(0), |val, t| val + (t.price * t.size));
    //                 let candle = get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
    //                     &self.clients[&event.exchange_name],
    //                     market,
    //                     start,
    //                     start,
    //                     86400,
    //                 )
    //                 .await
    //                 .pop();
    //                 // Update mtd status to archive if is validated, or move date back and update to get
    //                 let mtd = match candle {
    //                     Some(c) => {
    //                         println!("Trade val: {:?}, Candle Vol: {:?}", value, c.volume);
    //                         // FTX candle volume is in USD for perps. Match volume to value calculated
    //                         if c.volume == value {
    //                             // The candle was validated, set status to archive
    //                             mtd.update_prev_status(&self.ig_pool, &MarketDataStatus::Archive)
    //                                 .await
    //                                 .expect("Failed to update mtd status.")
    //                         } else {
    //                             // The candle was not validated. Move prev day -1 and set status to get
    //                             mtd.update_prev_day_prev_status(
    //                                 &self.ig_pool,
    //                                 &(start - Duration::days(1)),
    //                                 &MarketDataStatus::Get,
    //                             )
    //                             .await
    //                             .expect("Failed to update mtd.")
    //                         }
    //                     }
    //                     None => {
    //                         // No exchange candle to compare. Move prev day - 1. Set status to get
    //                         println!("No exchange candle.");
    //                         mtd.update_prev_day_prev_status(
    //                             &self.ig_pool,
    //                             &(start - Duration::days(1)),
    //                             &MarketDataStatus::Get,
    //                         )
    //                         .await
    //                         .expect("Failed to update mtd.")
    //                     }
    //                 };
    //                 // Create new event
    //                 let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
    //                 match new_event {
    //                     Some(e) => {
    //                         e.insert(&self.ig_pool)
    //                             .await
    //                             .expect("Failed to insert event.");
    //                     }
    //                     None => {
    //                         println!("Market complete, not further events.");
    //                         println!(
    //                             "Prev Status: {:?}\nNext Status: {:?}",
    //                             mtd.previous_status, mtd.next_status
    //                         );
    //                     }
    //                 };
    //                 // Update current event to closed
    //                 event
    //                     .update_status(&self.ig_pool, &EventStatus::Done)
    //                     .await
    //                     .expect("Failed to update event status.");
    //             }
    //             MarketDataStatus::Archive => {
    //                 // Save and archive trades
    //                 let market = self.market(&event.market_id);
    //                 let start = event.start_ts.unwrap();
    //                 // Check directory for exchange csv is created
    //                 let path = format!(
    //                     "{}/csv/{}",
    //                     &self.settings.application.archive_path,
    //                     &market.exchange_name.as_str()
    //                 );
    //                 std::fs::create_dir_all(&path).expect("Failed to create directories.");
    //                 let trade_table = format!(
    //                     "trades_ftx_{}_bf_{}",
    //                     market.as_strip(),
    //                     start.format("%Y%m%d")
    //                 );
    //                 let trades_to_archive = select_ftx_trades_by_table(&self.ftx_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to select backfill trades.");
    //                 let first_trade = trades_to_archive.first().unwrap();
    //                 // Define filename = TICKER_YYYYMMDD.csv
    //                 let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
    //                 // Set filepath and file name
    //                 let fp = std::path::Path::new(&path).join(f);
    //                 // Write trades to file
    //                 let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
    //                 for trade in trades_to_archive.iter() {
    //                     wtr.serialize(trade).expect("Failed to serialize trade.");
    //                 }
    //                 wtr.flush().expect("Failed to flush wtr.");
    //                 // Drop trade table
    //                 drop_table(&self.ftx_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to drop backfill trade table.");
    //                 // Update first trade id/ts. Move back prev day and set status to get
    //                 let mtd = mtd
    //                     .update_prev_day_prev_status(
    //                         &self.ig_pool,
    //                         &(start - Duration::days(1)),
    //                         &MarketDataStatus::Get,
    //                     )
    //                     .await
    //                     .expect("Failed to update mtd.");
    //                 // If first trade is ealier than mtd first trade, then update mtd
    //                 let mtd = if mtd.first_trade_ts > trades_to_archive.first().unwrap().time {
    //                     let mtd = mtd
    //                         .update_first_trade(
    //                             &self.ig_pool,
    //                             &first_trade.time,
    //                             &first_trade.id.to_string(),
    //                         )
    //                         .await
    //                         .expect("Faile to update first trade details.");
    //                     mtd
    //                 } else {
    //                     mtd
    //                 };
    //                 // Create new event
    //                 let new_event = Event::new_fill_trades(market, &mtd, &event.exchange_name);
    //                 match new_event {
    //                     Some(e) => {
    //                         e.insert(&self.ig_pool)
    //                             .await
    //                             .expect("Failed to insert event.");
    //                     }
    //                     None => {
    //                         println!("Market complete, not further events.");
    //                         println!(
    //                             "Prev Status: {:?}\nNext Status: {:?}",
    //                             mtd.previous_status, mtd.next_status
    //                         );
    //                     }
    //                 };
    //                 // Update current event to closed
    //                 event
    //                     .update_status(&self.ig_pool, &EventStatus::Done)
    //                     .await
    //                     .expect("Failed to update event status.");
    //             }
    //         };
    //     }

    //     pub async fn process_gdax_backfill(&self, event: &Event, mtd: &MarketTradeDetail) {
    //         // Try to match the notes from the event to a market data status
    //         let status = event.notes.as_ref().unwrap().clone();
    //         let status: MarketDataStatus = status.try_into().unwrap();
    //         match status {
    //             MarketDataStatus::Completed => {} // Completed, nothing to do.
    //             MarketDataStatus::Get => {
    //                 // Get market name to get strip name for table
    //                 let market = self.market(&event.market_id);
    //                 // Create trade table for market / exch / day
    //                 let start = event.start_ts.unwrap();
    //                 let trade_table = format!("bf_{}", start.format("%Y%m%d"));
    //                 println!("Trade Table: {:?}", trade_table);
    //                 drop_create_trade_table(
    //                     &self.gdax_pool,
    //                     &event.exchange_name,
    //                     market,
    //                     &trade_table,
    //                 )
    //                 .await
    //                 .expect("Failed to drop/create trade table.");
    //                 // Fill trade table with trades from start to end
    //                 // Get trade id from the start of the candle prior
    //                 let mut end_id = mtd.first_trade_id.parse::<i32>().unwrap();
    //                 let mut earliest_trade_ts = mtd.first_trade_ts;
    //                 while earliest_trade_ts > start {
    //                     // Get trades after the first trade of the previous candle in increments of 1000
    //                     // until the timestamp of the earliest trade returned is earlier than the start
    //                     // timestamp of the day that is being backfilled.
    //                     // Example:
    //                     // Backfill 12/22
    //                     // Get all trades from 2021:12:22 00:00:00 to 2021:12:23 00:00:00
    //                     // The first trade details for 12/13 are 12334567 and 2021:12:23 00:00:43
    //                     // start = 12/22
    //                     // end = 12/23
    //                     // end_id = 12334567
    //                     // earliest_trade_ts = 2021:12:23 00:00:43
    //                     // while 2021:12:23 00:00:43 > 2021:12:22 00:00:00 get the next 1000 trades.
    //                     // with those 1000 trades, reset earliest_trade_ts to the new earliest and
    //                     // continue until all trades are received
    //                     // Note that this is different logic than fill_gdax or recreate_gdax_candle
    //                     // In fill_gdax the loop is moving forward 1000 trades at a time to build
    //                     // candles as it gets them. The benefit is if there are any issues it can
    //                     // restart and pick up where it left off.
    //                     // In recreate_gdax_candle we are moving forward while checking back 1000 and
    //                     // forward 1000 extra candles to make sure it is all covered
    //                     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    //                     let mut new_trades = match self.clients[&event.exchange_name]
    //                         .get_gdax_trades(
    //                             market.market_name.as_str(),
    //                             Some(1000),
    //                             None,
    //                             Some(end_id),
    //                         )
    //                         .await
    //                     {
    //                         Err(RestError::Reqwest(e)) => {
    //                             if e.is_timeout() || e.is_connect() || e.is_request() {
    //                                 println!(
    //                                     "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
    //                                     e
    //                                 );
    //                                 tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    //                                 continue;
    //                             } else if e.is_status() {
    //                                 match e.status() {
    //                                     Some(s) => match s.as_u16() {
    //                                         500 | 502 | 503 | 504 | 520 | 522 | 530 => {
    //                                             println!(
    //                                                 "{} status code. Waiting 30 seconds before retry {:?}",
    //                                                 s, e
    //                                             );
    //                                             tokio::time::sleep(tokio::time::Duration::from_secs(
    //                                                 30,
    //                                             ))
    //                                             .await;
    //                                             continue;
    //                                         }
    //                                         _ => {
    //                                             panic!("Status code not handled: {:?} {:?}", s, e)
    //                                         }
    //                                     },
    //                                     None => panic!("No status code for request error: {:?}", e),
    //                                 }
    //                             } else {
    //                                 panic!("Error (not timeout / connect / request): {:?}", e)
    //                             }
    //                         }
    //                         Err(e) => panic!("Other RestError: {:?}", e),
    //                         Ok(result) => result,
    //                     };
    //                     if new_trades.is_empty() {
    //                         // No new trades were returned. This should only happen when an attempt is
    //                         // made to reterive trades after id = 1. You have reached the end of the
    //                         // backfill and the beginning of the market. Mark as completed.
    //                         // Update the first trade info - This will need to be validated
    //                         let mtd = mtd
    //                             .update_prev_day_prev_status(
    //                                 &self.ig_pool,
    //                                 &(start - Duration::days(1)),
    //                                 &MarketDataStatus::Completed,
    //                             )
    //                             .await
    //                             .expect("Failed to update mtd.");
    //                         // If first trade is ealier than mtd first trade, then update mtd
    //                         let _mtd = if mtd.first_trade_id.parse::<i32>().unwrap() > end_id {
    //                             let mtd = mtd
    //                                 .update_first_trade(
    //                                     &self.ig_pool,
    //                                     &earliest_trade_ts,
    //                                     &end_id.to_string(),
    //                                 )
    //                                 .await
    //                                 .expect("Faile to update first trade details.");
    //                             mtd
    //                         } else {
    //                             mtd
    //                         };
    //                         // Update current event to closed
    //                         event
    //                             .update_status(&self.ig_pool, &EventStatus::Done)
    //                             .await
    //                             .expect("Failed to update event status.");
    //                         return;
    //                     } else {
    //                         // Add the returned trade the the trade table, update the earliest trade
    //                         // timestamp and id
    //                         new_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
    //                         let earliest_trade = new_trades.first().unwrap();
    //                         let newest_trade = new_trades.last().unwrap();
    //                         earliest_trade_ts = earliest_trade.time;
    //                         end_id = earliest_trade.trade_id as i32;
    //                         println!(
    //                             "{} {} trades returned. First: {} Last: {}",
    //                             new_trades.len(),
    //                             market.market_name,
    //                             earliest_trade.time(),
    //                             newest_trade.time()
    //                         );
    //                         insert_gdax_trades(
    //                             &self.gdax_pool,
    //                             &event.exchange_name,
    //                             market,
    //                             &trade_table,
    //                             new_trades,
    //                         )
    //                         .await
    //                         .expect("Failed to insert trades.");
    //                     };
    //                 }
    //                 // Update mtd status to validate
    //                 let _mtd = mtd
    //                     .update_prev_status(&self.ig_pool, &MarketDataStatus::Validate)
    //                     .await
    //                     .expect("Failed to update mtd.");
    //             }
    //             MarketDataStatus::Validate => {
    //                 // Validate the trades - GDAX trade ids are sequential per product. Validate:
    //                 // 1) There are no gaps in trade ids. Highest ID - Lowest ID + 1 = Num Trades
    //                 // ie 1001 - 94 + 1 = 908 = 908 trades
    //                 // 2) The next trade id in sequence falls on the next day
    //                 // 3) The prev trade id in the sequence falls on the previous day
    //                 let market = self.market(&event.market_id);
    //                 // Create trade table for market / exch / day
    //                 let start = event.start_ts.unwrap();
    //                 let trade_table = format!(
    //                     "trades_gdax_{}_bf_{}",
    //                     market.as_strip(),
    //                     start.format("%Y%m%d")
    //                 );
    //                 let trades_in_table = select_gdax_trades_by_table(&self.gdax_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to select backfill trades.");
    //                 println!("{} trades in table.", trades_in_table.len());
    //                 // Filter trades
    //                 let mut trades_to_validate: Vec<crate::exchanges::gdax::Trade> = trades_in_table
    //                     .iter()
    //                     .filter(|t| t.time() >= start && t.time() < start + Duration::days(1))
    //                     .cloned()
    //                     .collect();
    //                 println!("{} trades to validate.", trades_to_validate.len());
    //                 // Sort trades by id
    //                 trades_to_validate.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
    //                 // Perform validations
    //                 println!("First: {:?}", trades_to_validate.first());
    //                 println!("Last: {:?}", trades_to_validate.last());
    //                 let validation_1 = if trades_to_validate.is_empty() {
    //                     false
    //                 } else {
    //                     trades_to_validate.last().unwrap().trade_id
    //                         - trades_to_validate.first().unwrap().trade_id
    //                         + 1
    //                         == trades_to_validate.len() as i64
    //                 };
    //                 println!("Validation 1: {}", validation_1);
    //                 let validation_2 = if trades_to_validate.is_empty() {
    //                     false
    //                 } else {
    //                     let next_trade = match self.clients[&event.exchange_name]
    //                         .get_gdax_next_trade(
    //                             market.market_name.as_str(),
    //                             trades_to_validate.last().unwrap().trade_id as i32,
    //                         )
    //                         .await
    //                     {
    //                         Err(RestError::Reqwest(e)) => {
    //                             if e.is_timeout() || e.is_connect() || e.is_request() {
    //                                 println!(
    //                                         "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
    //                                         e
    //                                     );
    //                                 tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    //                                 return; // Return event is imcomplete. Try to process again
    //                             } else if e.is_status() {
    //                                 match e.status() {
    //                                     Some(s) => match s.as_u16() {
    //                                         500 | 502 | 503 | 504 | 520 | 522 | 530 => {
    //                                             println!(
    //                                                     "{} status code. Waiting 30 seconds before retry {:?}",
    //                                                     s, e
    //                                                 );
    //                                             tokio::time::sleep(tokio::time::Duration::from_secs(
    //                                                 30,
    //                                             ))
    //                                             .await;
    //                                             return;
    //                                             // Leave event incomplete and try to process again
    //                                         }
    //                                         _ => {
    //                                             panic!("Status code not handled: {:?} {:?}", s, e)
    //                                         }
    //                                     },
    //                                     None => panic!("No status code for request error: {:?}", e),
    //                                 }
    //                             } else {
    //                                 panic!("Error (not timeout / connect / request): {:?}", e)
    //                             }
    //                         }
    //                         Err(e) => panic!("Other RestError: {:?}", e),
    //                         Ok(result) => result,
    //                     };
    //                     println!("Next trade: {:?}", next_trade);
    //                     if !next_trade.is_empty() {
    //                         // Pop off the trade
    //                         let next_trade = next_trade.first().unwrap();
    //                         // Compare the day of the next trade to the last trade day of the trades
    //                         // to validated
    //                         let last_trade = trades_to_validate.last().unwrap();
    //                         next_trade.time().day() > last_trade.time().day()
    //                     } else {
    //                         // If next trade is empty, return false. There should always be a next trade
    //                         false
    //                     }
    //                 };
    //                 println!("Validation 2: {}", validation_2);
    //                 let validation_3 = if trades_to_validate.is_empty() {
    //                     false
    //                 } else {
    //                     let previous_trade = match self.clients[&event.exchange_name]
    //                         .get_gdax_previous_trade(
    //                             market.market_name.as_str(),
    //                             trades_to_validate.first().unwrap().trade_id as i32,
    //                         )
    //                         .await
    //                     {
    //                         Err(RestError::Reqwest(e)) => {
    //                             if e.is_timeout() || e.is_connect() || e.is_request() {
    //                                 println!(
    //                                         "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
    //                                         e
    //                                     );
    //                                 tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    //                                 return; // Return event is imcomplete. Try to process again
    //                             } else if e.is_status() {
    //                                 match e.status() {
    //                                     Some(s) => match s.as_u16() {
    //                                         500 | 502 | 503 | 504 | 520 | 522 | 530 => {
    //                                             println!(
    //                                                     "{} status code. Waiting 30 seconds before retry {:?}",
    //                                                     s, e
    //                                                 );
    //                                             tokio::time::sleep(tokio::time::Duration::from_secs(
    //                                                 30,
    //                                             ))
    //                                             .await;
    //                                             return;
    //                                             // Leave event incomplete and try to process again
    //                                         }
    //                                         _ => {
    //                                             panic!("Status code not handled: {:?} {:?}", s, e)
    //                                         }
    //                                     },
    //                                     None => panic!("No status code for request error: {:?}", e),
    //                                 }
    //                             } else {
    //                                 panic!("Error (not timeout / connect / request): {:?}", e)
    //                             }
    //                         }
    //                         Err(e) => panic!("Other RestError: {:?}", e),
    //                         Ok(result) => result,
    //                     };
    //                     println!("Previous trade: {:?}", previous_trade);
    //                     if !previous_trade.is_empty() {
    //                         // Pop off the trade
    //                         let previous_trade = previous_trade.first().unwrap();
    //                         // Compare the day of the next trade to the last trade day of the trades
    //                         // to validated
    //                         let first_trade = trades_to_validate.first().unwrap();
    //                         previous_trade.time().day() < first_trade.time().day()
    //                     } else {
    //                         // If previous trade is empty, check if first trade id = 1.
    //                         // If so there is no previous day and it is valid.
    //                         let first_trade = trades_to_validate.first().unwrap();
    //                         first_trade.trade_id == 1
    //                     }
    //                 };
    //                 println!("Validation 3: {}", validation_3);
    //                 let _mtd = if validation_1 && validation_2 && validation_3 {
    //                     // All three validations met, market it as validated
    //                     mtd.update_prev_status(&self.ig_pool, &MarketDataStatus::Archive)
    //                         .await
    //                         .expect("Failed to update mtd status.")
    //                 } else {
    //                     let mtd = mtd
    //                         .update_prev_day_prev_status(
    //                             &self.ig_pool,
    //                             &(start - Duration::days(1)),
    //                             &MarketDataStatus::Get,
    //                         )
    //                         .await
    //                         .expect("Failed to update mtd.");
    //                     // Update first id
    //                     if !trades_to_validate.is_empty() {
    //                         let earliest_trade = trades_to_validate.first().unwrap();
    //                         if mtd.first_trade_id.parse::<i32>().unwrap()
    //                             > earliest_trade.trade_id as i32
    //                         {
    //                             mtd.update_first_trade(
    //                                 &self.ig_pool,
    //                                 &earliest_trade.time,
    //                                 &earliest_trade.trade_id.to_string(),
    //                             )
    //                             .await
    //                             .expect("Failed to update previous trade.")
    //                         } else {
    //                             mtd
    //                         }
    //                     } else {
    //                         mtd
    //                     }
    //                 };
    //                 // // Create new event
    //                 // let new_event = Event::new_backfill_trades(&mtd, &event.exchange_name);
    //                 // match new_event {
    //                 //     Some(e) => {
    //                 //         e.insert(&self.ig_pool)
    //                 //             .await
    //                 //             .expect("Failed to insert event.");
    //                 //     }
    //                 //     None => {
    //                 //         println!("Market complete, not further events.");
    //                 //         println!(
    //                 //             "Prev Status: {:?}\nNext Status: {:?}",
    //                 //             mtd.previous_status, mtd.next_status
    //                 //         );
    //                 //     }
    //                 // };
    //                 // // Update current event to closed
    //                 // event
    //                 //     .update_status(&self.ig_pool, &EventStatus::Done)
    //                 //     .await
    //                 //     .expect("Failed to update event status.");
    //             }
    //             MarketDataStatus::Archive => {
    //                 // Save and archive trades
    //                 let market = self.market(&event.market_id);
    //                 let start = event.start_ts.unwrap();
    //                 // Check directory for exchange csv is created
    //                 let path = format!(
    //                     "{}/csv/{}",
    //                     &self.settings.application.archive_path,
    //                     &market.exchange_name.as_str()
    //                 );
    //                 std::fs::create_dir_all(&path).expect("Failed to create directories.");
    //                 let trade_table = format!(
    //                     "trades_gdax_{}_bf_{}",
    //                     market.as_strip(),
    //                     start.format("%Y%m%d")
    //                 );
    //                 let trades_in_table = select_gdax_trades_by_table(&self.gdax_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to select backfill trades.");
    //                 // Filter trades
    //                 let trades_to_archive: Vec<crate::exchanges::gdax::Trade> = trades_in_table
    //                     .iter()
    //                     .filter(|t| t.time() >= start && t.time() < start + Duration::days(1))
    //                     .cloned()
    //                     .collect();
    //                 // Define filename = TICKER_YYYYMMDD.csv
    //                 let f = format!("{}_{}.csv", market.as_strip(), start.format("%F"));
    //                 // Set filepath and file name
    //                 let fp = std::path::Path::new(&path).join(f);
    //                 // Write trades to file
    //                 let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
    //                 for trade in trades_to_archive.iter() {
    //                     wtr.serialize(trade).expect("Failed to serialize trade.");
    //                 }
    //                 wtr.flush().expect("Failed to flush wtr.");
    //                 // Drop trade table
    //                 drop_table(&self.gdax_pool, &trade_table)
    //                     .await
    //                     .expect("Failed to drop backfill trade table.");
    //                 // Update first trade id/ts. Move back prev day and set status to get
    //                 let mtd = mtd
    //                     .update_prev_day_prev_status(
    //                         &self.ig_pool,
    //                         &(start - Duration::days(1)),
    //                         &MarketDataStatus::Get,
    //                     )
    //                     .await
    //                     .expect("Failed to update mtd.");
    //                 // If first trade is ealier than mtd first trade, then update mtd
    //                 let _mtd = if !trades_to_archive.is_empty() {
    //                     let first_trade = trades_to_archive.first().unwrap();
    //                     let mtd = if mtd.first_trade_ts > trades_to_archive.first().unwrap().time {
    //                         let mtd = mtd
    //                             .update_first_trade(
    //                                 &self.ig_pool,
    //                                 &first_trade.time,
    //                                 &first_trade.trade_id.to_string(),
    //                             )
    //                             .await
    //                             .expect("Faile to update first trade details.");
    //                         mtd
    //                     } else {
    //                         mtd
    //                     };
    //                     mtd
    //                 } else {
    //                     mtd
    //                 };
    //                 // // Create new event
    //                 // let new_event = Event::new_backfill_trades(&mtd, &event.exchange_name);
    //                 // match new_event {
    //                 //     Some(e) => {
    //                 //         e.insert(&self.ig_pool)
    //                 //             .await
    //                 //             .expect("Failed to insert event.");
    //                 //     }
    //                 //     None => {
    //                 //         println!("Market complete, not further events.");
    //                 //         println!(
    //                 //             "Prev Status: {:?}\nNext Status: {:?}",
    //                 //             mtd.previous_status, mtd.next_status
    //                 //         );
    //                 //     }
    //                 // };
    //                 // // Update current event to closed
    //                 // event
    //                 //     .update_status(&self.ig_pool, &EventStatus::Done)
    //                 //     .await
    //                 //     .expect("Failed to update event status.");
    //             }
    //         }
    //     }
}

// #[cfg(test)]
// mod tests {
//     use crate::configuration::get_configuration;
//     use crate::events::{Event, EventStatus, EventType};
//     use crate::exchanges::ftx::Trade as FtxTrade;
//     use crate::exchanges::{client::RestClient, ExchangeName};
//     use crate::inquisidor::Inquisidor;
//     use crate::markets::{
//         select_market_detail_by_name, select_market_details, MarketDataStatus, MarketDetail,
//     };
//     use crate::mita::Mita;
//     use crate::trades::{create_ftx_trade_table, drop_trade_table, insert_ftx_trade};
//     use chrono::{Duration, TimeZone, Utc};
//     use csv::Writer;
//     use rust_decimal::Decimal;
//     use rust_decimal_macros::dec;
//     use sqlx::PgPool;
//     use uuid::Uuid;

//     pub async fn prep_ftx_market(pool: &PgPool) {
//         // Update market to active with valid timestamp
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', '2021-12-01 00:00:00+00')
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Clear market trade details and daily candles
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(pool)
//             .await
//             .expect("Failed to update market trade details.");
//         let sql = r#"
//             DELETE FROM events
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(pool)
//             .await
//             .expect("Failed to update market trade details.");
//         let sql = r#"
//             DELETE FROM candles_01d
//             WHERE market_id = '19994c6a-fa3c-4b0b-96c4-c744c43a9514'
//             "#;
//         sqlx::query(sql)
//             .execute(pool)
//             .await
//             .expect("Failed to update market trade details.");
//         let sql = r#"
//             INSERT INTO candles_01d (
//                 datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
//                 trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
//                 market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
//             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
//                 $18, $19)
//             "#;
//         sqlx::query(sql)
//             .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(100))
//             .bind(dec!(0))
//             .bind(dec!(10))
//             .bind(dec!(5000))
//             .bind(20)
//             .bind(10)
//             .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
//             .bind("1234")
//             .bind(true)
//             .bind(Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap())
//             .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
//             .bind("1234")
//             .bind(true)
//             .bind(false)
//             .execute(pool)
//             .await
//             .expect("Failed to insert candle for test.");
//     }

//     pub async fn prep_ftx_table_with_trade(
//         pool: &PgPool,
//         market: &MarketDetail,
//         price: Decimal,
//         quantity: Decimal,
//     ) {
//         // Create trade table for market / exch / day
//         let trade_table = "bf_20211130";
//         drop_trade_table(pool, &ExchangeName::Ftx, market, trade_table)
//             .await
//             .expect("Failed to drop trade table.");
//         create_ftx_trade_table(pool, &ExchangeName::Ftx, market, trade_table)
//             .await
//             .expect("Failed to create trade table.");
//         // Create trade
//         let trade = create_ftx_trade(price, quantity);
//         // Insert trade into table
//         insert_ftx_trade(pool, &ExchangeName::Ftx, market, trade_table, trade)
//             .await
//             .expect("Failed to insert trade.");
//     }

//     pub fn prep_ftx_file(path: &str) {
//         // Create trade
//         let trade = create_ftx_trade(dec!(886644021.83975), dec!(1));
//         let trades = vec![trade];
//         // Check directory is created
//         std::fs::create_dir_all(&path).expect("Failed to create directories.");
//         // Set filepath
//         let fp = std::path::Path::new(&path).join("SOLPERP_2021-11-30.csv");
//         // Write trades to file
//         let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
//         for trade in trades.iter() {
//             wtr.serialize(trade).expect("Failed to serialize trade.");
//         }
//         wtr.flush().expect("Failed to flush wtr.");
//     }

//     pub fn create_ftx_trade(price: Decimal, quantity: Decimal) -> FtxTrade {
//         FtxTrade {
//             id: 123,
//             price,
//             size: quantity,
//             side: "buy".to_string(),
//             liquidation: false,
//             time: Utc.ymd(2021, 11, 30).and_hms(0, 1, 0),
//         }
//     }

//     #[tokio::test]
//     pub async fn get_gdax_start_old_asset_returns_90d() {
//         // Create mita
//         let m = Mita::new().await;
//         // Load configuration
//         let configuration = get_configuration().expect("Failed to read configuration.");
//         println!("Configuration: {:?}", configuration);
//         // Create db connection
//         let pool = PgPool::connect_with(configuration.ftx_db.with_db())
//             .await
//             .expect("Failed to connect to Postgres.");
//         // Create rest client
//         let client = RestClient::new(&ExchangeName::Gdax);
//         // Select old asset (BTC or ETH) and run get gdax start
//         let market_details = select_market_details(&pool)
//             .await
//             .expect("Failed to select market details.");
//         let market = market_details
//             .iter()
//             .find(|m| m.market_name == "BTC-USD")
//             .unwrap();
//         // Get gdax start
//         println!("Getting GDAX start for BTC-USD");
//         let (id, ts) = m.get_gdax_start(&client, market).await;
//         println!("ID / TS: {:?} {:?}", id, ts);
//     }

//     #[tokio::test]
//     pub async fn get_gdax_start_new_asset_returns_first_day() {
//         // Create mita
//         let m = Mita::new().await;
//         // Load configuration
//         let configuration = get_configuration().expect("Failed to read configuration.");
//         println!("Configuration: {:?}", configuration);
//         // Create db connection
//         let pool = PgPool::connect_with(configuration.ftx_db.with_db())
//             .await
//             .expect("Failed to connect to Postgres.");
//         // Create rest client
//         let client = RestClient::new(&ExchangeName::Gdax);
//         // Select old asset (BTC or ETH) and run get gdax start
//         let market_details = select_market_details(&pool)
//             .await
//             .expect("Failed to select market details.");
//         let market = market_details
//             .iter()
//             .find(|m| m.market_name == "ORCA-USD")
//             .unwrap();
//         // Get gdax start
//         println!("Getting GDAX start for ORCA-USD");
//         let (id, ts) = m.get_gdax_start(&client, market).await;
//         println!("ID / TS: {:?} {:?}", id, ts);
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_with_no_candle_fails() {
//         // Setup failing market scenario
//         let ig = Inquisidor::new().await;
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', Null)
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Select the market
//         let market = select_market_detail_by_name(&ig.ig_pool, "SOL-PERP")
//             .await
//             .expect("Failed to select market detail.");
//         // Test the eligibility fails
//         assert!(!ig.validate_market_eligibility_for_fill(&market));
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_with_non_active_market_fails() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('terminated', '2022-09-21 18:30:00+00')
//             WHERE market_name = 'SOL-PERP'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         // Select the market
//         let market = select_market_detail_by_name(&ig.ig_pool, "SOL-PERP")
//             .await
//             .expect("Failed to select market detail.");
//         // Test the eligibility fails
//         assert!(!ig.validate_market_eligibility_for_fill(&market));
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_with_no_mtd_creates_new() {
//         // Setup
//         // Get pool and prep market
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig instance will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get test market
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         // Test
//         let mtd = ig.get_market_trade_detail(&market).await;
//         // Validate
//         assert_eq!(mtd.previous_status, MarketDataStatus::Get);
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_with_completed_does_nothing() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd and event
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mtd = ig.get_market_trade_detail(&market).await;
//         let mut event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Change event notes to 'completed'
//         event.notes = Some("completed".to_string());
//         // Process the completed backfill event -> It should do nothing
//         ig.process_ftx_backfill(&event, &mtd).await;
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_get_gets_trades_updates_mtd_creates_new_event() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd and event
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mtd = ig.get_market_trade_detail(&market).await;
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Process the event to get the trades for the first day
//         ig.process_event_backfill_trades(&event).await;
//         // Assert mtd status is now validated
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.previous_status, MarketDataStatus::Validate);
//         // Assert new event was created to validate trades
//         let mi = Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap();
//         let events = Event::select_by_statuses_type(
//             &ig.ig_pool,
//             &vec![EventStatus::New, EventStatus::Open],
//             &EventType::BackfillTrades,
//         )
//         .await
//         .expect("Failed to select backfill event.");
//         let event = events.iter().find(|e| e.market_id == mi).unwrap();
//         assert_eq!(event.notes, Some("validate".to_string()));
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_get_gets_trades_zero_trades_completes_backfill() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         let sql = r#"
//             UPDATE candles_01d
//             SET datetime = '2017-12-31 00:00:00'
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update market trade details.");
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Run backfill to create mtd and event
//         // Get mtd and event
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mtd = ig.get_market_trade_detail(&market).await;
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Process the event to get the trades for the first day
//         ig.process_event_backfill_trades(&event).await;
//         // Assert mtd status is now completed
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.previous_status, MarketDataStatus::Completed);
//         // Assert new event was NOT created and there is no new event
//         let mi = Uuid::parse_str("19994c6a-fa3c-4b0b-96c4-c744c43a9514").unwrap();
//         let events = Event::select_by_statuses_type(
//             &ig.ig_pool,
//             &vec![EventStatus::New, EventStatus::Open],
//             &EventType::BackfillTrades,
//         )
//         .await
//         .expect("Failed to select backfill event.");
//         let event = events.iter().find(|e| e.market_id == mi);
//         assert!(event.is_none());
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_validate_success_updates_mtd_to_archive_creates_new_event() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to validate
//         mtd.previous_status = MarketDataStatus::Validate;
//         let original_prev_date = mtd.previous_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Add correct trade to trade table
//         prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(886644021.83975), dec!(1)).await;
//         // Process the event
//         ig.process_event_backfill_trades(&event).await;
//         // Assert mtd is now archive
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.previous_status, MarketDataStatus::Archive);
//         // Assert the new event was created to archive trades
//         let events = Event::select_by_statuses_type(
//             &ig.ig_pool,
//             &vec![EventStatus::New, EventStatus::Open],
//             &EventType::BackfillTrades,
//         )
//         .await
//         .expect("Failed to select backfill event.");
//         let event = events
//             .iter()
//             .find(|e| e.market_id == market.market_id)
//             .unwrap();
//         assert_eq!(event.notes, Some("archive".to_string()));
//         // Assert new mtd next date is not changed
//         assert_eq!(original_prev_date, mtd.previous_trade_day);
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_validate_failure_updates_mtd_to_next_get_creates_new_event() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to validate
//         mtd.previous_status = MarketDataStatus::Validate;
//         let original_prev_date = mtd.previous_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Add incorrect trade to trade table
//         prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(123.321), dec!(1)).await;
//         // Process the event
//         ig.process_event_backfill_trades(&event).await;
//         // Assert mtd is now archive
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.previous_status, MarketDataStatus::Get);
//         // Assert the new event was created to archive trades
//         let events = Event::select_by_statuses_type(
//             &ig.ig_pool,
//             &vec![EventStatus::New, EventStatus::Open],
//             &EventType::BackfillTrades,
//         )
//         .await
//         .expect("Failed to select backfill event.");
//         let event = events
//             .iter()
//             .find(|e| e.market_id == market.market_id)
//             .unwrap();
//         assert_eq!(event.notes, Some("archive".to_string()));
//         // Assert new mtd next date is not changed
//         assert_eq!(
//             original_prev_date,
//             mtd.previous_trade_day + Duration::days(1)
//         );
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_archive_updates_mtd_creates_new_event() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to validate
//         mtd.previous_status = MarketDataStatus::Archive;
//         let original_prev_date = mtd.previous_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Add correct trade to trade table
//         prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(886644021.83975), dec!(1)).await;
//         // Process the event
//         ig.process_event_backfill_trades(&event).await;
//         // Assert mtd is now get
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.previous_status, MarketDataStatus::Get);
//         // Assert the new event was created to archive trades
//         let events = Event::select_by_statuses_type(
//             &ig.ig_pool,
//             &vec![EventStatus::New, EventStatus::Open],
//             &EventType::BackfillTrades,
//         )
//         .await
//         .expect("Failed to select backfill event.");
//         let event = events
//             .iter()
//             .find(|e| e.market_id == market.market_id)
//             .unwrap();
//         assert_eq!(event.notes, Some("get".to_string()));
//         // Assert new mtd next date is not changed
//         assert_eq!(
//             original_prev_date,
//             mtd.previous_trade_day + Duration::days(1)
//         );
//         // Assert file exists
//         let f = format!(
//             "{}_{}.csv",
//             market.as_strip(),
//             original_prev_date.format("%F")
//         );
//         let archive_path = format!(
//             "{}/csv/{}",
//             &ig.settings.application.archive_path,
//             &market.exchange_name.as_str()
//         );
//         let f_path = std::path::Path::new(&archive_path).join(f.clone());
//         assert!(f_path.exists());
//     }

//     #[tokio::test]
//     pub async fn backfill_ftx_completed_creates_first_forwardfill_event() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Get mtd and event
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // Assert event is forwardfill
//         assert_eq!(event.event_type, EventType::ForwardFillTrades);
//         // Assert event is validate
//         assert_eq!(event.notes, Some("validate".to_string()));
//     }

//     #[tokio::test]
//     pub async fn forwardfill_ftx_get_accept_writes_file() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Add correct trade to trade table
//         prep_ftx_table_with_trade(&ig.ig_pool, market, dec!(1234021.83975), dec!(1)).await;
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         // Modify mtd next status to some(get)
//         mtd.next_status = Some(MarketDataStatus::Get);
//         // Process forwardfill event
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//     }

//     // pub async fn forwardfill_ftx_get_reject_does_nothing() {}

//     #[tokio::test]
//     pub async fn forwardfill_ftx_validate_loc3_moves_and_creates_archive() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Set up trade file in location 3
//         let path = format!("{}/csv/gdax2", &ig.settings.application.archive_path,);
//         prep_ftx_file(&path);
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         let original_next_date = mtd.next_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//         // Assert mtd is now archive
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
//         // Assert new mtd next date is not changed
//         assert_eq!(original_next_date, mtd.next_trade_day);
//         // Assert next event it correct
//         // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         mtd.previous_status = MarketDataStatus::Completed;
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         assert_eq!(
//             event.start_ts,
//             Some(mtd.previous_trade_day + Duration::days(1))
//         );
//         assert_eq!(event.notes.unwrap(), "archive".to_string());
//     }

//     #[tokio::test]
//     pub async fn forwardfill_ftx_validate_loc2_moves_and_creates_archive() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Set up trade file in location 3
//         let path = format!("{}/csv/gdax/gdax", &ig.settings.application.archive_path,);
//         prep_ftx_file(&path);
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         let original_next_date = mtd.next_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//         // Assert mtd is now archive
//         let mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
//         // Assert new mtd next date is not changed
//         assert_eq!(original_next_date, mtd.next_trade_day);
//     }

//     #[tokio::test]
//     pub async fn fowardfill_ftx_validate_loc1_creates_archive() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Set up trade file in proper location
//         let path = format!(
//             "{}/csv/{}",
//             &ig.settings.application.archive_path,
//             &market.exchange_name.as_str(),
//         );
//         prep_ftx_file(&path);
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         let original_next_date = mtd.next_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//         // Assert mtd is now archive
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         assert_eq!(mtd.next_status, Some(MarketDataStatus::Archive));
//         // Assert new mtd next date is not changed
//         assert_eq!(original_next_date, mtd.next_trade_day);
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("New Event: {:?}", event);
//     }

//     #[tokio::test]
//     pub async fn forwardfill_ftx_validate_noloc_creates_get() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         let original_next_date = mtd.next_trade_day;
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//         // Assert mtd is now archive
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         assert_eq!(mtd.next_status, Some(MarketDataStatus::Get));
//         // Assert new mtd next date is not changed
//         assert_eq!(original_next_date, mtd.next_trade_day);
//         // Assert next event it correct
//         // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         mtd.previous_status = MarketDataStatus::Completed;
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         assert_eq!(
//             event.start_ts,
//             Some(mtd.previous_trade_day + Duration::days(1))
//         );
//         assert_eq!(event.notes.unwrap(), "get".to_string());
//     }

//     #[tokio::test]
//     pub async fn forwardfill_ftx_archive_moves_file_creates_completed() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         prep_ftx_market(&ig.ig_pool).await;
//         // Get mtd and modify for test
//         let market = ig
//             .markets
//             .iter()
//             .find(|m| m.market_name == "SOL-PERP")
//             .unwrap();
//         // Set up trade file in proper location
//         let path = format!(
//             "{}/csv/{}",
//             &ig.settings.application.archive_path,
//             &market.exchange_name.as_str(),
//         );
//         prep_ftx_file(&path);
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // Modify mtd prev status to completed
//         mtd.previous_status = MarketDataStatus::Completed;
//         // Modify mtd next status to archive
//         mtd.next_status = Some(MarketDataStatus::Archive);
//         // Modify mt prev date to 11/29 (as 11/30 is our test day)
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         let original_next_date = mtd.previous_trade_day + Duration::days(1);
//         // Get event
//         let event = ig.get_fill_event(&market, &mtd).await.unwrap();
//         // println!("{:?}", event);
//         // Process event
//         ig.process_ftx_forwardfill(&event, &mtd).await;
//         // Assert mtd is now archive
//         let mut mtd = ig.get_market_trade_detail(&market).await;
//         // println!("mtd: {:?}", mtd);
//         assert_eq!(mtd.next_status, Some(MarketDataStatus::Completed));
//         // Assert new mtd next date incremented
//         assert_eq!(
//             Some(original_next_date + Duration::days(1)),
//             mtd.next_trade_day
//         );
//         // Assert next event it correct
//         // Modify mt prev date to 11/29 (as 11/30 is our test day) - Same mods as prior
//         mtd.previous_trade_day = mtd.previous_trade_day - Duration::days(1);
//         mtd.previous_status = MarketDataStatus::Completed;
//         // println!("{:?}", mtd);
//         let event = ig.get_fill_event(&market, &mtd).await;
//         assert!(event.is_none());
//     }

//     #[tokio::test]
//     pub async fn backfill_gdax_with_completed_does_nothing() {
//         // Setup
//         let ig = Inquisidor::new().await;
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', $1)
//             WHERE market_name = 'AAVE-USD'
//             "#;
//         sqlx::query(sql)
//             .bind(Utc.ymd(2021, 12, 01).and_hms(0, 0, 0))
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update market trade details.");
//         let sql = r#"
//             DELETE FROM candles_01d
//             WHERE market_id = $1
//             "#;
//         sqlx::query(sql)
//             .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to delete 01 candles.");
//         let sql = r#"
//             DELETE FROM events
//             WHERE event_type = 'backfilltrades'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to delete events.");
//         let sql = r#"
//             INSERT INTO candles_01d (
//                 datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
//                 trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
//                 market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
//             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
//                 $18, $19)
//             "#;
//         sqlx::query(sql)
//             .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(100))
//             .bind(dec!(0))
//             .bind(dec!(10))
//             .bind(dec!(5000))
//             .bind(20)
//             .bind(10)
//             .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
//             .bind("1234")
//             .bind(true)
//             .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
//             .bind(Utc.ymd(2020, 12, 16).and_hms(0, 0, 0))
//             .bind("1234")
//             .bind(true)
//             .bind(false)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to insert candle for test.");
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Run backfill to create mtd and event and run backfill loop
//         ig.fill().await;
//     }

//     #[tokio::test]
//     pub async fn backfill_gdax_get_validate_archive() {
//         // TODO: Refactor into get / validate / archive tests with assertions
//         // Setup
//         let ig = Inquisidor::new().await;
//         let sql = r#"
//             UPDATE markets
//             SET (market_data_status, last_candle) = ('active', $1)
//             WHERE market_name = 'AAVE-USD'
//             "#;
//         sqlx::query(sql)
//             .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update last candle to null.");
//         let sql = r#"
//             DELETE FROM market_trade_details
//             WHERE 1=1
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to update market trade details.");
//         let sql = r#"
//             DELETE FROM candles_01d
//             WHERE market_id = $1
//             "#;
//         sqlx::query(sql)
//             .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to delete 01 candles.");
//         let sql = r#"
//             DELETE FROM events
//             WHERE event_type = 'backfilltrades'
//             "#;
//         sqlx::query(sql)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to delete events.");
//         let sql = r#"
//             INSERT INTO candles_01d (
//                 datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
//                 trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
//                 market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
//             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
//                 $18, $19)
//             "#;
//         sqlx::query(sql)
//             .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(50))
//             .bind(dec!(100))
//             .bind(dec!(0))
//             .bind(dec!(10))
//             .bind(dec!(5000))
//             .bind(20)
//             .bind(10)
//             .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
//             .bind("13183391")
//             .bind(true)
//             .bind(Uuid::parse_str("94ef1d69-cddb-4e42-94db-af2675c05e1c").unwrap())
//             .bind(Utc.ymd(2021, 12, 06).and_hms(0, 0, 0))
//             .bind("13183391")
//             .bind(true)
//             .bind(false)
//             .execute(&ig.ig_pool)
//             .await
//             .expect("Failed to insert candle for test.");
//         // New ig will pick up new data items
//         let ig = Inquisidor::new().await;
//         // Run backfill to create mtd and event and run backfill loop
//         ig.fill().await;
//     }
// }
