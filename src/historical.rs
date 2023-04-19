use crate::{
    candles::{CandleType, ProductionCandle, ResearchCandle},
    configuration::Database,
    eldorado::ElDorado,
    exchanges::{gdax::Trade as GdaxTrade, ExchangeName},
    markets::{
        MarketCandleDetail, MarketDataStatus, MarketDetail, MarketStatus, MarketTradeDetail,
    },
    mita::Heartbeat,
    trades::{PrIdTi, Trade},
    utilities::TimeFrame,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use csv::Writer;
use rust_decimal_macros::dec;
use std::{collections::HashMap, path::Path, path::PathBuf};

impl ElDorado {
    pub async fn sync(&self, heartbeats: &mut HashMap<String, Heartbeat>) {
        // let mut heartbeats: HashMap<String, Heartbeat> = HashMap::new();
        for market in self.markets.iter() {
            // Create new heartbeat and insert
            heartbeats.insert(market.market_name.clone(), Heartbeat::new());
            // Set Vec capacity for base candles - use sync days + 10 days
            let capacity = market.tf.per_day() * (self.sync_days + 10);
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.candles
                        .insert(market.tf, Vec::with_capacity(capacity as usize));
                });
            // For each market in eldorado instnace, determine the start time and id for the sync
            self.calculate_sync_start(market, heartbeats).await;
            println!(
                "Sync Start: {}\tLast Trade: {:?}",
                heartbeats.get(&market.market_name).unwrap().ts,
                heartbeats.get(&market.market_name).unwrap().last
            );
            println!(
                "Candles Loaded: {:?}",
                heartbeats.get(&market.market_name).unwrap().candles.len()
            );
            let first_trade: PrIdTi = self.calculate_sync_end(market).await;
            println!("Sync End: {:?}", first_trade.dt);
            // Fill the trades and aggregate to candles for start to end period
            self.fill_trades_and_make_candles(market, heartbeats, first_trade)
                .await;
            // Complete the heartbeat for the market by resampling off the base market tf candles
            self.complete_heartbeat(market, heartbeats).await;
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
                    println!("Metric: {}\t{}", m.tf, m.datetime);
                }
            }
        }
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
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        // Set the min date that is needed for start - 92 days prior
        let sync_start = self.start_dt.duration_trunc(Duration::days(1)).unwrap()
            - Duration::days(self.sync_days);
        // let mut last_trade: Option<PrIdTi> = None;
        // let mut candles = Vec::new();
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hm| hm.ts = sync_start);
        println!(
            "{} - Initialize start to {} days prior.",
            market.market_name, self.sync_days
        );
        println!("{} - Sync Start: {}", market.market_name, sync_start);
        // Try checking the mcd record for last candle and confirming candles are in candles table
        self.try_mcd_start(market, heartbeats).await;
        println!(
            "{} MCD start complete. {} candles loaded.",
            market.market_name,
            heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&market.tf)
                .unwrap()
                .len()
        );
        // Start now equals the last trade captured in the last candle. Try mtd and try eld
        // Will need to check if the id field is Some or None to determine start dr for
        // trades or candles
        println!(
            "New Sync Start: {}\tLast Trade: {}",
            heartbeats.get(&market.market_name).unwrap().ts,
            heartbeats.get(&market.market_name).unwrap().last
        );
        // Try checking the mtd record for last trades and confirming trade table/trades exist
        self.try_mtd_start(market, heartbeats).await;
        println!(
            "{} MTD start complete. Total {} candles.",
            market.market_name,
            heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&market.tf)
                .unwrap()
                .len()
        );
        // Start now equals the last trade captured in candles made from the trade tables.
        println!(
            "{} New Sync Start: {}\tLast Trade: {}",
            market.market_name,
            heartbeats.get(&market.market_name).unwrap().ts,
            heartbeats.get(&market.market_name).unwrap().last
        );
        // Try checking system candles table and last candle
        self.try_eld_start(market, heartbeats).await;
        println!(
            "{} Eld start complete. Total {} candles.",
            market.market_name,
            heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&market.tf)
                .unwrap()
                .len()
        );
        // Start now equals the last trade captured in candles from the production table.
        println!(
            "{} New Sync Start: {}\tLast Trade: {}",
            market.market_name,
            heartbeats.get(&market.market_name).unwrap().ts,
            heartbeats.get(&market.market_name).unwrap().last
        );

        // If at this point there are no candles, calc the start based on exchange logic
        if heartbeats
            .get(&market.market_name)
            .unwrap()
            .candles
            .get(&market.tf)
            .unwrap()
            .is_empty()
        {
            match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.use_ftx_start(market, heartbeats).await;
                }
                ExchangeName::Gdax => {
                    self.use_gdax_start(market, heartbeats).await;
                }
            }
        }
    }

    // Try to determine the start dt for sync by checking the mcd record for the market. The mcd
    // will list the valid candle end dt if it exists. If this dt is greater than the given start dt
    // return the mcd last dt and the candles from start to last resampled in the market time frame
    // as these candles have been validated and archived.
    async fn try_mcd_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        println!("Trying MCD start.");
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        // Check for mcd
        let mcd = MarketCandleDetail::select(&self.pools[&Database::ElDorado], market).await;
        match mcd {
            Ok(m) => {
                if m.last_trade_ts > heartbeats.get(&market.market_name).unwrap().ts {
                    // Validate the data from the mcd record
                    self.use_mcd_start(&db, market, &m, heartbeats).await;
                } else {
                    // The archive market candles end before the request start and are of no use
                    println!("MCD ends before sync start");
                    println!(
                        "MCD end: {}\tSync start: {}",
                        m.last_trade_ts,
                        heartbeats.get(&market.market_name).unwrap().ts
                    );
                }
            }
            // No mcd exists, so no archive candles to use
            Err(sqlx::Error::RowNotFound) => {
                println!("No MCD exists.");
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
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        println!("Using MCD start.");
        // Select candles
        let archive_candles = ResearchCandle::select_dr(
            &self.pools[db],
            market,
            &mcd.time_frame,
            &heartbeats.get(&market.market_name).unwrap().ts,
            &(mcd.last_candle + mcd.time_frame.as_dur()),
        )
        .await
        .expect("Failed to select research candles by dr.");
        // Validate & Resample candles if the market time frame is longer than research base of S15
        if archive_candles.is_empty() {
            // Could not select any candles, panic as there is a data integrity issue
            panic!(
                "Excpected mcd candles for sync. Select dr of {:?} to {:?} returned None.",
                heartbeats.get(&market.market_name).unwrap().ts,
                mcd.last_candle + mcd.time_frame.as_dur()
            );
        } else if &archive_candles.first().unwrap().datetime
            > &heartbeats
                .get(&market.market_name)
                .unwrap()
                .ts
                .max(mcd.first_candle)
        {
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
                heartbeats
                    .get(&market.market_name)
                    .unwrap()
                    .ts
                    .max(mcd.first_candle),
                &(mcd.last_candle + mcd.time_frame.as_dur()),
                archive_candles.first().unwrap().datetime
            );
        } else if market.tf == mcd.time_frame {
            // Candles are already in time frame. Convert the candles to prod candles and add to hb.
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.candles.entry(market.tf).and_modify(|v| {
                        v.append(
                            &mut archive_candles
                                .iter()
                                .map(|c| c.as_production_candle())
                                .collect(),
                        );
                    });
                });
        } else {
            // Resample candles to market time frame and convert to prod candles.
            println!(
                "Resampling MCD candles from {} to {}.",
                mcd.time_frame, market.tf
            );
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.candles.entry(market.tf).and_modify(|v| {
                        v.append(
                            &mut self.resample_and_convert_research_candles_by_hashmap_v2(
                                &archive_candles,
                                &market.tf,
                            ),
                        );
                    });
                });
        };
        // Return the prepped candles ready for sync and new sync start and pridti
        let last = heartbeats
            .get(&market.market_name)
            .unwrap()
            .candles
            .get(&market.tf)
            .unwrap()
            .last()
            .unwrap()
            .clone();
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| {
                hb.ts = last.datetime + market.tf.as_dur();
                hb.last = last.close_as_pridti();
            });
    }

    // Try to determine start dt for sync by checking the mtd record for the market. The mtd will
    // list the last trade date that was validated with a trade file archived. If this date is
    // greater than the give start date, load all the trades for the days starting at the start dt
    // and ending at the last valid trade date from the mtd as these trades have been validated.
    // Create production candles from the trades and return with the updated start
    async fn try_mtd_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        println!("Trying MTD start.");
        let mtd = MarketTradeDetail::select(&self.pools[&Database::ElDorado], market).await;
        match mtd {
            Ok(m) => {
                if let Some(ntd) = m.next_trade_day {
                    // There is a next trade day, check if relevant to start date. ie if sync start
                    // is 08/12/2020 00:00 (it is a even date as it is only either a MCD or 92 day)
                    if ntd > heartbeats.get(&market.market_name).unwrap().ts {
                        // Validated trades available, convert to candles and use them
                        self.use_mtd_start(market, &m, heartbeats).await;
                    } else {
                        // The last validated trade date is less than current start, not useful
                        println!("MTD ends before sync start.");
                        println!(
                            "MTD next trade day: {}\tSync start: {}",
                            ntd,
                            heartbeats.get(&market.market_name).unwrap().ts
                        );
                    }
                };
            }
            // No mtd exists, so no archive trades to use
            Err(sqlx::Error::RowNotFound) => {
                println!("No MTD exists.");
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
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        println!("Using MTD start.");
        // Create date range for days to load and make candles from
        let dr = self.create_date_range(
            &heartbeats.get(&market.market_name).unwrap().ts,
            &mtd.next_trade_day.unwrap(),
            &TimeFrame::D01,
        );
        println!(
            "MTD Date Range: {} to {}",
            dr.first().unwrap(),
            dr.last().unwrap()
        );
        let mut last_trade =
            if heartbeats.get(&market.market_name).unwrap().last.dt == DateTime::<Utc>::MIN_UTC {
                None
            } else {
                Some(heartbeats.get(&market.market_name).unwrap().last.clone())
            };
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
                        heartbeats
                            .entry(market.market_name.clone())
                            .and_modify(|hb| {
                                hb.candles.entry(market.tf).and_modify(|v| {
                                    v.append(&mut c);
                                });
                            });
                    }
                    None => break,
                }
            } else {
                println!("Trade table does not exist. Exiting MTD start.");
                break;
            };
        }
        if !heartbeats
            .get(&market.market_name)
            .unwrap()
            .candles
            .get(&market.tf)
            .unwrap()
            .is_empty()
        {
            // Return the prepped candles ready for sync and new sync start and pridti
            let last = heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&market.tf)
                .unwrap()
                .last()
                .unwrap()
                .clone();
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.ts = last.datetime + market.tf.as_dur();
                    hb.last = last.close_as_pridti();
                });
        }
    }

    // Try to determine the dt for sync by checking for candles that were created from running the
    // instance. The start will have already checked for validated candles and made candles from any
    // validated trades. At this point check for the unvalidated candles in the system from the last
    // time it was run.
    async fn try_eld_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        println!("Trying ElD start.");
        // Check if production candle table exists for market
        if self
            .candle_table_exists(market, &market.tf, &CandleType::Production)
            .await
        {
            self.use_eld_start(market, heartbeats).await;
        } else {
            // No production candles, create the candle table and return None
            println!("ElD production candles table does not exist. Creating table.");
            let db = match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
                ExchangeName::Gdax => Database::Gdax,
            };
            ProductionCandle::create_table(&self.pools[&db], market, &market.tf)
                .await
                .expect("Failed to create candle table.");
        }
    }

    // Select all the production candles greater than the start date and return with updated PrIdTi
    async fn use_eld_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        // Clean up production candles - remove any 0 volume candles
        let last_non_zero_candle = ProductionCandle::select_last_non_zero(&self.pools[&db], market)
            .await
            .expect("Failed to select.");
        ProductionCandle::delete_gt_dt(&self.pools[&db], market, &last_non_zero_candle.datetime)
            .await
            .expect("Failed to delete candle.");
        let mut candles = ProductionCandle::select_gte_dt(
            &self.pools[&db],
            market,
            &heartbeats.get(&market.market_name).unwrap().ts,
        )
        .await
        .expect("Failed to select candles.");
        if !candles.is_empty() {
            let last = candles.last().unwrap().clone();
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.candles.entry(market.tf).and_modify(|v| {
                        v.append(&mut candles);
                    });
                    hb.ts = last.datetime + market.tf.as_dur();
                    hb.last = last.close_as_pridti();
                });
        }
    }

    // Calculate the time and id for the ftx trade that corresponds to 92 days prior to today
    // as there are no research candles, validated trades or production candles to start from.
    // FTX API uses date time as field so the first day that has trades is the first day to use
    async fn use_ftx_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        let end_dt = Utc::now().duration_trunc(Duration::days(1)).unwrap();
        let mut start_dt = heartbeats.get(&market.market_name).unwrap().ts;
        while start_dt < end_dt {
            // Only request 1 per second to avoid 429 errors
            println!("Checking Ftx for trades on {}", start_dt);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let trades = self.clients[&market.exchange_name]
                .get_ftx_trades(market.market_name.as_str(), Some(1), None, Some(start_dt))
                .await
                .expect("Failed to get ftx trades.");
            if trades.is_empty() {
                start_dt += Duration::days(1)
            } else {
                println!("Trades exist. Use start of day.");
                break;
            };
        }
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| hb.ts = start_dt);
    }

    // GDAX trade API uses trade id for pagination instead of date. In order to determine the trade
    // id that started 92 days ago this function uses a binary search algorithm to find a trade on
    // the given day 92 days ago, then refines it further to find the last trade of the previous
    // say to give the exact trade id to start syncing from
    async fn use_gdax_start(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
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
        while trade.time.duration_trunc(Duration::days(1)).unwrap()
            != heartbeats.get(&market.market_name).unwrap().ts
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
            if trade.time < heartbeats.get(&market.market_name).unwrap().ts {
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
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| {
                hb.ts =
                    last_trade.dt.duration_trunc(market.tf.as_dur()).unwrap() + market.tf.as_dur();
                hb.last = last_trade;
            });
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
        heartbeats: &mut HashMap<String, Heartbeat>,
        first_trade: PrIdTi,
    ) {
        // Create date range for days to fill
        let dr_start = heartbeats
            .get(&market.market_name)
            .unwrap()
            .ts
            .duration_trunc(Duration::days(1))
            .unwrap();
        let dr = self.create_date_range(
            &dr_start,
            &(first_trade.dt.duration_trunc(Duration::days(1)).unwrap() + Duration::days(1)),
            &TimeFrame::D01,
        );
        // Fill trades for each day
        for d in dr.iter() {
            println!("Filling trades and making candles for date: {}", d);
            self.fill_trades_and_make_candles_for_dt(market, heartbeats, &first_trade, d)
                .await;
        }
    }

    async fn fill_trades_and_make_candles_for_dt(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
        first_trade: &PrIdTi,
        dt: &DateTime<Utc>,
    ) {
        // Create trade table
        self.create_trade_table(market, *dt).await;
        match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                panic!("FTX not supported.");
            }
            ExchangeName::Gdax => {
                // Fill trades for day
                let interval_start = heartbeats.get(&market.market_name).unwrap().ts.max(*dt);
                let interval_end = first_trade.dt.min(*dt + Duration::days(1));
                let trades: Vec<GdaxTrade> = self
                    .get_gdax_trades_for_interval_forward(
                        market,
                        &interval_start,
                        &interval_end,
                        heartbeats.get(&market.market_name).unwrap().last.id as i32,
                        &heartbeats.get(&market.market_name).unwrap().last.dt,
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
                    &Some(heartbeats.get(&market.market_name).unwrap().last),
                    &interval_start,
                    &interval_end,
                    &trades,
                );
                if let Some(mut cs) = candles {
                    println!("Inserting {} candles into prod db table.", cs.len());
                    for candle in cs.iter() {
                        candle
                            .insert(&self.pools[&Database::Gdax], market, &market.tf)
                            .await
                            .expect("Failed to insert candle.");
                    }
                    if !cs.is_empty() {
                        let last = cs.last().unwrap().clone();
                        heartbeats
                            .entry(market.market_name.clone())
                            .and_modify(|hb| {
                                hb.candles.entry(market.tf).and_modify(|v| {
                                    v.append(&mut cs);
                                });
                                hb.ts = last.datetime + market.tf.as_dur();
                                hb.last = last.close_as_pridti();
                            });
                    }
                }
            }
        }
    }

    // Take the candles created in the fill and sync functions and create the current heartbeat
    // for the market
    async fn complete_heartbeat(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) {
        // Create hashmap for timeframes and their candles
        let last = heartbeats
            .get(&market.market_name)
            .unwrap()
            .candles
            .get(&market.tf)
            .unwrap()
            .last()
            .expect("Expected at least one candle.")
            .clone();
        let last_ts = last.datetime;
        println!("Last TS: {}", last_ts);
        for tf in TimeFrame::tfs().iter().skip(1) {
            // Filter candles from floor of new timeframe
            // TF Prev is last divisible tf to use as base resample. This improves performance from
            // resampling from base tf each time and prevents resample of non-divisible tf
            // (ie resampleing T05 from T03 candles)
            let last_candle_prev = heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&tf.resample_from())
                .unwrap()
                .last()
                .unwrap()
                .datetime;
            let filtered_candles: Vec<_> = heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&tf.resample_from())
                .unwrap()
                .iter()
                .filter(|c| {
                    c.datetime
                        < (last_candle_prev + tf.resample_from().as_dur())
                            .duration_trunc(tf.as_dur())
                            .unwrap()
                })
                .cloned()
                .collect();
            let capacity = tf.per_day() * (self.sync_days + 10);
            heartbeats
                .entry(market.market_name.clone())
                .and_modify(|hb| {
                    hb.candles
                        .insert(*tf, Vec::with_capacity(capacity as usize));
                    hb.candles.entry(*tf).and_modify(|v| {
                        v.append(&mut self.resample_production_candles(&filtered_candles, tf))
                    });
                });
        }
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| {
                hb.ts = last.datetime;
                hb.last = last.close_as_pridti();
            });
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
    pub async fn fill(&self, market: &Option<MarketDetail>, automate: bool) {
        // Eval params to determine to run and whether to automate or get manual inputs
        let markets = match market {
            Some(m) => {
                // Validate that market given is eligible for fill
                if self.validate_market_eligible_for_fill(m).await {
                    Some([m.clone()].to_vec())
                } else {
                    None
                }
            }
            None => self.select_markets_eligible_for_fill().await,
        };
        // Check if there are markets to fill
        if let Some(m) = markets {
            // For each market: fill back than forward until either fully synced or manual
            // input to stop
            for market in m.iter() {
                // Fill the market backward than forward until either the current date or
                // there is a manual rejection or if automated - a validation fails
                self.fill_market(market, &automate).await;
            }
        }
    }

    async fn validate_market_eligible_for_fill(&self, market: &MarketDetail) -> bool {
        // Market detail must contain a last candle value - meaning that it has been initialized
        // and run. The market must also be active.
        if market.status != MarketStatus::Active || market.last_candle.is_none() {
            println!(
                "{} not eligible for fill. Status: {:?}\tLast Candle: {:?}",
                market.market_name, market.status, market.last_candle
            );
            false
        } else {
            // Check that candles and trades schema is created
            println!("Creating candle and trade schemas if it does not exist.");
            match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.create_candles_schema(&self.pools[&Database::Ftx])
                        .await
                        .expect("Failed to create candle schema.");
                    self.create_trades_schema(&self.pools[&Database::Ftx])
                        .await
                        .expect("Failed to create ftx/ftxus trade schema.");
                }
                ExchangeName::Gdax => {
                    self.create_candles_schema(&self.pools[&Database::Gdax])
                        .await
                        .expect("Failed to create candle schema.");
                    self.create_trades_schema(&self.pools[&Database::Gdax])
                        .await
                        .expect("Failed to create gdax trade schema.");
                }
            };
            // Check that production candles table is created
            println!("Checking production candle tables are created.");
            if !self
                .candle_table_exists(market, &market.tf, &CandleType::Production)
                .await
            {
                println!("Creating production candle table.");
                let db = match market.exchange_name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
                    ExchangeName::Gdax => Database::Gdax,
                };
                ProductionCandle::create_table(&self.pools[&db], market, &market.tf)
                    .await
                    .expect("Failed to create candle table.");
            } else {
                println!("Table exists.");
            }
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
                let input: Option<String> = ElDorado::get_input_with_timer(
                    15,
                    "Volume does not match, what do you want to do? Enter 'Y' to use qc trades:",
                )
                .await;
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
                    "Forwardfill {} for {:?}. ARCHIVE: Move trade file to archives.",
                    market.market_name, mtd.next_trade_day
                );
                // Move file to archive
                let path = self.fill_forward_archive_move_file(market, &d);
                // Update archive candles table if within 100 days (only keep 100 days in db, any
                // more stored in flat files for research and backtesting. 100 days needed for
                // current price metric calculations. reduces db size)
                self.fill_forward_archive_process_candles(market, &d, &path)
                    .await;
                // Cleanup tables
                self.fill_forward_archive_cleaup_tables(market, &d).await;
                // Update mtd to the next day with the last trade price and id
                self.fill_forward_archive_update_mtd(market, mtd, &d, &path)
                    .await;
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
        // Move trade file to validated location if it is not already there
        if f_path.exists() && !a_path.exists() {
            std::fs::rename(f_path, &a_path).expect("Failed to copy file to trade folder.");
        } else if a_path.exists() {
            // File already archived
            println!("File already archived")
        } else {
            panic!("File not found, does not exist in initial or final location.");
        }
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
                let trades = self.read_gdax_trades_from_file_into_vec(pb);
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
        // Delete candles older than 100 days in the db - research
        let cutoff = Utc::now()
            .duration_trunc(Duration::days(1))
            .expect("Failed to trunc date.")
            - Duration::days(self.sync_days);
        let db = match market.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => Database::Ftx,
            ExchangeName::Gdax => Database::Gdax,
        };
        ResearchCandle::delete_lt_dt(&self.pools[&db], market, &TimeFrame::S15, &cutoff)
            .await
            .expect("Failed to delete candles.");
        // Delete candles older than the day in the db - production
        ProductionCandle::delete_lt_dt(
            &self.pools[&db],
            market,
            &market.tf,
            &(*dt + Duration::days(1)),
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
        // Drop the legacy trade and qc tables - remove once prod is cleaned up
        let table_pre = format!(
            "public.trades_{}_{}",
            market.exchange_name.as_str(),
            market.as_strip()
        );
        let tables = [
            "_rest",
            "_ws",
            "_validated",
            "_archived",
            "_processed",
            "_qc",
            "_qc_auto",
        ];
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
        let bf_table = format!(
            "public.trades_{}_{}_bf_{}",
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        let sql = format!(
            r#"
            DROP TABLE IF EXISTS {}
            "#,
            bf_table
        );
        sqlx::query(&sql)
            .execute(&self.pools[&db])
            .await
            .expect("Failed to drop table.");
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
            std::fs::rename(bf_path, fb).expect("Failed to copy file from location 3.");
        };
        // Try location 2
        let bad_archive_path = format!("{}/csv/gdax/gdax", &self.storage_path,);
        let bf_path = Path::new(&bad_archive_path).join(f);
        if bf_path.exists() {
            println!("File in location 2. Moving to correct location.");
            // Create directories in the correct location
            std::fs::create_dir_all(path).expect("Failed to create directories.");
            // File exists but in wrong location - copy to correct location
            std::fs::rename(bf_path, fb).expect("Failed to copy file from location 2.");
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
                self.process_fill_backward_validation_gdax(market, mtd, validated, &trades)
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
                self.process_fill_backward_archive_gdax(market, mtd, &trades)
                    .await;
            }
        }
    }

    async fn process_fill_backward_archive_gdax(
        &self,
        market: &MarketDetail,
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
                // Create the research candle table
                ResearchCandle::create_table(&self.pools[&Database::Gdax], market, &TimeFrame::S15)
                    .await
                    .expect("Failed to create table.")
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
        market: &MarketDetail,
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
                // Create the research candle table
                ResearchCandle::create_table(&self.pools[&Database::Gdax], market, &TimeFrame::S15)
                    .await
                    .expect("Failed to create table.");
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
        let f = format!("{}_{}.csv", market.as_strip(), dt.format("%F"));
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

#[cfg(test)]
mod tests {
    use crate::candles::ResearchCandle;
    use csv::Reader;
    use std::{fs::File, path::PathBuf};

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
    pub fn _read_sample_research_candles(pb: &PathBuf) -> Vec<ResearchCandle> {
        let file = File::open(pb).expect("Failed to open file.");
        let mut candles = Vec::new();
        let mut rdr = Reader::from_reader(file);
        for result in rdr.deserialize() {
            let record: ResearchCandle = result.expect("Failed to deserialize record.");
            candles.push(record);
        }
        candles
    }

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
}
