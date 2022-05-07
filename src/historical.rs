use crate::candles::{
    insert_candle, select_last_candle, select_previous_candle, Candle, TimeFrame,
};
use crate::events::{Event, EventStatus, EventType};
use crate::exchanges::{client::RestClient, error::RestError, Exchange, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{update_market_data_status, MarketDetail, MarketStatus, MarketTradeDetail};
use crate::mita::Mita;
use crate::trades::{
    insert_delete_ftx_trades, insert_delete_gdax_trades, insert_ftx_trades, insert_gdax_trades,
    select_ftx_trades_by_time, select_gdax_trades_by_time, select_trade_first_stream,
};
use crate::utilities::{get_input, Trade};
use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use std::convert::TryInto;

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
                    &self.exchange.name,
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
    pub async fn backfill(&self) {
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
        let exchange: String = get_input("Enter Exchange to Backfill: ");
        let exchange: ExchangeName = exchange.try_into().unwrap();
        if !self.exchanges.iter().any(|e| e.name == exchange) {
            // Exchange inputed is not part of existing exchanges
            println!("{:?} has not been added to El-Dorado.", exchange);
            println!("Available exchanges are: {:?}", self.list_exchanges());
            return;
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
                return;
            }
        };
        // Validate market is eligible to backfill
        // 1) Market detail contains a last candle - ie it has been live in El-Dorado
        // 2) Market Trade Archive record exists and status is not Complete
        // 3) Backfill event does not exist
        if market.market_status != MarketStatus::Active || market.last_candle.is_none() {
            println!(
                "Market not eligible for backfill. \nStatus: {:?}\nLast Candle: {:?}",
                market.market_status, market.last_candle
            );
            return;
        };
        let market_trade_details = MarketTradeDetail::select_all(&self.ig_pool)
            .await
            .expect("Failed to select market trade details.");
        let mtd = match market_trade_details
            .iter()
            .find(|mtd| mtd.market_id == market.market_id)
        {
            Some(mtd) => mtd.clone(),
            None => {
                // No trade archive exists, create one
                let mtd = MarketTradeDetail::new(&self.ig_pool, &market).await;
                mtd.insert(&self.ig_pool)
                    .await
                    .expect("Failed to insert market trade detail.");
                mtd
            }
        };
        let events = Event::select_by_status_type(
            &self.ig_pool,
            &EventStatus::Open,
            &EventType::BackfillTrades,
        )
        .await
        .expect("Failed to select backfill event.");
        let event = match events.iter().find(|e| e.market_id == market.market_id) {
            Some(e) =>
            // There exists an event, start there
            {
                Some(e.clone())
            }
            None => {
                // There is no event, create one then start here
                Event::new_backfill_trades(&mtd)
            }
        };
        match event {
            Some(e) => match e.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => e
                    .insert(&self.ig_pool)
                    .await
                    .expect("Failed to insert event."),
                ExchangeName::Gdax => self.process_event_backfill_trades(&e).await,
            },
            None => {
                println!("Market not eligible for backfill event.");
                println!(
                    "Prev Status: {:?}\nNext Status: {:?}",
                    mtd.previous_status, mtd.next_status
                );
            }
        }
    }

    pub async fn process_event_backfill_trades(&self, event: &Event) {}

    pub async fn process_ftx_backfill_event() {
        // Create trade table for market / exch / day
        // Fill trade table with trades
        // Validate trades
        // Archive trades
        // Drop trade table
        // Update database - markets / market_trades
        // Close event
        // Create event for previous day if trades exist
    }

    pub async fn process_gdax_backfill_event() {}
}

#[cfg(test)]
mod tests {
    use crate::configuration::get_configuration;
    use crate::exchanges::{client::RestClient, ExchangeName};
    use crate::markets::select_market_details;
    use crate::mita::Mita;
    use sqlx::PgPool;

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
}
