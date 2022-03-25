use crate::candles::TimeFrame;
use crate::candles::{resample_candles, select_candles_gte_datetime, select_last_candle, Candle};
use crate::configuration::{get_configuration, Settings};
use crate::events::insert_event_process_trades;
use crate::exchanges::{select_exchanges, Exchange, ExchangeName};
use crate::markets::{
    select_market_detail_by_exchange_mita, update_market_data_status, MarketDetail, MarketStatus,
};
use crate::metrics::{delete_metrics_ap_by_exchange_market, insert_metric_ap, MetricAP};
use crate::trades::{
    insert_delete_ftx_trades, insert_delete_gdax_trades, select_ftx_trades_by_time,
    select_gdax_trades_by_time, select_insert_drop_trades,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Mita {
    pub settings: Settings,
    pub markets: Vec<MarketDetail>,
    pub exchange: Exchange,
    pub ed_pool: PgPool,
    pub trade_pool: PgPool,
    pub restart: bool,
    pub last_restart: DateTime<Utc>,
    pub restart_count: i8,
    pub hbtf: TimeFrame,
}

#[derive(Debug)]
pub struct Heartbeat {
    pub ts: DateTime<Utc>,
    pub last: Decimal,
    pub candles: HashMap<TimeFrame, Vec<Candle>>,
}

impl Mita {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = get_configuration().expect("Failed to read configuration.");
        // Create db connection with pgpool
        let ed_pool = PgPool::connect_with(settings.ed_db.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        // Get exchange details
        let exchanges = select_exchanges(&ed_pool)
            .await
            .expect("Could not select exchanges from db.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .into_iter()
            .find(|e| e.name.as_str() == settings.application.exchange)
            .unwrap();
        // Create db pool for trades
        let trade_pool = match exchange.name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                PgPool::connect_with(settings.ftx_db.with_db())
                    .await
                    .expect("Failed to connect to trade database.")
            }
            ExchangeName::Gdax => PgPool::connect_with(settings.gdax_db.with_db())
                .await
                .expect("Failed to connect to trade database."),
        };
        // Get market details assigned to mita
        let markets = select_market_detail_by_exchange_mita(
            &ed_pool,
            &exchange.name,
            &settings.application.droplet,
        )
        .await
        .expect("Could not select market details from exchange.");
        Self {
            settings,
            markets,
            exchange,
            ed_pool,
            trade_pool,
            restart: true,
            last_restart: Utc::now(),
            restart_count: 0,
            hbtf: TimeFrame::time_frames()[0], // Sets hb to lowest tf
        }
    }

    pub async fn process_restart(&self) -> Duration {
        // Get sleep time from current restart count
        let mut sleep_duration = match self.restart_count {
            0 => tokio::time::Duration::from_secs(5),
            1 => tokio::time::Duration::from_secs(30),
            _ => tokio::time::Duration::from_secs(60),
            // 3 => tokio::time::Duration::from_secs(300),
            // 4 => tokio::time::Duration::from_secs(300),
            // _ => tokio::time::Duration::from_secs(300),
        };
        // Get time from last restart
        let time_since_last_restart = Utc::now() - self.last_restart;
        // If time from last restart is more than 24 hours then overwrite sleep
        if time_since_last_restart > Duration::days(1) {
            sleep_duration = tokio::time::Duration::from_secs(5);
        };
        // Sleep for duration
        println!("Sleeping for {:?} before restarting.", sleep_duration);
        tokio::time::sleep(sleep_duration).await;
        // Return time since last restart
        time_since_last_restart
    }

    pub async fn run(&self) -> bool {
        // Backfill trades from last candle to first trade of live stream
        self.historical("stream").await;
        // Sync from last candle to current stream last trade
        println!("Starting sync.");
        let mut heartbeats = self.sync().await;
        // Set events heartbeat ts
        let mut events_ts = Utc::now();
        // Loop forever making a new candle at each new interval
        // println!("Heartbeats: {:?}", heartbeats);
        println!("Starting MITA loop.");
        loop {
            // Set loop timestamp
            let timestamp = Utc::now().duration_trunc(self.hbtf.as_dur()).unwrap();
            // For each market, check if loop timestamp > heartbeat
            for market in self.markets.iter() {
                if timestamp > heartbeats[&market.market_name.as_str()].ts + self.hbtf.as_dur() {
                    println!(
                        "New heartbeat interval. Create candle for {}. HB: {:?}, TS: {:?}",
                        market.market_name,
                        heartbeats[&market.market_name.as_str()].ts,
                        timestamp,
                    );
                    let start = heartbeats[&market.market_name.as_str()].ts;
                    match self
                        .process_interval(
                            start,
                            timestamp,
                            market,
                            &heartbeats[&market.market_name.as_str()],
                        )
                        .await
                    {
                        Some(h) => heartbeats.insert(market.market_name.as_str(), h),
                        None => continue,
                    };
                    println!(
                        "New heartbeats for {}:  {:?} {:?}",
                        market.market_name,
                        heartbeats[market.market_name.as_str()].ts,
                        heartbeats[market.market_name.as_str()].last
                    );
                }
            }
            // Process any events for the droplet mita
            let events_processed = self.process_events(events_ts).await;
            if events_processed {
                events_ts = Utc::now();
            };
            // Reload heartbeats if needed (ie when a candle validation is updated)
            // Sleep for 200 ms to give control back to tokio scheduler
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    }

    pub async fn sync(&self) -> HashMap<&str, Heartbeat> {
        // Initiate heartbeat interval map
        let mut heartbeats = HashMap::new();
        for market in self.markets.iter() {
            // Update status to sync
            update_market_data_status(
                &self.ed_pool,
                &market.market_id,
                &MarketStatus::Sync,
                self.settings.application.ip_addr.as_str(),
            )
            .await
            .expect("Could not update market status.");
            // Get start time for candle sync
            let start = match select_last_candle(
                &self.ed_pool,
                &self.exchange.name,
                &market.market_id,
                self.hbtf,
            )
            .await
            {
                Ok(c) => c.datetime + self.hbtf.as_dur(),
                Err(_) => panic!("Sqlx Error getting start time in sync."),
            };
            // Get current hb floor for end time of sync
            let now = Utc::now();
            let end = now.duration_trunc(self.hbtf.as_dur()).unwrap();
            println!(
                "Syncing {} from {:?} to {:?}",
                market.market_name, start, now
            );
            // Migrate rest trades to ws
            select_insert_drop_trades(
                &self.trade_pool,
                &self.exchange.name,
                market,
                start,
                now,
                "rest",
                "ws",
            )
            .await
            .expect("Failed to select insert drop ftx trades.");
            // Create and save any candles necessary
            if start != end {
                // Create candles from ws table
                // Get trades to sync. There has to be at least one trade because the historical
                // fill needs a ws trade to start the backfill function.
                match self.exchange.name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        self.sync_ftx_trades(market, start, end).await
                    }
                    ExchangeName::Gdax => self.sync_gdax_trades(market, start, end).await,
                };
            };
            // Create heartbeat
            let heartbeat = self.create_heartbeat(market).await;
            // Update mita heartbeat interval
            heartbeats.insert(market.market_name.as_str(), heartbeat);
            // Delete metrics for market from table
            delete_metrics_ap_by_exchange_market(&self.ed_pool, &self.exchange.name, market)
                .await
                .expect("Failed to delete metrics.");
            // Create metrics for all time frames
            let mut metrics: Vec<MetricAP> = Vec::new();
            for tf in TimeFrame::time_frames().iter() {
                let mut new_metrics = MetricAP::new(
                    &market.market_name,
                    &self.exchange.name,
                    *tf,
                    &heartbeats[market.market_name.as_str()].candles[tf],
                );
                metrics.append(&mut new_metrics);
            }
            for metric in metrics.iter() {
                insert_metric_ap(&self.ed_pool, metric)
                    .await
                    .expect("Failed to insert metric.");
            }
            // Update status to sync
            update_market_data_status(
                &self.ed_pool,
                &market.market_id,
                &MarketStatus::Active,
                self.settings.application.ip_addr.as_str(),
            )
            .await
            .expect("Could not update market status.");
        }
        heartbeats
    }

    pub async fn sync_ftx_trades(
        &self,
        market: &MarketDetail,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) {
        let sync_trades = select_ftx_trades_by_time(
            &self.trade_pool,
            &self.exchange.name,
            market,
            "ws",
            start,
            end,
        )
        .await
        .expect("Failed to select ws trades.");
        // Get date range
        let date_range = self.create_date_range(start, end, self.hbtf.as_dur());
        // Make new candles
        let candles = self
            .create_interval_candles(market, date_range, &sync_trades)
            .await;
        println!("Created {} candles to insert into db.", candles.len());
        // Insert candles to db
        self.insert_candles(market, candles).await;
        // Move trades from ws to processed and delete from ws
        insert_delete_ftx_trades(
            &self.trade_pool,
            &self.exchange.name,
            market,
            start,
            end,
            "ws",
            "processed",
            sync_trades,
        )
        .await
        .expect("Failed to insert delete ftx trades.");
    }

    pub async fn sync_gdax_trades(
        &self,
        market: &MarketDetail,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) {
        let sync_trades = select_gdax_trades_by_time(
            &self.trade_pool,
            &self.exchange.name,
            market,
            "ws",
            start,
            end,
        )
        .await
        .expect("Failed to select ws trades.");
        // Get date range
        let date_range = self.create_date_range(start, end, self.hbtf.as_dur());
        // Make new candles
        let candles = self
            .create_interval_candles(market, date_range, &sync_trades)
            .await;
        println!("Created {} candles to insert into db.", candles.len());
        // Insert candles to db
        self.insert_candles(market, candles).await;
        // Move trades from ws to processed and delete from ws
        insert_delete_gdax_trades(
            &self.trade_pool,
            &self.exchange.name,
            market,
            start,
            end,
            "ws",
            "processed",
            sync_trades,
        )
        .await
        .expect("Failed to insert delete ftx trades.");
    }

    pub async fn process_interval(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        market: &MarketDetail,
        heartbeat: &Heartbeat,
    ) -> Option<Heartbeat> {
        // println!("Process interval start: {:?}", Utc::now());
        // Get trades
        let mut new_candles = match self.exchange.name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                let trades = select_ftx_trades_by_time(
                    &self.trade_pool,
                    &self.exchange.name,
                    market,
                    "ws",
                    start,
                    end,
                )
                .await
                .expect("Failed to select ftx ws trades.");
                // If no trades return without updating hashmap
                if trades.is_empty() {
                    // TODO: Consider returning candle forward filled from last and updating hb
                    return None;
                } else {
                    // Get date range
                    let date_range =
                        self.create_date_range(start + self.hbtf.as_dur(), end, self.hbtf.as_dur());
                    println!("Date Range: {:?}", date_range);
                    self.create_interval_candles(market, date_range, &trades)
                        .await
                }
            }
            ExchangeName::Gdax => {
                let trades = select_gdax_trades_by_time(
                    &self.trade_pool,
                    &self.exchange.name,
                    market,
                    "ws",
                    start,
                    end,
                )
                .await
                .expect("Failed to select gdax ws trades.");
                // If no trades return without updating hashmap
                if trades.is_empty() {
                    // TODO: Consider returning candle forward filled from last and updating hb
                    return None;
                } else {
                    // Get date range
                    let date_range =
                        self.create_date_range(start + self.hbtf.as_dur(), end, self.hbtf.as_dur());
                    println!("Date Range: {:?}", date_range);
                    self.create_interval_candles(market, date_range, &trades)
                        .await
                }
            }
        };
        let n = new_candles.len();
        // println!("{} new candles: {:?}", n, new_candles);
        // Set last and heartbeat time
        let last = new_candles.last().unwrap().close;
        let ts = new_candles.last().unwrap().datetime;
        // Create hash map of candles for new heartbeat
        let mut map_candles = HashMap::new();
        // Insert hbft candles - clone the current heartbeat which will be dropped when replaced
        let mut candles = heartbeat.candles[&TimeFrame::T15].clone();
        candles.append(&mut new_candles);
        map_candles.insert(TimeFrame::T15, candles);
        // Insert metrics for hbtf
        let mut metrics = MetricAP::new(
            &market.market_name,
            &market.exchange_name,
            TimeFrame::T15,
            &map_candles[&TimeFrame::T15],
        );
        // For each time frame either append new candle and insert or insert existing candles
        for tf in TimeFrame::time_frames().iter().skip(1) {
            if heartbeat.candles[tf].last().unwrap().datetime + tf.as_dur()
                < end.duration_trunc(tf.as_dur()).unwrap()
            {
                // Resample trades for new candles and append
                let new_candles: Vec<Candle> = map_candles[&self.hbtf]
                    .iter()
                    .filter(|c| {
                        c.datetime >= heartbeat.candles[tf].last().unwrap().datetime + tf.as_dur()
                            && c.datetime < end.duration_trunc(tf.as_dur()).unwrap()
                    })
                    .cloned()
                    .collect();
                let mut resampled_candles =
                    resample_candles(market.market_id, &new_candles, tf.as_dur());
                // println!(
                //     "New {} tf resampled candles: {:?}",
                //     tf.as_str(),
                //     resampled_candles.len()
                // );
                let mut candles = heartbeat.candles[tf].clone();
                candles.append(&mut resampled_candles);
                // Calc metrics on new candle vec
                let mut more_metrics =
                    MetricAP::new(&market.market_name, &market.exchange_name, *tf, &candles);
                metrics.append(&mut more_metrics);
                map_candles.insert(*tf, candles);
            } else {
                // No new candles to resample, clone existing candles
                let candles = heartbeat.candles[tf].clone();
                map_candles.insert(*tf, candles);
            }
        }
        // Insert new candles
        let new_candles = &map_candles[&self.hbtf][map_candles[&self.hbtf].len() - n..];
        // println!("New hb candles to insert: {:?}", new_candles);
        self.insert_candles(market, new_candles.to_vec()).await;
        // Insert new metrics
        for metric in metrics.iter() {
            insert_metric_ap(&self.ed_pool, metric)
                .await
                .expect("Failed to insert metric ap.");
        }
        // Insert new processing event
        insert_event_process_trades(
            &self.ed_pool,
            &self.settings.application.droplet,
            start,
            end,
            market,
        )
        .await
        .expect("Failed in insert event - process interval.");
        // Return new heartbeat
        Some(Heartbeat {
            ts,
            last,
            candles: map_candles,
        })
    }

    pub async fn create_heartbeat(&self, market: &MarketDetail) -> Heartbeat {
        // Get candles from the past 97 days. Metrics are calculated with a DON lbp of 192 max for
        // timeframes of 12H. 192 / 12H = 96 days. Add one to get clean calcs.
        let candles = select_candles_gte_datetime(
            &self.ed_pool,
            &market.exchange_name,
            &market.market_id,
            Utc::now() - Duration::days(97),
        )
        .await
        .expect("Failed to select candles.");
        // Set last and heartbeat time
        let last = candles.last().unwrap().close;
        let ts = candles.last().unwrap().datetime;
        // Create map for each time frame
        let mut map_candles = HashMap::new();
        map_candles.insert(TimeFrame::T15, candles);
        for tf in TimeFrame::time_frames().iter().skip(1) {
            // Filter candles from floor of new timeframe
            let filtered_candles: Vec<Candle> = map_candles[&tf.prev()]
                .iter()
                .filter(|c| c.datetime < ts.duration_trunc(tf.as_dur()).unwrap())
                .cloned()
                .collect();
            // Resample candles to timeframe
            let resampled_candles =
                resample_candles(market.market_id, &filtered_candles, tf.as_dur());
            map_candles.insert(*tf, resampled_candles);
        }
        Heartbeat {
            ts,
            last,
            candles: map_candles,
        }
    }

    pub fn create_date_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        duration: Duration,
    ) -> Vec<DateTime<Utc>> {
        // Takes a start and end time and creates a vec of dates
        let mut dr_start = start;
        let mut date_range = Vec::new();
        while dr_start < end {
            date_range.push(dr_start);
            dr_start = dr_start + duration;
        }
        date_range
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_new_mita() {
        let mita = Mita::new().await;
        println!("Mita: {:?}", mita);
    }

    #[tokio::test]
    async fn create_heartbeat_returns_resampled_candles() {
        let mita = Mita::new().await;
        for market in mita.markets.iter() {
            let heartbeat = mita.create_heartbeat(market).await;
            println!("Heartbeat for {:?}", market.market_name);
            println!("TS {:?}", heartbeat.ts);
            println!("Last {:?}", heartbeat.last);
            for (k, v) in heartbeat.candles.iter() {
                println!("K: {:?} L: {:?}", k, v.last().unwrap());
            }
        }
    }
}
