use crate::candles::TimeFrame;
use crate::candles::{Candle, resample_candles, select_candles_gte_datetime, select_last_candle};
use crate::configuration::{get_configuration, Settings};
use crate::events::insert_event_process_trades;
use crate::exchanges::{select_exchanges, Exchange};
use crate::markets::{
    select_market_detail_by_exchange_mita, update_market_data_status, MarketDetail, MarketStatus,
};
use crate::metrics::{insert_metric_ap, MetricAP};
use crate::trades::{
    insert_delete_ftx_trades, select_ftx_trades_by_time, select_insert_drop_trades,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;
use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
pub struct Mita {
    pub settings: Settings,
    pub markets: Vec<MarketDetail>,
    pub exchange: Exchange,
    pub pool: PgPool,
    pub restart: bool,
    pub last_restart: DateTime<Utc>,
    pub restart_count: i8,
    pub hbtf: TimeFrame,
}

pub struct Heartbeat {
    pub ts: DateTime<Utc>,
    pub candles: HashMap<TimeFrame, VecDeque<Candle>>,
}

impl Mita {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = get_configuration().expect("Failed to read configuration.");
        // Create db connection with pgpool
        let pool = PgPool::connect_with(settings.database.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        // Get exchange details
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not select exchanges from db.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .into_iter()
            .find(|e| e.name.as_str() == settings.application.exchange)
            .unwrap();
        // Get market details assigned to mita
        let markets = select_market_detail_by_exchange_mita(
            &pool,
            &exchange.name,
            &settings.application.droplet,
        )
        .await
        .expect("Could not select market details from exchange.");
        Self {
            settings,
            markets,
            exchange,
            pool,
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
            2 => tokio::time::Duration::from_secs(60),
            3 => tokio::time::Duration::from_secs(300),
            4 => tokio::time::Duration::from_secs(900),
            _ => tokio::time::Duration::from_secs(1800),
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
        // Loop forever making a new candle at each new interval
        println!("Heartbeats: {:?}", heartbeats);
        println!("Starting MITA loop.");
        loop {
            // Set loop timestamp
            let timestamp = Utc::now().duration_trunc(self.hbtf.as_dur()).unwrap();
            // For each market, check if loop timestamp > heartbeat
            for market in self.markets.iter() {
                if timestamp > heartbeats[&market.market_name.as_str()] {
                    println!(
                        "New heartbeat interval. Create candle for {}",
                        market.market_name
                    );
                    let start = heartbeats[&market.market_name.as_str()];
                    let new_heartbeat = self.process_interval(start, timestamp, market).await;
                    heartbeats.insert(market.market_name.as_str(), new_heartbeat);
                    println!("New heartbeats: {:?}", heartbeats);
                }
            }
            // Process any events for the droplet mita
            self.process_events().await;
            // Reload heartbeats if needed (ie when a candle validation is updated)
            // Sleep for 200 ms to give control back to tokio scheduler
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }

    pub async fn sync(&self) -> HashMap<&str, DateTime<Utc>> {
        // Initiate heartbeat interval map
        let mut heartbeats = HashMap::new();
        for market in self.markets.iter() {
            // Update status to sync
            update_market_data_status(
                &self.pool,
                &market.market_id,
                &MarketStatus::Sync,
                self.settings.application.ip_addr.as_str(),
            )
            .await
            .expect("Could not update market status.");
            // Get start time for candle sync
            let start = match select_last_candle(
                &self.pool,
                self.exchange.name.as_str(),
                &market.market_id,
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
                &self.pool,
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
                let sync_trades = select_ftx_trades_by_time(
                    &self.pool,
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
                    &self.pool,
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
            // Update mita heartbeat interval
            heartbeats.insert(market.market_name.as_str(), end);
            // Update status to sync
            update_market_data_status(
                &self.pool,
                &market.market_id,
                &MarketStatus::Active,
                self.settings.application.ip_addr.as_str(),
            )
            .await
            .expect("Could not update market status.");
        }
        heartbeats
    }

    pub async fn process_interval(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        market: &MarketDetail,
    ) -> DateTime<Utc> {
        println!("Process interval start: {:?}", Utc::now());
        // Get trades
        let trades =
            select_ftx_trades_by_time(&self.pool, &self.exchange.name, market, "ws", start, end)
                .await
                .expect("Failed to select ftx ws trades.");
        // If no trades return without updating hashmap
        if trades.is_empty() {
            start
        } else {
            // Get date range
            let date_range = self.create_date_range(start, end, self.hbtf.as_dur());
            // Make new candles
            let mut new_candles = self
                .create_interval_candles(market, date_range, &trades)
                .await;
            let n = new_candles.len();
            // Calc new metrics
            let mut candles = select_candles_gte_datetime(
                &self.pool,
                &market.exchange_name,
                &market.market_id,
                start - Duration::days(91),
            )
            .await
            .expect("Failed to select candles.");
            // Append newly created candles
            candles.append(&mut new_candles);
            println!("Got candles, calc metrics: {:?}", Utc::now());
            // Insert metrics
            let mut metrics = MetricAP::new(
                &market.market_name,
                &market.exchange_name,
                TimeFrame::T15,
                &candles,
            );
            // Check remaining timeframes for interval processing
            for tf in TimeFrame::time_frames().iter().skip(1) {
                if end <= end.duration_trunc(tf.as_dur()).unwrap() {
                    // Resample to new time frame
                    let resampled_candles =
                        resample_candles(market.market_id, &candles, tf.as_dur());
                    let mut more_metrics = MetricAP::new(
                        &market.market_name,
                        &market.exchange_name,
                        *tf,
                        &resampled_candles,
                    );
                    metrics.append(&mut more_metrics);
                }
            }
            // Insert new candles
            let new_candles = &candles[candles.len() - n..];
            self.insert_candles(market, new_candles.to_vec()).await;
            // Insert new metrics
            for metric in metrics.iter() {
                insert_metric_ap(&self.pool, metric)
                    .await
                    .expect("Failed to insert metric ap.");
            }
            // Insert new processing event
            insert_event_process_trades(
                &self.pool,
                &self.settings.application.droplet,
                start,
                end,
                market,
            )
            .await
            .expect("Failed in insert event - process interval.");
            // Return new heartbeat
            end
        }
    }

    fn create_date_range(
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
}
