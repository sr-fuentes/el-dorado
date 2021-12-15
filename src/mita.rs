use crate::candles::Candle;
use crate::candles::{insert_candle, select_last_candle, select_previous_candle};
use crate::configuration::{get_configuration, Settings};
use crate::exchanges::{fetch_exchanges, ftx::Trade, Exchange};
use crate::markets::{select_market_detail_by_exchange_mita, MarketDetail};
use crate::trades::{
    delete_ftx_trades_by_time, drop_ftx_trade_table, insert_ftx_trades, select_ftx_trades_by_time,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Mita {
    pub settings: Settings,
    pub markets: Vec<MarketDetail>,
    pub exchange: Exchange,
    pub pool: PgPool,
    pub restart: bool,
    pub last_restart: DateTime<Utc>,
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
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not select exchanges from db.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .into_iter()
            .find(|e| e.exchange_name == settings.application.exchange)
            .unwrap();
        // Get market details assigned to mita
        let markets = select_market_detail_by_exchange_mita(
            &pool,
            &exchange.exchange_name,
            &settings.application.droplet,
        )
        .await
        .expect("Could not select market details from exchange.");
        Self {
            settings,
            markets,
            exchange,
            pool,
            restart: false,
            last_restart: Utc::now(),
        }
    }

    pub async fn run(&self) {
        // Backfill trades from last candle to first trade of live stream
        self.historical("stream").await;
        // Sync from last candle to current stream last trade
        println!("Starting sync.");
        let map_heartbeats = self.sync().await;
        // Loop forever making a new candle at each new interval
        println!("Heartbeats: {:?}", map_heartbeats);
    }

    pub async fn sync(&self) -> HashMap<&str, DateTime<Utc>> {
        // Initiate heartbeat interval map
        let mut map_heartbeats = HashMap::new();
        for market in self.markets.iter() {
            // Get start time for candle sync
            let start = match select_last_candle(
                &self.pool,
                &self.exchange.exchange_name,
                &market.market_id,
            )
            .await
            {
                Ok(c) => c.datetime + Duration::seconds(900),
                Err(_) => panic!("Sqlx Error getting start time in sync."),
            };
            // Get current hb floor for end time of sync
            let end = Utc::now().duration_trunc(Duration::seconds(900)).unwrap();
            println!("Syncing {} from {:?} to {:?}", market.market_name, start, end);
            // Migrate rest trades to ws
            let rest_trades = select_ftx_trades_by_time(
                &self.pool,
                &self.exchange.exchange_name,
                market.strip_name().as_str(),
                "rest",
                start,
                end,
            )
            .await
            .expect("Could not select ftx trades.");
            insert_ftx_trades(
                &self.pool,
                &market.market_id,
                &self.exchange.exchange_name,
                market.strip_name().as_str(),
                "ws",
                rest_trades,
            )
            .await
            .expect("Could not insert into ws trades.");
            // Drop rest table
            drop_ftx_trade_table(
                &self.pool,
                &self.exchange.exchange_name,
                market.strip_name().as_str(),
                "rest",
            )
            .await
            .expect("Could not drop rest table.");
            // Create and save any candles necessary
            if start != end {
                // Create candles from ws table
                // Get trades to sync. There has to be at least one trade because the historical
                // fill needs a ws trade to start the backfill function.
                let sync_trades = select_ftx_trades_by_time(
                    &self.pool,
                    &self.exchange.exchange_name,
                    market.strip_name().as_str(),
                    "ws",
                    start,
                    end,
                )
                .await
                .expect("Could not select ws trades.");
                // Get date range
                let mut dr_start = start;
                let mut date_range = Vec::new();
                while dr_start <= end {
                    date_range.push(dr_start);
                    dr_start = dr_start + Duration::seconds(900);
                }
                // Create vec of candles for date range
                let mut previous_candle = select_previous_candle(
                    &self.pool,
                    &self.exchange.exchange_name,
                    &market.market_id,
                    start,
                )
                .await
                .expect("No previous candle.");
                let candles = date_range.iter().fold(Vec::new(), |mut v, d| {
                    let mut filtered_trades: Vec<Trade> = sync_trades
                        .iter()
                        .filter(|t| t.time.duration_trunc(Duration::seconds(900)).unwrap() == *d)
                        .cloned()
                        .collect();
                    let new_candle = match filtered_trades.len() {
                        0 => {
                            // Get previous candle and forward fill from close
                            Candle::new_from_last(
                                market.market_id,
                                *d,
                                previous_candle.close,
                                previous_candle.last_trade_ts,
                                &previous_candle.last_trade_id.to_string(),
                            )
                        }
                        _ => {
                            filtered_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                            filtered_trades.dedup_by(|t1, t2| t1.id == t2.id);
                            Candle::new_from_trades(market.market_id, *d, &filtered_trades)
                        }
                    };
                    previous_candle = new_candle.clone();
                    v.push(new_candle);
                    v
                });
                println!("Created {} candles to insert into db.", candles.len());
                // Insert candles to db
                for candle in candles.into_iter() {
                    insert_candle(
                        &self.pool,
                        &self.exchange.exchange_name,
                        &market.market_id,
                        candle,
                        false,
                    )
                    .await
                    .expect("Could not insert new candle.");
                }
                // Move trades from ws to processed and delete from ws
                delete_ftx_trades_by_time(
                    &self.pool,
                    &self.exchange.exchange_name,
                    market.strip_name().as_str(),
                    "ws",
                    start,
                    end,
                )
                .await
                .expect("Could not delete trades from db.");
                insert_ftx_trades(
                    &self.pool,
                    &market.market_id,
                    &self.exchange.exchange_name,
                    market.strip_name().as_str(),
                    "processed",
                    sync_trades,
                )
                .await
                .expect("Could not insert processed trades.");
            }
            // Update mita heartbeat interval
            map_heartbeats.insert(market.market_name.as_str(), end);
            // Clean up tables
        }
        map_heartbeats
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
