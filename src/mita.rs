use crate::candles::select_last_candle;
use crate::configuration::{get_configuration, Settings};
use crate::exchanges::{fetch_exchanges, Exchange};
use crate::markets::{select_market_detail_by_exchange_mita, MarketDetail};
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;

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
        self.sync().await;
        // Loop forever making a new candle at each new interval
    }

    pub async fn sync(&self) {
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
            // Create and save any candles necessary
            if start != end {}
            // Update mita heartbeat interval
            //self.update_market_heartbeat(market, end);
            // Clean up tables
        }
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
