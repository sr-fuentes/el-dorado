use crate::{
    configuration::{get_configuration, Settings},
    exchanges::{ftx::RestClient, ExchangeClient},
};
use chrono::{Duration, DurationRound, Utc};
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Inquisidor {
    pub settings: Settings,
    pub pool: PgPool,
    pub clients: HashMap<String, ExchangeClient>,
}

impl Inquisidor {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = get_configuration().expect("Failed to read configuration.");
        // Create db connection with pgpool
        let pool = PgPool::connect_with(settings.database.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        let mut clients = HashMap::new();
        clients.insert(
            String::from("ftx"),
            ExchangeClient::Ftx(RestClient::new_intl()),
        );
        clients.insert(
            String::from("ftxus"),
            ExchangeClient::FtxUs(RestClient::new_us()),
        );
        Self {
            settings,
            pool,
            clients,
        }
    }

    pub async fn run(&self) {
        // Create heartbeat set to current 15 minute floor + 30 seconds. The 30 seconds is to allow
        // for candles to be created on the interval and for them to settle, then to start
        // validations without straining the database.
        let mut heartbeat =
            Utc::now().duration_trunc(Duration::seconds(900)).unwrap() + Duration::seconds(30);
        println!("Starting INQUI loop.");
        loop {
            // Set loop timestamp
            let timestamp =
                Utc::now().duration_trunc(Duration::seconds(900)).unwrap() + Duration::seconds(30);
            if timestamp > heartbeat {
                // Current time is greater than heartbeat which means we are in a new interval.
                // Check for candles to validated
                println!("New heartbeat interval. Validate candles.");
                self.validate_candles().await;
                // Set heartbeat to new interval
                heartbeat = timestamp;
                println!("New heartbeat: {:?}", heartbeat);
            }
            // Process any validation events
            self.process_candle_validations().await;
            // Sleep for 200 ms to give control back to tokio scheduler
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_new_inquisidor() {
        let ig = Inquisidor::new().await;
        println!("Inquisidor: {:?}", ig);
    }

    #[tokio::test]
    async fn access_inqui_clients() {
        let ig = Inquisidor::new().await;
        println!("Inquisidor: {:?}", ig);
        let trades = if let ExchangeClient::Ftx(c) = &ig.clients[&String::from("ftx")] {
            c.get_trades("BTC/USD", Some(5), None, None)
                .await
                .expect("Failed to get trades.")
        } else {
            panic!()
        };
        println!("Trades: {:?}", trades);
    }
}
