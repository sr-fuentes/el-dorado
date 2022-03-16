use crate::{
    candles::TimeFrame,
    configuration::{get_configuration, Settings},
    exchanges::{client::RestClient, ExchangeName},
    validation::ValidationStatus,
};
use sqlx::PgPool;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Inquisidor {
    pub settings: Settings,
    pub ig_pool: PgPool,
    pub ftx_pool: PgPool,
    pub gdax_pool: PgPool,
    pub clients: HashMap<ExchangeName, RestClient>,
    pub hbtf: TimeFrame,
}

impl Inquisidor {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = get_configuration().expect("Failed to read configuration.");
        // Create db connection with pgpool
        let ig_pool = PgPool::connect_with(settings.ftx_db.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        let ftx_pool = PgPool::connect_with(settings.ftx_db.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        let gdax_pool = PgPool::connect_with(settings.gdax_db.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        let mut clients = HashMap::new();
        clients.insert(ExchangeName::Ftx, RestClient::new(&ExchangeName::Ftx));
        clients.insert(ExchangeName::FtxUs, RestClient::new(&ExchangeName::FtxUs));
        clients.insert(ExchangeName::Gdax, RestClient::new(&ExchangeName::Gdax));
        Self {
            settings,
            ig_pool,
            ftx_pool,
            gdax_pool,
            clients,
            hbtf: TimeFrame::time_frames()[0],
        }
    }

    pub async fn run(&self) {
        // Check that a daily event to create candles for each exchange exists and create one if it
        // does not then start loop to process events and validations.
        self.set_initial_event().await;
        println!("Starting INQUI loop.");
        loop {
            // Process any events for ig
            self.process_events().await;
            // Process any validation events
            self.process_candle_validations(ValidationStatus::New).await;
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
        let ftx_trades = &ig.clients[&ExchangeName::Ftx]
            .get_ftx_trades("BTC/USD", Some(5), None, None)
            .await
            .expect("Failed to get trades.");
        let ftxus_trades = &ig.clients[&ExchangeName::FtxUs]
            .get_ftx_trades("BTC/USD", Some(5), None, None)
            .await
            .expect("Failed to get trades.");
        println!("FTX Trades: {:?}", ftx_trades);
        println!("FTX Trades: {:?}", ftxus_trades);
    }
}
