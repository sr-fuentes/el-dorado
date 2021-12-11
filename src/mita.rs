use crate::configuration::{Settings, get_configuration};
use crate::markets::{MarketDetail, select_market_detail_by_exchange_mita};
use crate::exchanges::{Exchange, fetch_exchanges};
use sqlx::PgPool;

#[derive(Debug)]
pub struct Mita {
    pub settings: Settings,
    pub markets: Vec<MarketDetail>,
    pub exchange: Exchange,
    pub pool: PgPool,
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
        let exchanges = fetch_exchanges(&pool).await.expect("Could not select exchanges from db.");
        // Match exchange to exchanges in database
        let exchange = exchanges.into_iter().find(|e| e.exchange_name == settings.application.exchange)
        .unwrap();
        // Get market details assigned to mita
        let markets = select_market_detail_by_exchange_mita(&pool, &exchange.exchange_name, &settings.application.droplet)
            .await
            .expect("Could not select market details from exchange.");
        Self {settings, markets, exchange, pool}
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