use crate::{configuration::Database, eldorado::ElDorado};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

pub mod client;
pub mod error;
pub mod ftx;
pub mod gdax;
pub mod kraken;
pub mod ws;

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
pub struct Exchange {
    pub id: Uuid,
    pub name: ExchangeName,
    pub rank: i32,
    pub is_spot: bool,
    pub is_derivitive: bool,
    pub status: ExchangeStatus,
    pub added_dt: DateTime<Utc>,
    pub last_refresh_dt: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, sqlx::Type, Hash, Serialize, Deserialize)]
#[sqlx(rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum ExchangeName {
    Ftx,
    FtxUs,
    Gdax,
    Kraken,
    Hyperliquid,
    Drift,
    Mango,
    Dydx,
}

impl ExchangeName {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeName::Ftx => "ftx",
            ExchangeName::FtxUs => "ftxus",
            ExchangeName::Gdax => "gdax",
            ExchangeName::Kraken => "kraken",
            ExchangeName::Hyperliquid => "hyperliquid",
            ExchangeName::Drift => "drift",
            ExchangeName::Mango => "mango",
            ExchangeName::Dydx => "dydx",
        }
    }
}

impl TryFrom<String> for ExchangeName {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "ftx" => Ok(Self::Ftx),
            "ftxus" => Ok(Self::FtxUs),
            "gdax" => Ok(Self::Gdax),
            "kraken" => Ok(Self::Kraken),
            "hyperliquid" => Ok(Self::Ftx),
            "drift" => Ok(Self::FtxUs),
            "mango" => Ok(Self::Gdax),
            "dydx" => Ok(Self::Kraken),
            other => Err(format!("{} is not a supported exchange.", other)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum ExchangeStatus {
    New,
    Active,
    Terminated,
}

impl ExchangeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeStatus::New => "new",
            ExchangeStatus::Active => "active",
            ExchangeStatus::Terminated => "terminated",
        }
    }
}

impl TryFrom<String> for ExchangeStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "active" => Ok(Self::Active),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported exchange status.", other)),
        }
    }
}

impl Exchange {
    // Used for testing - pass the given exchange and it will return a new exchange struct
    // with New Status and current dates
    pub fn new_sample_exchange(exchange: &ExchangeName) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: *exchange,
            rank: 1,
            is_spot: true,
            is_derivitive: true,
            status: ExchangeStatus::Active,
            added_dt: Utc::now(),
            last_refresh_dt: Utc::now(),
        }
    }
    pub async fn select_by_status(
        pool: &PgPool,
        status: ExchangeStatus,
    ) -> Result<Vec<Exchange>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT exchange_id as id, exchange_name as "name: ExchangeName",
            exchange_rank as rank,
            is_spot,
            is_derivitive,
            exchange_status as "status: ExchangeStatus",
            added_date as added_dt,
            last_refresh_date as last_refresh_dt
            FROM exchanges
            WHERE exchange_status = $1
            "#,
            status.as_str(),
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }
}

impl ElDorado {
    // Get user input for market, then validate against either all markets in the db or against
    // the vec of markets passed in as a param
    pub async fn prompt_exchange_input(&self) -> Option<Exchange> {
        // Get user input
        let exchange: String = ElDorado::get_input("Please enter exchange: ").await;
        // Parse input to check if it is a valid exchange name
        let exchange: ExchangeName = match exchange.try_into() {
            Ok(x) => x,
            Err(e) => panic!("Could not parse exchange name. {:?}", e),
        };
        // Match against available exchanges for instance
        let active_exchanges =
            Exchange::select_by_status(&self.pools[&Database::ElDorado], ExchangeStatus::Active)
                .await
                .expect("Failed to select exchanges from db.");
        active_exchanges.into_iter().find(|e| e.name == exchange)
    }

    // Get an exchange input and refresh the markets for the exchange
    pub async fn refresh_exchange(&self) {
        match self.prompt_exchange_input().await {
            Some(e) => match e.name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    println!("FTX / FTXUS API no longer active.")
                }
                ExchangeName::Gdax => self.refresh_gdax_markets().await,
                ExchangeName::Kraken => self.refresh_kraken_markets().await,
                name => panic!("{:?} not supported for exchange refresh.", name),
            },
            None => println!("No exchange to refresh."),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        configuration::get_configuration,
        exchanges::{Exchange, ExchangeName, ExchangeStatus},
    };
    use sqlx::PgPool;

    #[tokio::test]
    async fn select_exchanges_returns_all_exchanges() {}

    #[test]
    fn check_if_exchanges_already_exists_returns_true() {
        // Arrange
        let exchange1 = Exchange::new_sample_exchange(&ExchangeName::Ftx);
        let exchange2 = Exchange::new_sample_exchange(&ExchangeName::FtxUs);
        let mut exchanges = Vec::<Exchange>::new();
        exchanges.push(exchange1);
        exchanges.push(exchange2);

        let dup_exchange = Exchange::new_sample_exchange(&ExchangeName::FtxUs);

        // Assert
        assert!(exchanges.iter().any(|e| e.name == dup_exchange.name));
    }

    #[tokio::test]
    async fn select_exchanges_by_status_works() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        let exchanges = Exchange::select_by_status(&pool, ExchangeStatus::New)
            .await
            .expect("Failed to select exchanges.");
        println!("Exchanges: {:?}", exchanges);
    }
}
