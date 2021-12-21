use crate::candles::create_exchange_candle_table;
use crate::inquisidor::Inquisidor;
use crate::utilities::get_input;
use chrono::Utc;
use sqlx::PgPool;
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

pub mod ftx;

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
pub struct Exchange {
    pub id: Uuid,
    pub name: ExchangeName,
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum ExchangeName {
    Ftx,
    FtxUs,
}

impl ExchangeName {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeName::Ftx => "ftx",
            ExchangeName::FtxUs => "ftxus",
        }
    }
}

impl TryFrom<String> for ExchangeName {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "ftx" => Ok(Self::Ftx),
            "ftxus" => Ok(Self::FtxUs),
            other => Err(format!("{} is not a supported exchange.", other)),
        }
    }
}

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

impl Inquisidor {
    pub async fn add_new_exchange(&self) {
        // Get user input for exchange to add
        let exchange: String = get_input("Enter Exchange to Add:");
        // Parse input to see if there is a valid exchange
        let exchange: ExchangeName = exchange.try_into().unwrap();
        // Get current exchanges from db
        let exchanges = select_exchanges(&self.pool)
            .await
            .expect("Could not fetch exchanges.");
        // Compare input to existing exchanges
        if exchanges.iter().any(|e| e.name == exchange) {
            // Exchange already exists in db. Return command.
            println!("{:?} has all ready been added to El-Dorado.", exchange);
            return;
        }
        // Add new exchange to db.
        println!("Adding {:?} to El-Dorado.", exchange);
        let new_exchange = Exchange {
            id: Uuid::new_v4(),
            name: exchange,
        };
        insert_new_exchange(&self.pool, &new_exchange)
            .await
            .expect("Failed to insert new exchange.");
        // Refresh markets for new exchange (should insert all)
        self.refresh_exchange_markets(&new_exchange.name).await;
        // Create candle table for exchange
        create_exchange_candle_table(&self.pool, new_exchange.name.as_str())
            .await
            .expect("Failed to create exchange table.");
    }
}

pub async fn select_exchanges(pool: &PgPool) -> Result<Vec<Exchange>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Exchange,
        r#"
        SELECT exchange_id as id, exchange_name as "name: ExchangeName"
        FROM exchanges
        "#
    )
    .fetch_all(pool)
    .await?;
    // println!("Rows: {:?}", rows);
    Ok(rows)
}

pub async fn insert_new_exchange(pool: &PgPool, exchange: &Exchange) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO exchanges (
            exchange_id, exchange_name, exchange_rank, is_spot, is_derivitive, 
            exchange_status, added_date, last_refresh_date)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        exchange.id,
        exchange.name.as_str(),
        1,
        true,
        false,
        "New",
        Utc::now(),
        Utc::now()
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::ftx::*;
    use crate::markets::select_market_ids_by_exchange;
    use crate::trades::{create_ftx_trade_table, insert_ftx_trades};

    #[tokio::test]
    async fn fetch_exchanges_returns_all_exchanges() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let connection_pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        let exchanges = select_exchanges(&connection_pool)
            .await
            .expect("Failed to load exchanges.");

        println!("Exchanges: {:?}", exchanges);
    }

    #[test]
    fn check_if_exchanges_already_exists_returns_true() {
        // Arrange
        let exchange1 = Exchange {
            id: Uuid::new_v4(),
            name: String::from("ftxus").try_into().unwrap(),
        };
        let exchange2 = Exchange {
            id: Uuid::new_v4(),
            name: String::from("ftx").try_into().unwrap(),
        };
        let mut exchanges = Vec::<Exchange>::new();
        exchanges.push(exchange1);
        exchanges.push(exchange2);

        let dup_exchange = Exchange {
            id: Uuid::new_v4(),
            name: String::from("ftxus").try_into().unwrap(),
        };

        // Assert
        assert!(exchanges.iter().any(|e| e.name == dup_exchange.name));
    }

    #[tokio::test]
    async fn create_dynamic_exchange_tables_works() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let connection_pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Create exchange struct
        let exchange = Exchange {
            id: Uuid::new_v4(),
            name: String::from("ftxus").try_into().unwrap(),
        };

        // Set market for test
        let markets = select_market_ids_by_exchange(&connection_pool, &exchange.name)
            .await
            .expect("Failed to fetch markets.");
        let market = markets
            .iter()
            .find(|m| m.market_name == "BTC/USD")
            .expect("Failed to grab BTC/USD market.");

        // Set table name variables
        let market_table_name = market.market_name.replace(&['/', '-'][..], "");
        let test_trade_table = "dynamic_test";

        // Create db tables
        create_ftx_trade_table(
            &connection_pool,
            &exchange.name.as_str(),
            &market_table_name,
            &test_trade_table,
        )
        .await
        .expect("Failed to create tables.");

        // Create rest client
        let client = RestClient::new_us();

        // Get last 10 BTC/USD trades
        let trades = client
            .get_trades(&market.market_name, Some(10), None, Some(Utc::now()))
            .await
            .expect("Failed to get last 10 BTC/USD trades.");

        insert_ftx_trades(
            &connection_pool,
            &market.market_id,
            &exchange.name.as_str(),
            &market_table_name,
            &test_trade_table,
            trades,
        )
        .await
        .expect("Failed to insert trades.");
    }
}
