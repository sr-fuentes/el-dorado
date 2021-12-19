use crate::candles::create_exchange_candle_table;
use crate::configuration::*;
use crate::markets::{fetch_markets, insert_new_market, pull_usd_markets_from_ftx};
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

pub async fn add(pool: &PgPool, config: &Settings) {
    // Get input from user for exchange to add
    // TODO: implemenet new and parse functions for Exchange and
    // parse / validated input
    let exchange: String = get_input("Enter Exchange to Add:");
    let exchange = Exchange {
        id: Uuid::new_v4(),
        name: exchange.try_into().expect("Failed to parse exchange."),
    };

    // Set list of supported exchanges
    let supported_exchanges = ["ftx", "ftxus"];

    // Get list of exchanges from db
    let exchanges = fetch_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");

    // Compare input to existing exchanges in db and add if new
    if exchanges.iter().any(|e| e.name == exchange.name) {
        println!(
            "{:?} has already been added and is in the database.",
            exchange.name
        );
        return;
    } else if !supported_exchanges.contains(&exchange.name.as_str()) {
        // Not in supported exchanges
        println!("{:?} is not a supported exchange.", exchange.name);
        return;
    } else {
        println!("Adding {:?} to the database.", exchange);
        insert_new_exchange(pool, &exchange)
            .await
            .expect("Failed to insert new exchange.");
    }

    // Fetch markets from new exchange
    let markets = match exchange.name {
        ExchangeName::Ftx | ExchangeName::FtxUs => pull_usd_markets_from_ftx(&exchange.name).await,
    };
    let markets = match markets {
        Ok(markets) => markets,
        Err(err) => {
            println!("Could not fetch markets from new exchange, try to refresh later.");
            println!("Err: {:?}", err);
            return;
        }
    };

    // Fetch existing markets for exchange
    // There should be none but this function can be used to refresh markets too
    let market_ids = fetch_markets(pool, &exchange)
        .await
        .expect("Could not fetch exchanges.");
    println!("Markets pull from db: {:?}", market_ids);

    // Insert market into db if not already there
    for market in markets.iter() {
        if market_ids.iter().any(|m| m.market_name == market.name) {
            println!("{} already in markets table.", market.name);
        } else {
            insert_new_market(pool, &exchange, market, config.application.ip_addr.as_str())
                .await
                .expect("Failed to insert market.");
        }
    }

    // Create candle table for exchange
    create_exchange_candle_table(pool, &exchange.name.as_str())
        .await
        .expect("Could not create candle table.");
}

pub async fn fetch_exchanges(pool: &PgPool) -> Result<Vec<Exchange>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Exchange,
        r#"
        SELECT exchange_id as id, exchange_name as "name: ExchangeName"
        FROM exchanges
        "#
    )
    .fetch_all(pool)
    .await?;
    println!("Rows: {:?}", rows);
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

        let exchanges = fetch_exchanges(&connection_pool)
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
        let markets = fetch_markets(&connection_pool, &exchange)
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
