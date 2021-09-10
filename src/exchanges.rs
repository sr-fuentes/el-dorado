use crate::markets::{fetch_markets, insert_new_market, pull_usd_markets_from_ftx};
use crate::utilities::get_input;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub mod ftx;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Exchange {
    pub exchange_id: Uuid,
    pub exchange_name: String,
}

pub async fn add(pool: &PgPool) {
    // Get input from user for exchange to add
    // TODO: implemenet new and parse functions for Exchange and
    // parse / validated input
    let exchange: String = get_input("Enter Exchange to Add:");
    let exchange = Exchange {
        exchange_id: Uuid::new_v4(),
        exchange_name: exchange,
    };

    // Get list of exchanges from db
    let exchanges = fetch_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");

    // Compare input to existing exchanges in db and add if new
    if exchanges
        .iter()
        .any(|e| e.exchange_name == exchange.exchange_name)
    {
        println!(
            "{:?} has already been added and is in the database.",
            exchange.exchange_name
        );
        return;
    } else {
        println!("Adding {:?} to the database.", exchange);
        insert_new_exchange(pool, &exchange)
            .await
            .expect("Failed to insert new exchange.");
    }

    // Fetch markets from new exchange
    let markets = match exchange.exchange_name.as_str() {
        "ftxus" => pull_usd_markets_from_ftx("ftxus").await, // fetch ftxus markets,
        "ftx" => pull_usd_markets_from_ftx("ftx").await,     // fetch ftx markets,
        _ => {
            println!(
                "{:?} exchange not yet supported.",
                exchange.exchange_name.as_str()
            );
            return;
        }
    };
    let markets = match markets {
        Ok(markets) => markets,
        Err(err) => {
            println!("Could not fetch markets from new exchange, try to refresh later.");
            println!("Err: {:?}", err);
            return;
        }
    };
    // println!("Markets pulled from exchange: {:?}.", markets);

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
            insert_new_market(pool, &exchange, &market)
                .await
                .expect("Failed to insert market.");
        }
    }

    // Create tables for new exchanges (trades, candles, etc)
    create_exchange_tables(pool, &exchange)
        .await
        .expect(&format!(
            "Could not create new tables for exchange {}.",
            exchange.exchange_name
        ));
}

pub async fn fetch_exchanges(pool: &PgPool) -> Result<Vec<Exchange>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Exchange,
        r#"
        SELECT exchange_id, exchange_name
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
        exchange.exchange_id,
        exchange.exchange_name,
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

pub async fn create_exchange_tables(pool: &PgPool, exchange: &Exchange) -> Result<(), sqlx::Error> {
    // Create trades, trades_ws, and candles tables
    let tables = ["est", "ws", "validated"];
    for table in tables {
        let sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS trades_{}_{} (
                market_id uuid NOT NULL,
                trade_id BIGINT NOT NULL,
                price NUMERIC NOT NULL,
                size NUMERIC NOT NULL,
                side TEXT NOT NULL,
                liquidation BOOLEAN,
                time timestamptz NOT NULL)
            "#,
            exchange.exchange_name, table
        );
        sqlx::query(&sql).execute(pool).await?;
    }
    let sql = format!(
        r#"
            CREATE TABLE IF NOT EXISTS candles_15T_{} (
                datetime timestamptz NOT NULL,
                PRIMARY KEY (datetime),
                open FLOAT NOT NULL,
                high FLOAT NOT NULL,
                low FLOAT NOT NULL,
                close FLOAT NOT NULL,
                volume FLOAT NOT NULL,
                volume_net FLOAT NOT NULL,
                volume_liquidation FLOAT NOT NULL,
                volume_liquidation_net FLOAT NOT NULL,
                value FLOAT NOT NULL,
                trade_count BIGINT NULL,
                liquidation_count BIGINT NOT NULL,
                last_trade_ts timestamptz NOT NULL,
                last_trade_id TEXT NOT NULL,
                candle_status TEXT NOT NULL
            )
        "#,
        exchange.exchange_name
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::ftx::*;
    use crate::historical::insert_ftxus_trades;

    #[tokio::test]
    async fn fetch_exchanges_returns_all_exchanges() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let connection_pool = PgPool::connect(&configuration.database.connection_string())
            .await
            .expect("Failed to connect to Postgres.");

        let _ = fetch_exchanges(&connection_pool)
            .await
            .expect("Failed to load exchanges.");
    }

    #[test]
    fn check_if_exchanges_already_exists_returns_true() {
        // Arrange
        let exchange1 = Exchange {
            exchange_id: Uuid::new_v4(),
            exchange_name: "ftxus".to_string(),
        };
        let exchange2 = Exchange {
            exchange_id: Uuid::new_v4(),
            exchange_name: "ftx".to_string(),
        };
        let mut exchanges = Vec::<Exchange>::new();
        exchanges.push(exchange1);
        exchanges.push(exchange2);

        let dup_exchange = Exchange {
            exchange_id: Uuid::new_v4(),
            exchange_name: "ftxus".to_string(),
        };

        // Assert
        assert!(exchanges
            .iter()
            .any(|e| e.exchange_name == dup_exchange.exchange_name));
    }

    #[tokio::test]
    async fn create_dynamic_exchange_tables_works() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let connection_pool = PgPool::connect(&configuration.database.connection_string())
            .await
            .expect("Failed to connect to Postgres.");

        // Create exchange struct
        let exchange = Exchange {
            exchange_id: Uuid::new_v4(),
            exchange_name: "ftxus".to_string(),
        };

        // Create db tables
        create_exchange_tables(&connection_pool, &exchange)
            .await
            .expect("Failed to create tables.");

        // Create rest client
        let client = RestClient::new_us();

        // Get last 10 BTC/USD trades
        let markets = fetch_markets(&connection_pool, &exchange)
            .await
            .expect("Failed to fetch markets.");
        let market = markets
            .iter()
            .find(|m| m.market_name == "BTC/USD")
            .expect("Failed to grab BTC/USD market.");
        let trades = client
            .get_trades(&market.market_name, Some(10), None, Some(Utc::now()))
            .await
            .expect("Failed to get last 10 BTC/USD trades.");

        insert_ftxus_trades(&connection_pool, &market, &exchange, trades)
            .await
            .expect("Failed to insert trades.");
        // Assert
        // Find a way to query db for table to assert that it was created
        //todo!()
    }
}
