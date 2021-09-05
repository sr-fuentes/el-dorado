use crate::markets::pull_markets_from_exchange;
use crate::utilities::get_input;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub mod ftx;

#[derive(Debug, PartialEq, Eq)]
pub struct Exchange {
    exchange_id: Uuid,
    exchange_name: String,
}

pub async fn add(pool: &PgPool) {
    // Get input from user for exchange to add
    let exchange: String = get_input("Enter Exchange to Add:");
    let exchange = Exchange {
        exchange_id: Uuid::new_v4(),
        exchange_name: exchange,
    };

    // Get list of exchanges from db
    let exchanges = fetch_exchanges(pool)
        .await
        .expect("Could not fetch exchanges.");

    // Compare input to existing exchanges in table and add if new
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
        let _ = insert_new_exchange(pool, &exchange)
            .await
            .expect("Failed to insert new exchange.");
    }

    // Fetch markets for new exchange
    let markets = match exchange.exchange_name.as_str() {
        "ftxus" => pull_markets_from_exchange("ftxus").await, // fetch ftxus markets,
        "ftx" => pull_markets_from_exchange("ftx").await,     // fetch ftx markets,
        _ => {
            println!(
                "{:?} exchange not yet supported.",
                exchange.exchange_name.as_str()
            );
            return;
        }
    };

    // Insert markets of new exchange into database if returned successfully
    let markets = match markets {
        Ok(markets) => markets,
        Err(err) => {
            println!("Could not fetch markets from new exchange, try to refresh later.");
            println!("Err: {:?}", err);
            return;
        }
    };

    println!("Markets pulled: {:?}.", markets);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::get_configuration;

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
}
