use sqlx::PgPool;
use uuid::Uuid;

pub mod ftx;

#[derive(Debug)]
struct Exchange {exchange_id: Uuid, exchange_name: String}

pub async fn add(pool: &PgPool) {
    // Get input from user for exchange to add

    // Compare imput to existing exchanges in table

    // If exchange already exists, do nothing

    // If exchange does not exists, get remaining inputs and insert into db
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
        
        let _ = fetch_exchanges(&connection_pool).await.expect("Failed to load exchanges.");
        }
}