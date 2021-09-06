use crate::exchanges::ftx::*;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct MarketId {
    market_id: Uuid,
    market_name: String,
}

pub async fn pull_markets_from_exchange(exchange: &str) -> Result<Vec<Market>, RestError> {
    // Get Rest Client
    let client = match exchange {
        "ftx" => RestClient::new_us(),
        "ftxus" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange),
    };

    // Get Markets
    let markets = client.get_markets().await?;
    Ok(markets)
}

pub async fn fetch_markets(
    pool: &PgPool,
    exchange_name: String,
) -> Result<Vec<MarketId>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketId,
        r#"
        SELECT market_id, market_name
        FROM markets
        WHERE exchange_name = $1
        "#,
        exchange_name
    )
    .fetch_all(pool)
    .await?;
    println!("Rows: {:?}", rows);
    Ok(rows)
}

pub async fn insert_new_market() {}
