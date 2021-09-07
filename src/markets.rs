use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct MarketId {
    pub market_id: Uuid,
    pub market_name: String,
}

pub async fn pull_usd_markets_from_ftx(exchange: &str) -> Result<Vec<Market>, RestError> {
    // Get Rest Client
    let client = match exchange {
        "ftxus" => RestClient::new_us(),
        "ftx" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange),
    };

    // Get Markets
    let mut markets = client.get_markets().await?;
    //let filtered_markets = markets.retain(|m| m.base_currency == Some("USD"));
    markets.retain(|m| m.quote_currency == Some("USD".to_string()));
    Ok(markets)
}

pub async fn fetch_markets(
    pool: &PgPool,
    exchange: &Exchange,
) -> Result<Vec<MarketId>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketId,
        r#"
        SELECT market_id, market_name
        FROM markets
        WHERE exchange_name = $1
        "#,
        exchange.exchange_name
    )
    .fetch_all(pool)
    .await?;
    println!("Rows: {:?}", rows);
    Ok(rows)
}

pub async fn insert_new_market(
    pool: &PgPool,
    exchange: &Exchange,
    market: &Market,
) -> Result<(), sqlx::Error> {
    let network = "127.0.0.1"
        .parse::<sqlx::types::ipnetwork::IpNetwork>()
        .unwrap();
    sqlx::query!(
        r#"
        INSERT INTO markets (
            market_id, exchange_name, market_name, market_type, base_currency,
            quote_currency, underlying, market_status, market_data_status,
            candle_base_interval, candle_base_in_seconds, last_update_ts, last_update_ip_address)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)    
        "#,
        Uuid::new_v4(),
        exchange.exchange_name,
        market.name,
        market.market_type,
        market.base_currency,
        market.quote_currency,
        market.underlying,
        "Active",
        "New",
        "15T",
        900,
        Utc::now(),
        network
    )
    .execute(pool)
    .await?;
    Ok(())
}
