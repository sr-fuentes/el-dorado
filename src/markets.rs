use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;
use crate::candles::Candle;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct MarketId {
    pub market_id: Uuid,
    pub market_name: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MarketDetail {
    pub market_id: Uuid,
    pub exchange_name: String,
    pub market_name: String,
    pub market_type: String,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub underlying: Option<String>,
    pub market_status: String,
    pub market_data_status: String,
    pub first_validated_trade_id: Option<String>,
    pub first_validated_trade_ts: Option<DateTime<Utc>>,
    pub last_validated_trade_id: Option<String>,
    pub last_validated_trade_ts: Option<DateTime<Utc>>,
    pub candle_base_interval: String,
    pub candle_base_in_seconds: i32,
    pub first_validated_candle: Option<DateTime<Utc>>,
    pub last_validated_candle: Option<DateTime<Utc>>,
    pub last_update_ts: DateTime<Utc>,
    pub last_update_ip_address: sqlx::types::ipnetwork::IpNetwork,
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
    markets.retain(|m| {
        m.quote_currency == Some("USD".to_string()) || m.market_type == "future".to_string()
    });
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
    // println!("Rows: {:?}", rows);
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

pub async fn select_market_detail(
    pool: &PgPool,
    market: &MarketId,
) -> Result<MarketDetail, sqlx::Error> {
    let row = sqlx::query_as!(
        MarketDetail,
        r#"
            SELECT * FROM markets
            WHERE market_id = $1
        "#,
        market.market_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn update_market_last_validated(
    pool: &PgPool,
    market: &MarketId,
    candle: &Candle,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
            UPDATE markets
            SET (last_validated_trade_id, last_validated_trade_ts, last_validated_candle, 
                last_update_ts) = ($1, $2, $3, $4)
            WHERE market_id = $5
        "#,
        candle.last_trade_id,
        candle.last_trade_ts,
        candle.datetime,
        Utc::now(),
        market.market_id
    )
    .execute(pool)
    .await?;
    Ok(())
}