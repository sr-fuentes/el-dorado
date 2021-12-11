use crate::candles::Candle;
use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
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

impl MarketId {
    pub fn strip_name(&self) -> String {
        self.market_name.replace(&['/', '-'][..], "")
    }
}

impl MarketDetail {
    pub fn strip_name(&self) -> String {
        self.market_name.replace(&['/', '-'][..], "")
    }
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
    markets.retain(|m| m.quote_currency == Some("USD".to_string()) || m.market_type == *"future");
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

pub async fn select_market_detail_by_exchange_mita(
    pool: &PgPool,
    exchange_name: &str,
    mita: &str,
) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
        r#"
        SELECT *
        FROM markets
        WHERE exchange_name = $1
        AND mita = $2
        "#,
        exchange_name,
        mita
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn insert_new_market(
    pool: &PgPool,
    exchange: &Exchange,
    market: &Market,
    ip_addr: &str,
) -> Result<(), sqlx::Error> {
    let network = ip_addr
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

pub async fn select_markets_active(pool: &PgPool) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
        r#"
            SELECT * FROM markets
            WHERE market_data_status in ('Active','Syncing')
        "#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn update_market_last_validated(
    pool: &PgPool,
    market_id: &Uuid,
    candle: &Candle,
    ip_addr: &str,
) -> Result<(), sqlx::Error> {
    let network = ip_addr
        .parse::<sqlx::types::ipnetwork::IpNetwork>()
        .unwrap();
    sqlx::query!(
        r#"
            UPDATE markets
            SET (last_validated_trade_id, last_validated_trade_ts, last_validated_candle, 
                last_update_ts, last_update_ip_address) = ($1, $2, $3, $4, $5)
            WHERE market_id = $6
        "#,
        candle.last_trade_id,
        candle.last_trade_ts,
        candle.datetime,
        Utc::now(),
        network,
        market_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_data_status(
    pool: &PgPool,
    market_id: &Uuid,
    status: &str,
    ip_addr: &str,
) -> Result<(), sqlx::Error> {
    let network = ip_addr
        .parse::<sqlx::types::ipnetwork::IpNetwork>()
        .unwrap();
    sqlx::query!(
        r#"
        UPDATE markets
        SET (market_data_status, last_update_ip_address)  = ($1, $2)
        WHERE market_id = $3
        "#,
        status,
        network,
        market_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::configuration::*;
    use crate::exchanges::fetch_exchanges;
    use crate::markets::fetch_markets;
    use sqlx::PgPool;

    #[tokio::test]
    pub async fn strip_name_removes_dash_and_slash() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.exchange_name == configuration.application.exchange)
            .unwrap();

        // Get input from config for market to archive
        let market_ids = fetch_markets(&pool, &exchange)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == configuration.application.market)
            .unwrap();

        // Print market and strip market
        println!("Market: {:?}", market);
        let stripped_market = market.strip_name();
        println!("Stripped Market: {}", stripped_market);
    }
}
