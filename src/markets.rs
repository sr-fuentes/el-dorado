use crate::candles::Candle;
use crate::exchanges::{
    ftx::{Market, RestClient, RestError},
    select_exchanges, ExchangeName,
};
use crate::inquisidor::Inquisidor;
use crate::utilities::get_input;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::convert::{TryInto, TryFrom};
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
    pub first_candle: Option<DateTime<Utc>>,
    pub last_candle: Option<DateTime<Utc>>,
    pub mita: Option<String>,
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

#[derive(Debug, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum MarketStatus {
    // Market is new and has never been run
    New,
    // Market is backfilling from start to current last trade
    Backfill,
    // Market is syncing between the backfill and the streamed trades collected while backfilling
    Sync,
    // Market is active in loop to stream trades and create candles
    Active,
    // Market has crashed the the program is restarting.
    Restart,
    // Market is backfilling from start to current start of day
    Historical,
    // Market is not available for streaming or backfilling. Ignore completely.
    Terminated,
}

impl MarketStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketStatus::New => "new",
            MarketStatus::Backfill => "backfill",
            MarketStatus::Sync => "sync",
            MarketStatus::Active => "active",
            MarketStatus::Restart => "restart",
            MarketStatus::Historical => "historical",
            MarketStatus::Terminated => "terminated",
        }
    }
}

impl TryFrom<String> for MarketStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "backfill" => Ok(Self::Backfill),
            "sync" => Ok(Self::Sync),
            "active" => Ok(Self::Active),
            "restart" => Ok(Self::Restart),
            "historical" => Ok(Self::Historical),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported market status.", other)),
        }
    }
}

impl Inquisidor {
    pub async fn refresh_exchange(&self) {
        // Get user input for exchange to refresh
        let exchange: String = get_input("Enter Exchange to Refresh:");
        // Parse input to see if it is a valid exchange
        let exchange: ExchangeName = exchange.try_into().unwrap();
        // Get current exchanges from db
        let exchanges = select_exchanges(&self.pool)
            .await
            .expect("Failed to fetch exchanges.");
        // Compare input to existing exchanges
        if !exchanges.iter().any(|e| e.name == exchange) {
            // Exchange not added
            println!("{:?} has not been added to El-Dorado.", exchange);
            return;
        }
        self.refresh_exchange_markets(&exchange).await;
    }

    pub async fn refresh_exchange_markets(&self, exchange: &ExchangeName) {
        // Get USD markets from exchange
        let markets = get_usd_markets(exchange).await;
        // Get existing markets for exchange from db.
        let market_ids = select_market_ids_by_exchange(&self.pool, exchange)
            .await
            .expect("Failed to get markets from db.");
        // For each market that is not in the exchange, insert into db.
        for market in markets.iter() {
            if !market_ids.iter().any(|m| m.market_name == market.name) {
                // Exchange market not in database.
                println!("Adding {:?} market for {:?}", market.name, exchange);
                insert_new_market(
                    &self.pool,
                    exchange,
                    market,
                    self.settings.application.ip_addr.as_str(),
                )
                .await
                .expect("Failed to insert market.");
            }
        }
    }
}

pub async fn get_usd_markets(exchange: &ExchangeName) -> Vec<Market> {
    let markets = match exchange {
        ExchangeName::FtxUs => {
            let client = RestClient::new_us();
            get_ftx_usd_markets(&client).await
        }
        ExchangeName::Ftx => {
            let client = RestClient::new_intl();
            get_ftx_usd_markets(&client).await
        }
    };
    match markets {
        Ok(markets) => markets,
        Err(err) => panic!("Failed to fetch markets. RestError {:?}", err),
    }
}

pub async fn get_ftx_usd_markets(client: &RestClient) -> Result<Vec<Market>, RestError> {
    // Get markets from exchange
    let mut markets = client.get_markets().await?;
    // Filter for USD based markets
    markets.retain(|m| m.quote_currency == Some("USD".to_string()) || m.market_type == *"future");
    Ok(markets)
}

pub async fn select_market_ids_by_exchange(
    pool: &PgPool,
    exchange: &ExchangeName,
) -> Result<Vec<MarketId>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketId,
        r#"
        SELECT market_id, market_name
        FROM markets
        WHERE exchange_name = $1
        "#,
        exchange.as_str()
    )
    .fetch_all(pool)
    .await?;
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
    exchange: &ExchangeName,
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
        exchange.as_str(),
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
    use crate::exchanges::select_exchanges;
    use crate::markets::select_market_ids_by_exchange;
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
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == configuration.application.exchange)
            .unwrap();

        // Get input from config for market to archive
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
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
