use crate::candles::{create_01d_candles, validate_01d_candles, validate_hb_candles};
use crate::exchanges::{ftx::RestClient, select_exchanges_by_status, ExchangeName, ExchangeStatus};
use crate::inquisidor::Inquisidor;
use crate::markets::{select_market_details_by_status_exchange, MarketStatus};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CandleValidation {
    pub exchange_name: ExchangeName,
    pub market_name: String,
    pub datetime: DateTime<Utc>,
    pub duration: i32,
    pub validation_type: ValidationType,
    pub created_ts: DateTime<Utc>,
    pub processed_ts: Option<DateTime<Utc>>,
    pub validation_status: ValidationStatus,
    pub notes: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
pub enum ValidationType {
    Auto,
    Manual,
}

impl ValidationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValidationType::Auto => "auto",
            ValidationType::Manual => "manual",
        }
    }
}

impl TryFrom<String> for ValidationType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "manual" => Ok(Self::Manual),
            other => Err(format!("{} is not a supported validation type.", other)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
pub enum ValidationStatus {
    New,
    Open,
    Done,
}

impl ValidationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValidationStatus::New => "new",
            ValidationStatus::Open => "open",
            ValidationStatus::Done => "done",
        }
    }
}

impl TryFrom<String> for ValidationStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "open" => Ok(Self::Open),
            "done" => Ok(Self::Done),
            other => Err(format!("{} is not a supported validation status.", other)),
        }
    }
}

impl Inquisidor {
    pub async fn validate_candles(&self) {
        // Validate heartbeat candles for each exchange and market that is active
        let exchanges = select_exchanges_by_status(&self.pool, ExchangeStatus::Active)
            .await
            .expect("Failed to select exchanges.");
        for exchange in exchanges.iter() {
            // Get REST client
            let client = match exchange.name {
                ExchangeName::FtxUs => RestClient::new_us(),
                ExchangeName::Ftx => RestClient::new_intl(),
            };
            // Get active markets for exchange
            let markets = select_market_details_by_status_exchange(
                &self.pool,
                &exchange.name,
                &MarketStatus::Active,
            )
            .await
            .expect("Failed to select active markets for exchange.");
            for market in markets.iter() {
                // For each active market, validated heartbeat candles
                validate_hb_candles(
                    &self.pool,
                    &client,
                    exchange.name.as_str(),
                    market,
                    &self.settings,
                )
                .await;
                // Create any 01d candles
                create_01d_candles(&self.pool, exchange.name.as_str(), &market.market_id).await;
                // Validated 01d candles
                validate_01d_candles(&self.pool, &client, exchange.name.as_str(), market).await;
            }
        }
    }

    pub async fn process_candle_validations(&self) {}
}

pub async fn insert_candle_validation(
    pool: &PgPool,
    exchange: &str,
    market: &str,
    datetime: &DateTime<Utc>,
    duration: i32,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO candle_validations (
            exchange_name, market_name, datetime, duration, validation_type, created_ts,
            validation_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
    sqlx::query(sql)
        .bind(exchange)
        .bind(market)
        .bind(datetime)
        .bind(duration)
        .bind(ValidationType::Auto)
        .bind(Utc::now())
        .bind(ValidationStatus::New)
        .bind("Basic QC failed, re-download trades and re-validate.")
        .execute(pool)
        .await?;
    Ok(())
}
