use crate::candles::{create_01d_candles, validate_01d_candles, validate_hb_candles};
use crate::exchanges::{ftx::RestClient, ftx::RestError, select_exchanges_by_status, ExchangeName, ExchangeStatus};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    select_market_details, select_market_details_by_status_exchange, MarketDetail, MarketStatus,
};
use crate::trades::{create_ftx_trade_table, drop_ftx_trade_table};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct CandleValidation {
    pub exchange_name: ExchangeName,
    pub market_id: Uuid,
    pub datetime: DateTime<Utc>,
    pub duration: i64,
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

    pub async fn process_candle_validations(&self) {
        // Get all candle validations from the table
        let validations = select_candle_validations_by_status(&self.pool, ValidationStatus::New)
            .await
            .expect("Failed to select candle validations.");
        // Get all market details - for market id and strip name fn in validation
        let markets = select_market_details(&self.pool)
            .await
            .expect("Failed to select all market details.");
        // Validate all entries
        for validation in validations.iter() {
            // Get market detail for validation
            let market = markets
                .iter()
                .find(|m| m.market_id == validation.market_id)
                .unwrap();
            match validation.validation_type {
                ValidationType::Auto => {
                    self.auto_process_candle_validation(validation, market)
                        .await
                }
                ValidationType::Manual => println!("Manual validation not implemented."),
            }
        }
    }

    pub async fn auto_process_candle_validation(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // For 01d candles - re-sample from heartbeat candles
        // For hb candles - re-download trades from REST API
        println!("Attempting auto-validation for {:?}", validation);
        let validated = match validation.duration {
            900 => self.auto_reprocess_candle(validation, market).await,
            86400 => todo!(),
            d => panic!("{} is not a supported candle validation duration.", d),
        };
        if validated {
            // New candle was validated, save trades if heartbeat and replace unvalidated candle
            todo!();
        } else {
            // Candle was not auto validated, update type to manual and status to open
            todo!();
        }
    }

    pub async fn auto_reprocess_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) -> bool {
        // Create temp trade table if it does not exists. Then re-download trades for candle
        // timeperiod. Compare new candle to exchange candle for validation and return result.
        drop_ftx_trade_table(
            &self.pool,
            validation.exchange_name.as_str(),
            market.strip_name().as_str(),
            "qc",
        )
        .await
        .expect("Failed to drop qc table.");
        create_ftx_trade_table(
            &self.pool,
            validation.exchange_name.as_str(),
            market.strip_name().as_str(),
            "qc",
        )
        .await
        .expect("Failed to create qc table.");
        // Set start and end for candle period
        let candle_start = validation.datetime;
        let candle_end = candle_start + Duration::seconds(900);
        let mut candle_end_or_last_trade = candle_end;
        // Download trades for candle period
        while candle_start < candle_end_or_last_trade {
            // Prevent 429 errors by only requesting 4 per second
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = match client
                .get_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(candle_start),
                    Some(candle_end_or_last_trade),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() {
                        println!("Request timed out. Waiting 30 seconds before retrying.");
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_connect() {
                        println!(
                            "Connect error with reqwest. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.status() == Some(reqwest::StatusCode::BAD_GATEWAY) {
                        println!("502 Bad Gateway. Waiting 30 seconds before retry. {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.status() == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) {
                        println!(
                            "503 Service Unavailable. Waiting 60 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                        continue;
                    } else {
                        panic!("Error (not timeout or connect): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
        }
        true
    }
}

pub async fn insert_candle_validation(
    pool: &PgPool,
    exchange: &str,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
    duration: i64,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO candle_validations (
            exchange_name, market_id, datetime, duration, validation_type, created_ts,
            validation_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
    sqlx::query(sql)
        .bind(exchange)
        .bind(market_id)
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

pub async fn select_candle_validations_by_status(
    pool: &PgPool,
    status: ValidationStatus,
) -> Result<Vec<CandleValidation>, sqlx::Error> {
    let rows = sqlx::query_as!(
        CandleValidation,
        r#"
        SELECT exchange_name as "exchange_name: ExchangeName",
            market_id, datetime, duration,
            validation_type as "validation_type: ValidationType",
            created_ts, processed_ts,
            validation_status as "validation_status: ValidationStatus",
            notes
        FROM candle_validations
        WHERE validation_status = $1
        ORDER by exchange_name, market_id
        "#,
        status.as_str()
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
