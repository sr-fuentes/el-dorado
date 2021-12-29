use crate::candles::{
    create_01d_candles, delete_candle, delete_candle_01d, get_ftx_candles, insert_candle,
    insert_candles_01d, resample_candles, select_candles_by_daterange, validate_01d_candles,
    validate_candle, validate_hb_candles, Candle,
};
use crate::exchanges::{
    ftx::RestClient, ftx::RestError, select_exchanges_by_status, ExchangeName, ExchangeStatus,
};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    select_market_details, select_market_details_by_status_exchange, MarketDetail, MarketStatus,
};
use crate::trades::{
    create_ftx_trade_table, delete_ftx_trades_by_time, drop_ftx_trade_table, drop_table,
    insert_ftx_trades, select_ftx_trades_by_table,
};
use crate::utilities::get_input;
use chrono::{DateTime, Duration, Utc};
use rust_decimal_macros::dec;
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
#[sqlx(rename_all = "lowercase")]
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
#[sqlx(rename_all = "lowercase")]
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

    pub async fn process_candle_validations(&self, status: ValidationStatus) {
        // Get all candle validations from the table
        let validations = select_candle_validations_by_status(&self.pool, status)
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
                ValidationType::Manual => {
                    self.manual_process_candle_validation(validation, market)
                        .await
                }
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
        match validation.duration {
            900 => self.auto_validate_candle(validation, market).await,
            86400 => self.auto_validate_01d_candle(validation, market).await,
            d => panic!("{} is not a supported candle validation duration.", d),
        };
    }

    pub async fn manual_process_candle_validation(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // For hb candles - re-download trades from REST API, download first trade before and after
        // hb candle. Calc the delta from the exchange candle and present the information to the
        // user to determine whether to accept or not.
        // For 01d candles - TODO!
        println!("Attempting manual validation for {:?}", validation);
        match validation.duration {
            900 => self.manual_validate_candle(validation, market).await,
            86400 => self.manual_validate_01_candle(validation, market).await,
            d => panic!("{} is not a supported candle validation duration.", d),
        };
    }

    pub async fn auto_validate_candle(&self, validation: &CandleValidation, market: &MarketDetail) {
        // Recreate candle and then compare new candle to exchange candle for validation and return result.
        let candle = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.recreate_ftx_candle(validation, market).await
            }
        };
        // Set start and end for candle period
        let candle_start = validation.datetime;
        // Get exchange candles from REST client
        let mut exchange_candles = get_ftx_candles(
            &self.clients[&validation.exchange_name],
            market,
            candle_start,
            candle_start,
            900,
        )
        .await;
        // Validate new candle an d return validation status
        let is_valid = validate_candle(&candle, &mut exchange_candles);
        if is_valid {
            let message = "Re-validation successful.".to_string();
            // New candle was validated, save trades if heartbeat and replace unvalidated candle
            self.process_revalidated_candle(validation, market, candle)
                .await;
            // Update validation to complete
            update_candle_validation_status_processed(&self.pool, validation, &message)
                .await
                .expect("Failed to update valdiations status to done.");
        } else {
            // Candle was not auto validated, update type to manual and status to open
            update_candle_validations_type_status(
                &self.pool,
                validation,
                ValidationType::Manual,
                ValidationStatus::Open,
                "Failed to auto-validate.",
            )
            .await
            .expect("Failed to update validation status.");
        }
        // Drop the validation trade table
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.strip_name(),
            validation.validation_type.as_str(),
        );
        drop_table(&self.pool, &qc_table)
            .await
            .expect("Failed to drop qc table.");
    }

    pub async fn auto_validate_01d_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Check that all of the hb candles are validated before continuing. If they are all
        // validated then re-create the daily candle from the heartbeat candles and re-validate.
        let hb_candles = select_candles_by_daterange(
            &self.pool,
            validation.exchange_name.as_str(),
            &validation.market_id,
            validation.datetime,
            validation.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to select hb candles.");
        // Return if not all hb candles are validated
        if !hb_candles.iter().all(|c| c.is_validated) {
            return;
        };
        // Resample 01d candle
        let candle =
            match resample_candles(validation.market_id, &hb_candles, Duration::days(1)).pop() {
                Some(c) => c,
                None => {
                    println!("No heartbeat candles to resample.");
                    return;
                }
            };
        // Get exchange candle
        let mut exchange_candles = get_ftx_candles(
            &self.clients[&validation.exchange_name],
            market,
            validation.datetime,
            validation.datetime,
            86400,
        )
        .await;
        // Validate new candle volume
        let is_valid = validate_candle(&candle, &mut exchange_candles);
        if is_valid {
            let message = "Re-validation successful.".to_string();
            // New candle was validated, update new candle
            self.process_revalidated_01d_candle(validation, candle)
                .await;
            // Update validation to complete
            update_candle_validation_status_processed(&self.pool, validation, &message)
                .await
                .expect("Failed to update validation status to done.");
        } else {
            // Candle was not validated, update to manual and open
            println!("Failed to validated 01D candle.");
            update_candle_validations_type_status(
                &self.pool,
                validation,
                ValidationType::Manual,
                ValidationStatus::Open,
                "Failed to auto-validate.",
            )
            .await
            .expect("Failed to update validation status.");
        }
    }

    pub async fn manual_validate_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Recreate candle and then compare new candle to exchange candle for validation and return result.
        let candle = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.recreate_ftx_candle(validation, market).await
            }
        };
        // Set start and end for candle period
        let candle_start = validation.datetime;
        // Get exchange candles from REST client TODO - make multi exchange
        let exchange_candle = get_ftx_candles(
            &self.clients[&validation.exchange_name],
            market,
            candle_start,
            candle_start,
            900,
        )
        .await
        .pop()
        .unwrap();
        // Get +/- 1 seconds of candles
        let end_trades = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => self.clients[&validation.exchange_name]
                .get_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(candle_start + Duration::seconds(899)),
                    Some(candle_start + Duration::seconds(901)),
                )
                .await
                .expect("Failed to get ftx end of candle trades."),
        };
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
        let start_trades = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => self.clients[&validation.exchange_name]
                .get_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(candle_start - Duration::seconds(1)),
                    Some(candle_start + Duration::seconds(1)),
                )
                .await
                .expect("Failed to get ftx end of candle trades."),
        };
        // Present candle data versus exchange data and get input from user to validate or not
        let message = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                println!(
                    "Manual Validation for {:?} {} {}",
                    validation.exchange_name, market.market_name, validation.datetime
                );
                let delta = candle.value - exchange_candle.volume;
                let percent = delta / exchange_candle.volume * dec!(100.0);
                println!("End of candle trades:\n{:?}", end_trades);
                println!("Start of candle trades:\n{:?}", start_trades);
                println!("New Candle: {:?}", candle);
                println!("ED Value versus FTX Volume:");
                println!("ElDorado: {:?}", candle.value);
                println!("Exchange: {:?}", exchange_candle.volume);
                let message = format!(
                    "Delta: {:?} & Percent: {:?}",
                    delta.round_dp(2),
                    percent.round_dp(4)
                );
                println!("{}", message);
                message
            }
        };
        // Get input for validation
        let response: String = get_input("Accept El-Dorado Candle? [y/yes/n/no]:");
        let is_valid = match response.to_lowercase().as_str() {
            "y" | "yes" => {
                println!("Accepting recreated candle.");
                true
            }
            _ => {
                println!("Rejecting recreated candle.");
                false
            }
        };
        if is_valid {
            // New candle was manually approved and validated, save trades and replace unvalidted
            self.process_revalidated_candle(validation, market, candle)
                .await;
            // Update validation to complete
            update_candle_validation_status_processed(&self.pool, validation, &message)
                .await
                .expect("Failed to update validation status to done.");
        }
        // Drop the validation trade table
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.strip_name(),
            validation.validation_type.as_str(),
        );
        drop_table(&self.pool, &qc_table)
            .await
            .expect("Failed to drop qc table.");
    }

    pub async fn manual_validate_01_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Get hb candles and resample to daily candle
        let hb_candles = select_candles_by_daterange(
            &self.pool,
            validation.exchange_name.as_str(),
            &validation.market_id,
            validation.datetime,
            validation.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to select hb candles.");
        // Resample 01d candle
        let candle = resample_candles(validation.market_id, &hb_candles, Duration::days(1))
            .pop()
            .unwrap();
        // Get exchange candle
        let exchange_candle = get_ftx_candles(
            &self.clients[&validation.exchange_name],
            market,
            validation.datetime,
            validation.datetime,
            86400,
        )
        .await
        .pop()
        .unwrap();
        // Get hb validations
        let hb_validations = select_candle_validations_for_01d(&self.pool, validation)
            .await
            .expect("Failed to get hb validations.");
        // Present candle data versus exchange data and get input from user to validate
        let message = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                println!(
                    "Manual Validation for 01D {:?} {} {}",
                    validation.exchange_name, market.market_name, validation.datetime
                );
                let delta = candle.value - exchange_candle.volume;
                let percent = delta / exchange_candle.volume * dec!(100.0);
                println!("New Candle: {:?}", candle);
                println!("ED Value versus FTX Volume:");
                println!("ElDorado: {:?}", candle.value);
                println!("Exchange: {:?}", exchange_candle.volume);
                let message = format!(
                    "Delta: {:?} & Percent: {:?}",
                    delta.round_dp(2),
                    percent.round_dp(4)
                );
                println!("{}", message);
                println!("HB Validations:\n{:?}", hb_validations);
                message
            }
        };
        // Get input for validation
        let response: String = get_input("Accept El-Dorado 01D Candle? [y/yes/n/no]:");
        let is_valid = match response.to_lowercase().as_str() {
            "y" | "yes" => {
                println!("Accepting recreated candle.");
                true
            }
            _ => {
                println!("Rejecting recreated candle.");
                false
            }
        };
        if is_valid {
            // New candle was validated, update new candle
            self.process_revalidated_01d_candle(validation, candle)
                .await;
            // Update validation to complete
            update_candle_validation_status_processed(&self.pool, validation, &message)
                .await
                .expect("Failed to update validation status to done.");
        }
    }

    pub async fn recreate_ftx_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) -> Candle {
        // Create temp tables to store new trades. Re-download trades for candle timeperiod
        // Return new candle to be evaluated
        let trade_table = format!("qc_{}", validation.validation_type.as_str());
        drop_ftx_trade_table(
            &self.pool,
            validation.exchange_name.as_str(),
            market.strip_name().as_str(),
            &trade_table,
        )
        .await
        .expect("Failed to drop qc table.");
        create_ftx_trade_table(
            &self.pool,
            validation.exchange_name.as_str(),
            market.strip_name().as_str(),
            &trade_table,
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
            let mut new_trades = match self.clients[&validation.exchange_name]
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
            let num_trades = new_trades.len();
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                candle_end_or_last_trade = new_trades.first().unwrap().time;
                let first_trade = new_trades.last().unwrap().time;
                println!(
                    "{} trades returned. First: {}, Last: {}",
                    num_trades, candle_end_or_last_trade, first_trade
                );
                if candle_end_or_last_trade == first_trade {
                    candle_end_or_last_trade = candle_end_or_last_trade - Duration::microseconds(1);
                    println!(
                        "More than 5000 trades in microsecond. Resetting to: {}",
                        candle_end_or_last_trade
                    );
                };
                //println!("Inserting trades in temp table.");
                // Temp table name can be used instead of exhcange name as logic for table name is
                // located in the insert function
                insert_ftx_trades(
                    &self.pool,
                    &market.market_id,
                    validation.exchange_name.as_str(),
                    market.strip_name().as_str(),
                    &trade_table,
                    new_trades,
                )
                .await
                .expect("Failed to insert tmp ftx trades.");
            };
            if num_trades < 5000 {
                // Trades returned are less than 100, end trade getting and make candle
                let qc_table = format!(
                    "trades_{}_{}_qc_{}",
                    validation.exchange_name.as_str(),
                    market.strip_name(),
                    validation.validation_type.as_str(),
                );
                let interval_trades = select_ftx_trades_by_table(&self.pool, qc_table.as_str())
                    .await
                    .expect("Could not fetch trades from temp table.");
                // Create candle from interval trades, trades are already sorted and deduped
                // from select query and primary key uniqueness
                if !interval_trades.is_empty() {
                    let new_candle =
                        Candle::new_from_trades(market.market_id, candle_start, &interval_trades);
                    return new_candle;
                };
                break;
            };
        }
        // If it gets to this point the validaion failed. Return false and the original candle
        let original_candle = select_candles_by_daterange(
            &self.pool,
            validation.exchange_name.as_str(),
            &market.market_id,
            validation.datetime,
            validation.datetime + Duration::seconds(900),
        )
        .await
        .expect("Failed to select candle from db.")
        .pop()
        .unwrap();
        original_candle
    }

    async fn process_revalidated_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
        candle: Candle,
    ) {
        // Delete all trades from _processed (_rest and _ws will drop on next restart)
        println!("New candle is validated. Deleting old trades.");
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                delete_ftx_trades_by_time(
                    &self.pool,
                    validation.exchange_name.as_str(),
                    market.strip_name().as_str(),
                    "processed",
                    validation.datetime,
                    validation.datetime + Duration::seconds(900),
                )
                .await
                .expect("Could not delete processed trades.");
            }
        }
        // Delete existing candle
        delete_candle(
            &self.pool,
            validation.exchange_name.as_str(),
            &market.market_id,
            &validation.datetime,
        )
        .await
        .expect("Could not delete old candle.");
        // Insert candle with validated status
        insert_candle(
            &self.pool,
            validation.exchange_name.as_str(),
            &market.market_id,
            candle,
            true,
        )
        .await
        .expect("Could not insert validated candle.");
        // Get validated trades from temp table, the temp table is only used by the inqui run loop
        // and while async, it does not have mulitple instances writing to the table. An alternative
        // is to pass the trades from the revalidation function. Any duplicate trades added to the
        // _validated table will not be inserted do to the ON CONFLICT clause on INSERT. Any missing
        // trades will be identified on archive of the daily candle as there is a qc to check the
        // total number of trades for the day against the number of trades in the _validated table.
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.strip_name(),
            validation.validation_type.as_str(),
        );
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                let validated_trades = select_ftx_trades_by_table(&self.pool, qc_table.as_str())
                    .await
                    .expect("Failed to select trades from temp table.");
                // Insert trades into _validated
                insert_ftx_trades(
                    &self.pool,
                    &market.market_id,
                    validation.exchange_name.as_str(),
                    market.strip_name().as_str(),
                    "validated",
                    validated_trades,
                )
                .await
                .expect("Could not insert validated trades.");
            }
        }
    }

    pub async fn process_revalidated_01d_candle(
        &self,
        validation: &CandleValidation,
        candle: Candle,
    ) {
        // Delete existing candle
        delete_candle_01d(&self.pool, &validation.market_id, &validation.datetime)
            .await
            .expect("Failed to delete 01D candle.");
        // Insert candle with validated status
        insert_candles_01d(&self.pool, &validation.market_id, &vec![candle])
            .await
            .expect("Failed to insert 01D candle.");
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
        ON CONFLICT (exchange_name, market_id, datetime) DO NOTHING
        "#;
    sqlx::query(sql)
        .bind(exchange)
        .bind(market_id)
        .bind(datetime)
        .bind(duration)
        .bind(ValidationType::Auto.as_str())
        .bind(Utc::now())
        .bind(ValidationStatus::New.as_str())
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

pub async fn select_candle_validations_for_01d(
    pool: &PgPool,
    validation: &CandleValidation,
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
        WHERE duration = 900
        AND market_id = $1
        AND datetime >= $2 AND datetime < $3
        ORDER BY datetime
        "#,
        validation.market_id,
        validation.datetime,
        validation.datetime + Duration::days(1),
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn update_candle_validation_status_processed(
    pool: &PgPool,
    validation: &CandleValidation,
    message: &str,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        UPDATE candle_validations
        SET (processed_ts, validation_status, notes) = ($1, $2, $3)
        WHERE exchange_name = $4
        AND market_id = $5
        AND datetime = $6
        "#;
    sqlx::query(sql)
        .bind(Utc::now())
        .bind(ValidationStatus::Done.as_str())
        .bind(message)
        .bind(validation.exchange_name.as_str())
        .bind(validation.market_id)
        .bind(validation.datetime)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_candle_validations_type_status(
    pool: &PgPool,
    validation: &CandleValidation,
    validation_type: ValidationType,
    status: ValidationStatus,
    message: &str,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        UPDATE candle_validations
        SET (validation_type, validation_status, notes) = ($1, $2, $3)
        WHERE exchange_name = $4
        AND market_id = $5
        AND datetime = $6
        "#;
    sqlx::query(sql)
        .bind(validation_type.as_str())
        .bind(status.as_str())
        .bind(message)
        .bind(validation.exchange_name.as_str())
        .bind(validation.market_id)
        .bind(validation.datetime)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::candles::create_exchange_candle_table;
    use crate::configuration::get_configuration;
    use crate::exchanges::ExchangeName;
    use crate::inquisidor::Inquisidor;
    use crate::trades::{create_ftx_trade_table, drop_ftx_trade_table};
    use crate::validation::{
        insert_candle_validation, CandleValidation, ValidationStatus, ValidationType,
    };
    use chrono::{TimeZone, Utc};
    use sqlx::PgPool;
    use uuid::Uuid;

    pub async fn prep_candle_validation(validation: CandleValidation) {
        // Load configuration and db connection to dev
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Update FTX BTC-PERP market_id to b3bf21db-92bb-4613-972a-1d0f1aab1e95 to match prod val
        let sql = r#"
            UPDATE markets set market_id = 'b3bf21db-92bb-4613-972a-1d0f1aab1e95'
            WHERE market_name = 'BTC-PERP'
            AND exchange_name = 'ftx'
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to update id.");

        // Create candles table if it does not exists
        let sql = r#"
            DROP TABLE IF EXISTS candles_15t_ftx
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to drop table.");
        create_exchange_candle_table(&pool, "ftx")
            .await
            .expect("Failed to create table.");

        // Create trades table if it does not exist
        drop_ftx_trade_table(&pool, "ftx", "btcperp", "processed")
            .await
            .expect("Failed to drop table.");
        create_ftx_trade_table(&pool, "ftx", "btcperp", "processed")
            .await
            .expect("Failed to create trade table.");
        drop_ftx_trade_table(&pool, "ftx", "btcperp", "validated")
            .await
            .expect("Failed to drop table.");
        create_ftx_trade_table(&pool, "ftx", "btcperp", "validated")
            .await
            .expect("Failed to create trade table.");

        // Clear candle validations table
        let sql = r#"
            DELETE FROM candle_validations
            WHERE 1=1
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to delete validation records.");

        // Insert bad candle validation that can be fixed automatically
        insert_candle_validation(
            &pool,
            validation.exchange_name.as_str(),
            &validation.market_id,
            &validation.datetime,
            validation.duration,
        )
        .await
        .expect("Failed to insert candle validation.");
    }

    #[tokio::test]
    pub async fn auto_revalidate_candle() {
        // Create validation that can be auto revalidated
        let validation = CandleValidation {
            exchange_name: ExchangeName::Ftx,
            market_id: Uuid::parse_str("b3bf21db-92bb-4613-972a-1d0f1aab1e95").unwrap(),
            datetime: Utc.ymd(2021, 12, 18).and_hms(7, 45, 00),
            duration: 900,
            validation_type: ValidationType::Auto,
            created_ts: Utc::now(),
            processed_ts: None,
            validation_status: ValidationStatus::New,
            notes: None,
        };
        prep_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }

    #[tokio::test]
    pub async fn manual_revalidate_candle() {
        // Create validation that cannot be auto revalidated and needs to be manual revalidated
        let validation = CandleValidation {
            exchange_name: ExchangeName::Ftx,
            market_id: Uuid::parse_str("b3bf21db-92bb-4613-972a-1d0f1aab1e95").unwrap(),
            datetime: Utc.ymd(2021, 12, 10).and_hms(7, 30, 00),
            duration: 900,
            validation_type: ValidationType::Auto,
            created_ts: Utc::now(),
            processed_ts: None,
            validation_status: ValidationStatus::New,
            notes: None,
        };
        prep_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
        println!("Sleeping 5 seconds before starting manual validation.");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        ig.process_candle_validations(ValidationStatus::Open).await;
    }

    #[tokio::test]
    pub async fn auto_revalidate_01d_candle_no_heartbeats() {
        // Create validation that cannot be auto revalidated and needs to be manual revalidated
        let validation = CandleValidation {
            exchange_name: ExchangeName::Ftx,
            market_id: Uuid::parse_str("b3bf21db-92bb-4613-972a-1d0f1aab1e95").unwrap(),
            datetime: Utc.ymd(2021, 12, 18).and_hms(0, 0, 0),
            duration: 86400,
            validation_type: ValidationType::Auto,
            created_ts: Utc::now(),
            processed_ts: None,
            validation_status: ValidationStatus::New,
            notes: None,
        };
        prep_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }
}
