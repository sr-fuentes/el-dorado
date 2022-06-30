use crate::candles::{
    delete_candle, delete_candle_01d, get_ftx_candles_daterange, get_gdax_candles_daterange,
    insert_candle, insert_candles_01d, resample_candles, select_candles_by_daterange,
    validate_ftx_candle, validate_gdax_candle_by_trade_ids, Candle,
};
use crate::exchanges::{
    error::RestError, ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade, ExchangeName,
};
use crate::inquisidor::Inquisidor;
use crate::markets::{select_market_details, MarketDetail};
use crate::trades::{
    create_ftx_trade_table, create_gdax_trade_table, delete_trades_by_time, drop_table,
    drop_trade_table, insert_ftx_trades, insert_gdax_trades, select_ftx_trades_by_table,
    select_ftx_trades_by_time, select_gdax_trades_by_table, select_gdax_trades_by_time,
};
use crate::utilities::{get_input, TimeFrame, Trade};
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal_macros::dec;
use sqlx::PgPool;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone, sqlx::FromRow)]
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
    Count,
}

impl ValidationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValidationType::Auto => "auto",
            ValidationType::Manual => "manual",
            ValidationType::Count => "count",
        }
    }
}

impl TryFrom<String> for ValidationType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "manual" => Ok(Self::Manual),
            "count" => Ok(Self::Count),
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
    pub async fn process_candle_validations(&self, status: ValidationStatus) {
        // Get all candle validations from the table
        let validations = select_candle_validations_by_status(&self.ig_pool, status)
            .await
            .expect("Failed to select candle validations.");
        // Get all market details - for market id and strip name fn in validation
        let markets = select_market_details(&self.ig_pool)
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
                ValidationType::Count => {
                    self.process_candle_count_validation(validation, market)
                        .await
                }
            }
        }
    }

    pub async fn process_candle_count_validation(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Get all trades for the day from the validated table, get all candles for the day from
        // the heartbeat candles table. Get the 01d candle. For each hb candle compare trade count
        // to the number of trades in the processed table. If there are difference, re-download
        // trades, validate and insert into processed.
        let hb_candles = select_candles_by_daterange(
            &self.ig_pool,
            &validation.exchange_name,
            &validation.market_id,
            validation.datetime,
            validation.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to select hb candles.");
        let updated_count = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.process_ftx_candle_count_validation(validation, market, &hb_candles)
                    .await
            }
            ExchangeName::Gdax => {
                self.process_gdax_candle_count_validation(validation, market, &hb_candles)
                    .await
            }
        };
        // Mark as closed - if there is another issue then it wont be able to be added do to unique
        // constrain on validation table but we don't want it constantly trying to re-download
        // trades. TODO add a check on the insert to see if a validtion exists that is closed and
        // raise a manual validation.
        let message = format!("{} candles updated to fix trades.", updated_count);
        update_candle_validation_status_processed(&self.ig_pool, validation, &message)
            .await
            .expect("Failed to update validation status to done.");
        // Drop the validation trade table
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.as_strip(),
            validation.validation_type.as_str(),
        );
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                drop_table(&self.ftx_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
            ExchangeName::Gdax => {
                drop_table(&self.gdax_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
        }
    }

    pub async fn process_ftx_candle_count_validation(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
        hb_candles: &[Candle],
    ) -> i32 {
        let trades = select_ftx_trades_by_time(
            &self.ftx_pool,
            &validation.exchange_name,
            market,
            "validated",
            validation.datetime,
            validation.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to select daily trades.");
        let mut updated_count = 0;
        for candle in hb_candles.iter() {
            // Check trade counts against candle
            let filtered_trades: Vec<FtxTrade> = trades
                .iter()
                .filter(|t| t.time.duration_trunc(self.hbtf.as_dur()).unwrap() == candle.datetime)
                .cloned()
                .collect();
            if filtered_trades.len() as i64 != candle.trade_count {
                updated_count += 1;
                println!(
                    "{:?} trade count does not match: {} trades v {} candle trade count.",
                    candle.datetime,
                    filtered_trades.len(),
                    candle.trade_count
                );
                let new_candle = self
                    .recreate_ftx_candle(validation, market, candle.datetime)
                    .await;
                // If new candle trade count matches candle trade count, insert trades
                if new_candle.trade_count == candle.trade_count {
                    println!("Found missing trades, updating trade table.");
                    let qc_table = format!(
                        "trades_{}_{}_qc_{}",
                        validation.exchange_name.as_str(),
                        market.as_strip(),
                        validation.validation_type.as_str(),
                    );
                    let validated_trades =
                        select_ftx_trades_by_table(&self.ftx_pool, qc_table.as_str())
                            .await
                            .expect("Failed to select qc trades.");
                    insert_ftx_trades(
                        &self.ftx_pool,
                        &validation.exchange_name,
                        market,
                        "validated",
                        validated_trades,
                    )
                    .await
                    .expect("Failed to insert validated trades.");
                }
            }
        }
        updated_count
    }

    pub async fn process_gdax_candle_count_validation(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
        hb_candles: &[Candle],
    ) -> i32 {
        let trades = select_gdax_trades_by_time(
            &self.gdax_pool,
            market,
            "validated",
            validation.datetime,
            validation.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to select daily trades.");
        let mut updated_count = 0;
        for candle in hb_candles.iter() {
            // Check trade counts against candle
            let filtered_trades: Vec<GdaxTrade> = trades
                .iter()
                .filter(|t| t.time.duration_trunc(self.hbtf.as_dur()).unwrap() == candle.datetime)
                .cloned()
                .collect();
            if filtered_trades.len() as i64 != candle.trade_count {
                updated_count += 1;
                println!(
                    "{:?} trade count does not match: {} trades v {} candle trade count.",
                    candle.datetime,
                    filtered_trades.len(),
                    candle.trade_count
                );
                let new_candle = self
                    .recreate_gdax_candle(validation, market, candle.datetime)
                    .await;
                // If new candle trade count matches candle trade count, insert trades
                if new_candle.trade_count == candle.trade_count {
                    println!("Found missing trades, updating trade table.");
                    let qc_table = format!(
                        "trades_{}_{}_qc_{}",
                        validation.exchange_name.as_str(),
                        market.as_strip(),
                        validation.validation_type.as_str(),
                    );
                    let validated_trades =
                        select_gdax_trades_by_table(&self.gdax_pool, qc_table.as_str())
                            .await
                            .expect("Failed to select qc trades.");
                    insert_gdax_trades(
                        &self.gdax_pool,
                        &validation.exchange_name,
                        market,
                        "validated",
                        validated_trades,
                    )
                    .await
                    .expect("Failed to insert validated trades.");
                }
            }
        }
        updated_count
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
        println!(
            "Manual Candle Validation ({}) for {:?} {}",
            validation.duration, validation.exchange_name, validation.datetime, 
        );
        match validation.duration {
            900 => self.manual_validate_candle(validation, market).await,
            86400 => self.manual_validate_01_candle(validation, market).await,
            d => panic!("{} is not a supported candle validation duration.", d),
        };
    }

    pub async fn auto_validate_candle(&self, validation: &CandleValidation, market: &MarketDetail) {
        // Recreate candle and then compare new candle to exchange candle for validation
        // and return result.
        // Set start and end for candle period
        let candle_start = validation.datetime;
        let (candle, is_valid) = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                let candle = self
                    .recreate_ftx_candle(validation, market, validation.datetime)
                    .await;
                // Get exchange candles from REST client
                let mut exchange_candles =
                    get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
                        &self.clients[&validation.exchange_name],
                        market,
                        candle_start,
                        candle_start,
                        900,
                    )
                    .await;
                // Validate new candle an d return validation status
                let is_valid = Some(validate_ftx_candle(&candle, &mut exchange_candles));
                (candle, is_valid)
            }
            ExchangeName::Gdax => {
                let candle = self
                    .recreate_gdax_candle(validation, market, validation.datetime)
                    .await;
                // Get exchange candles from REST client
                let mut exchange_candles =
                    get_gdax_candles_daterange::<crate::exchanges::gdax::Candle>(
                        &self.clients[&validation.exchange_name],
                        market,
                        candle_start,
                        candle_start,
                        900,
                    )
                    .await;
                println!("Exchange candles: {:?}", exchange_candles);
                // Validate trades in qc table to trade id validation
                let trade_table = format!("qc_{}", validation.validation_type.as_str());
                let is_valid = validate_gdax_candle_by_trade_ids(
                    &self.gdax_pool,
                    &self.clients[&validation.exchange_name],
                    market,
                    &candle,
                    &mut exchange_candles,
                    &TimeFrame::T15, // TODO: Remove hardcoding to HB TF
                    &trade_table,
                )
                .await;
                // Check for missing trades - gap from last trade id of previous candle to first
                // trade id of current validation candle as cause of invalidation. If missing trades
                // are found -> 1) if part of previous candle - mark previous candle as invalid,
                // remove any candle validations for previous canle, create ne candle validtion for
                // previous candle. 2) if part of current candle - re-create candle for the second
                // time with the new trades and re-validate
                (candle, is_valid)
            }
        };
        match is_valid {
            Some(v) => {
                if v {
                    let message = "Re-validation successful.".to_string();
                    // New candle was validated, save trades if heartbeat and replace unvalidated candle
                    self.process_revalidated_candle(validation, market, candle)
                        .await;
                    // Update validation to complete
                    update_candle_validation_status_processed(&self.ig_pool, validation, &message)
                        .await
                        .expect("Failed to update valdiations status to done.");
                } else {
                    // Candle was not auto validated, update type to manual and status to open
                    update_candle_validations_type_status(
                        &self.ig_pool,
                        validation,
                        ValidationType::Manual,
                        ValidationStatus::Open,
                        "Failed to auto-validate.",
                    )
                    .await
                    .expect("Failed to update validation status.");
                }
            }
            None => {
                // There was no validation completed, return without doing anything
                println!("There was no result from validation, try again.");
            }
        };
        // Drop the validation trade table
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.as_strip(),
            validation.validation_type.as_str(),
        );
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                drop_table(&self.ftx_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
            ExchangeName::Gdax => {
                drop_table(&self.gdax_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
        }
    }

    pub async fn auto_validate_01d_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Check that all of the hb candles are validated before continuing. If they are all
        // validated then re-create the daily candle from the heartbeat candles and re-validate.
        let hb_candles = select_candles_by_daterange(
            &self.ig_pool,
            &validation.exchange_name,
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
        let is_valid = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                // Get exchange candle
                let mut exchange_candles =
                    get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
                        &self.clients[&validation.exchange_name],
                        market,
                        validation.datetime,
                        validation.datetime,
                        86400,
                    )
                    .await;
                // Validate new candle volume
                Some(validate_ftx_candle(&candle, &mut exchange_candles))
            }
            ExchangeName::Gdax => {
                // Get exchange candle
                let mut exchange_candles =
                    get_gdax_candles_daterange::<crate::exchanges::gdax::Candle>(
                        &self.clients[&validation.exchange_name],
                        market,
                        validation.datetime,
                        validation.datetime,
                        86400,
                    )
                    .await;
                // Validate new candle volume
                validate_gdax_candle_by_trade_ids(
                    &self.gdax_pool,
                    &self.clients[&validation.exchange_name],
                    market,
                    &candle,
                    &mut exchange_candles,
                    &TimeFrame::D01,
                    "validated",
                )
                .await
            }
        };
        match is_valid {
            Some(v) => {
                if v {
                    let message = "Re-validation successful.".to_string();
                    // New candle was validated, update new candle
                    self.process_revalidated_01d_candle(validation, candle)
                        .await;
                    // Update validation to complete
                    update_candle_validation_status_processed(&self.ig_pool, validation, &message)
                        .await
                        .expect("Failed to update validation status to done.");
                } else {
                    // Candle was not validated, update to manual and open
                    println!("Failed to validated 01D candle.");
                    update_candle_validations_type_status(
                        &self.ig_pool,
                        validation,
                        ValidationType::Manual,
                        ValidationStatus::Open,
                        "Failed to auto-validate.",
                    )
                    .await
                    .expect("Failed to update validation status.");
                }
            }
            None => {
                // There was no validation completed, return without doing anything
                println!("There was no result from validation, try again.");
            }
        };
    }

    pub async fn manual_validate_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Recreate candle and then compare new candle to exchange candle for validation and return result.
        let candle = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.recreate_ftx_candle(validation, market, validation.datetime)
                    .await
            }
            ExchangeName::Gdax => {
                self.recreate_gdax_candle(validation, market, validation.datetime)
                    .await
            }
        };
        // Set start and end for candle period
        let candle_start = validation.datetime;
        // Present candle data versus exchange data and get input from user to validate or not
        let message = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                // println!(
                //     "New Candle (DT/Close/Count/Volume/Value): {} / {} / {} / {} / {}",
                //     candle.datetime, candle.close, candle.trade_count, candle.volume, candle.value
                // );
                println!("Compare ED Value versus FTX Volume:");
                println!("ELD Value: \t{:?}", candle.value);
                let exchange_candle = get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
                    &self.clients[&validation.exchange_name],
                    market,
                    candle_start,
                    candle_start,
                    900,
                )
                .await
                .pop();
                // There is a chance that the REST API returns an empty list with no candles
                let message = match exchange_candle {
                    Some(ec) => {
                        let delta = candle.value - ec.volume;
                        let percent = delta / ec.volume * dec!(100.0);
                        println!("FTX Volume: \t{:?}", ec.volume);
                        format!(
                            "Delta: ${:?} & Percent: {:?}%",
                            delta.round_dp(2),
                            percent.round_dp(4)
                        )
                    }
                    None => "No FTX Candle returned from API.".to_string(),
                };
                println!("{}", message);
                message
            }
            ExchangeName::Gdax => {
                // println!(
                //     "New Candle (DT/Close/Count/Volume/Value): {} / {} / {} / {} / {}",
                //     candle.datetime, candle.close, candle.trade_count, candle.volume, candle.value
                // );
                println!("Compare ED Volume versus GDAX Volume:");
                println!("ELD Volume: \t{:?}", candle.volume);
                let exchange_candle = get_gdax_candles_daterange::<crate::exchanges::gdax::Candle>(
                    &self.clients[&validation.exchange_name],
                    market,
                    candle_start,
                    candle_start,
                    900,
                )
                .await
                .pop();
                let message = match exchange_candle {
                    Some(ec) => {
                        let delta = candle.volume - ec.volume;
                        let percent = delta / ec.volume * dec!(100.0);
                        println!("Gdax Volume: \t{:?}", ec.volume);
                        format!(
                            "Delta: {:?} & Percent: {:?}%",
                            delta.round_dp(2),
                            percent.round_dp(4)
                        )
                    }
                    None => "No Exchange Candle for cmp.".to_string(),
                };
                println!("{}", message);
                // Get trades from recreated candle to do trade id validation and present info
                let start_ts = validation.datetime;
                let end_ts = start_ts + Duration::seconds(validation.duration);
                let trade_table = format!("qc_{}", validation.validation_type.as_str());
                let mut interval_trades = select_gdax_trades_by_time(
                    &self.gdax_pool,
                    market,
                    &trade_table,
                    start_ts,
                    end_ts,
                )
                .await
                .expect("Could not fetch trades from temp table.");
                if interval_trades.is_empty() {
                    println!("No trades for candle. Printing First/Last from rebuild candle.");
                    println!(
                        "First Id: {} Last Id: {}",
                        candle.first_trade_id, candle.last_trade_id
                    );
                } else {
                    // Sort trades, calc n trades then pull one trade before and after
                    interval_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                    // Unwrap safe because interval trades is not empty
                    let first_trade = interval_trades.first().unwrap();
                    let last_trade = interval_trades.last().unwrap();
                    println!("First: {:?}", first_trade);
                    println!("Last: {:?}", last_trade);
                    println!(
                        "Last - First + 1 = {}",
                        last_trade.trade_id - first_trade.trade_id + 1
                    );
                    println!("N Trades: {}", interval_trades.len());
                    // Check next is one trade if after the last trade
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let next_trade = match self.clients[&ExchangeName::Gdax]
                        .get_gdax_next_trade(
                            market.market_name.as_str(),
                            last_trade.trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return validation unresolved
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    if !next_trade.is_empty() {
                        // Pop off the trade
                        let next_trade = next_trade.first().unwrap();
                        println!(
                            "Next trade id and time: {}, {}",
                            next_trade.trade_id, next_trade.time
                        );
                        // next_trade.time().day() > last_trade.time().day()
                    } else {
                        // If next trade is empty, return false. There should always be a next trade
                        println!("There is no next trade, Vec empty.");
                    };
                    // Check prev is one trade if before the first trade
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let previous_trade = match self.clients[&ExchangeName::Gdax]
                        .get_gdax_previous_trade(
                            market.market_name.as_str(),
                            first_trade.trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return validation unresolved
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    if !previous_trade.is_empty() {
                        // Pop off the trade
                        let previous_trade = previous_trade.first().unwrap();
                        println!(
                            "Next trade id and time: {}, {}",
                            previous_trade.trade_id, previous_trade.time
                        );
                    } else {
                        // If next trade is empty, return false. There should always be a next trade
                        println!("There is no previous trade, Vec empty.");
                    };
                };
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
            update_candle_validation_status_processed(&self.ig_pool, validation, &message)
                .await
                .expect("Failed to update validation status to done.");
        }
        // Drop the validation trade table
        let qc_table = format!(
            "trades_{}_{}_qc_{}",
            validation.exchange_name.as_str(),
            market.as_strip(),
            validation.validation_type.as_str(),
        );
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                drop_table(&self.ftx_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
            ExchangeName::Gdax => {
                drop_table(&self.gdax_pool, &qc_table)
                    .await
                    .expect("Failed to drop qc table.");
            }
        }
    }

    pub async fn manual_validate_01_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
    ) {
        // Get hb candles and resample to daily candle
        let hb_candles = select_candles_by_daterange(
            &self.ig_pool,
            &validation.exchange_name,
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
        // Get hb validations
        let hb_validations = select_candle_validations_for_01d(&self.ig_pool, validation)
            .await
            .expect("Failed to get hb validations.");
        // Set start and end for the candle period
        let candle_start = validation.datetime;
        // Present candle data versus exchange data and get input from user to validate
        let message = match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                // println!(
                //     "New Candle (DT/Close/Count/Volume/Value): {} / {} / {} / {} / {}",
                //     candle.datetime, candle.close, candle.trade_count, candle.volume, candle.value
                // );
                println!("Compare ED Value versus FTX Volume:");
                println!("ELD Value: \t{:?}", candle.value);
                let exchange_candle = get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
                    &self.clients[&validation.exchange_name],
                    market,
                    validation.datetime,
                    validation.datetime,
                    86400,
                )
                .await
                .pop();
                // There is a chance the REST API returns empty list with no candles
                let message = match exchange_candle {
                    Some(ec) => {
                        let delta = candle.value - ec.volume;
                        let percent = delta / ec.volume * dec!(100.0);
                        println!("FTX Volume: \t{:?}", ec.volume);
                        format!(
                            "Delta: ${:?} & Percent: {:?}%",
                            delta.round_dp(2),
                            percent.round_dp(4)
                        )
                    }
                    None => "No FTX candle returned from API.".to_string(),
                };
                println!("{}", message);
                println!("HB Validations:");
                for hbv in hb_validations.iter() {
                    println!("Validation: {:?}", hbv);
                }
                message
            }
            ExchangeName::Gdax => {
                // println!(
                //     "New Candle (DT/Close/Count/Volume/Value): {} / {} / {} / {} / {}",
                //     candle.datetime, candle.close, candle.trade_count, candle.volume, candle.value
                // );
                println!("Compare ED Volume versus GDAX Volume:");
                println!("ELD Volume: \t{:?}", candle.volume);
                let exchange_candle = get_gdax_candles_daterange::<crate::exchanges::gdax::Candle>(
                    &self.clients[&validation.exchange_name],
                    market,
                    validation.datetime,
                    validation.datetime,
                    86400,
                )
                .await
                .pop();
                let message = match exchange_candle {
                    Some(ec) => {
                        let delta = candle.volume - ec.volume;
                        let percent = delta / ec.volume * dec!(100.0);
                        println!("Gdax Volume: \t{:?}", ec.volume);
                        format!(
                            "Delta: {:?} & Percent: {:?}%",
                            delta.round_dp(2),
                            percent.round_dp(4)
                        )
                    }
                    None => "No Exchange Candle for cmp.".to_string(),
                };
                // Get trades from validated table to do trade id validation and present info
                let end_ts = candle_start + Duration::seconds(validation.duration);
                let mut interval_trades = select_gdax_trades_by_time(
                    &self.gdax_pool,
                    market,
                    "validated",
                    candle_start,
                    end_ts,
                )
                .await
                .expect("Failed to fetch trades from validated table.");
                if interval_trades.is_empty() {
                    println!("No trades for candle. Printing First/Last from rebuild candle.");
                    println!(
                        "First Id: {} Last Id: {}",
                        candle.first_trade_id, candle.last_trade_id
                    );
                } else {
                    // Sort trades, calc n trades then pull one trade before and after
                    interval_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                    // Unwrap safe because interval trades is not empty
                    let first_trade = interval_trades.first().unwrap();
                    let last_trade = interval_trades.last().unwrap();
                    println!("First: {:?}", first_trade);
                    println!("Last: {:?}", last_trade);
                    println!(
                        "Last - First + 1 = {}",
                        last_trade.trade_id - first_trade.trade_id + 1
                    );
                    println!("N Trades: {}", interval_trades.len());
                    // Check next is one trade if after the last trade
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let next_trade = match self.clients[&ExchangeName::Gdax]
                        .get_gdax_next_trade(
                            market.market_name.as_str(),
                            last_trade.trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return validation unresolved
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    if !next_trade.is_empty() {
                        // Pop off the trade
                        let next_trade = next_trade.first().unwrap();
                        println!(
                            "Next trade id and time: {}, {}",
                            next_trade.trade_id, next_trade.time
                        );
                        // next_trade.time().day() > last_trade.time().day()
                    } else {
                        // If next trade is empty, return false. There should always be a next trade
                        println!("There is no next trade, Vec empty.");
                    };
                    // Check prev is one trade if before the first trade
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    let previous_trade = match self.clients[&ExchangeName::Gdax]
                        .get_gdax_previous_trade(
                            market.market_name.as_str(),
                            first_trade.trade_id as i32,
                        )
                        .await
                    {
                        Err(RestError::Reqwest(e)) => {
                            if e.is_timeout() || e.is_connect() || e.is_request() {
                                println!(
                                    "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                                    e
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                return; // Return validation unresolved
                            } else if e.is_status() {
                                match e.status() {
                                    Some(s) => match s.as_u16() {
                                        500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                            println!(
                                                "{} status code. Waiting 30 seconds before retry {:?}",
                                                s, e
                                            );
                                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                                30,
                                            ))
                                            .await;
                                            return;
                                            // Leave event incomplete and try to process again
                                        }
                                        _ => {
                                            panic!("Status code not handled: {:?} {:?}", s, e)
                                        }
                                    },
                                    None => panic!("No status code for request error: {:?}", e),
                                }
                            } else {
                                panic!("Error (not timeout / connect / request): {:?}", e)
                            }
                        }
                        Err(e) => panic!("Other RestError: {:?}", e),
                        Ok(result) => result,
                    };
                    if !previous_trade.is_empty() {
                        // Pop off the trade
                        let previous_trade = previous_trade.first().unwrap();
                        println!(
                            "Next trade id and time: {}, {}",
                            previous_trade.trade_id, previous_trade.time
                        );
                    } else {
                        // If next trade is empty, return false. There should always be a next trade
                        println!("There is no previous trade, Vec empty.");
                    };
                };
                println!("{}", message);
                println!("HB Validations:");
                for hbv in hb_validations.iter() {
                    println!("Validation: {:?}", hbv);
                }
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
            update_candle_validation_status_processed(&self.ig_pool, validation, &message)
                .await
                .expect("Failed to update validation status to done.");
        }
    }

    pub async fn recreate_ftx_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
        candle_start: DateTime<Utc>,
    ) -> Candle {
        // Create temp tables to store new trades. Re-download trades for candle timeperiod
        // Return new candle to be evaluated
        let trade_table = format!("qc_{}", validation.validation_type.as_str());
        drop_trade_table(
            &self.ftx_pool,
            &validation.exchange_name,
            market,
            &trade_table,
        )
        .await
        .expect("Failed to drop qc table.");
        create_ftx_trade_table(
            &self.ftx_pool,
            &validation.exchange_name,
            market,
            &trade_table,
        )
        .await
        .expect("Failed to create qc table.");
        // Set start and end for candle period
        let candle_end = candle_start + self.hbtf.as_dur();
        let mut candle_end_or_last_trade = candle_end;
        // Download trades for candle period
        while candle_start < candle_end_or_last_trade {
            // Prevent 429 errors by only requesting 4 per second
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = match self.clients[&validation.exchange_name]
                .get_ftx_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(candle_start),
                    Some(candle_end_or_last_trade),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    continue;
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
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
                    &self.ftx_pool,
                    &validation.exchange_name,
                    market,
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
                    market.as_strip(),
                    validation.validation_type.as_str(),
                );
                let interval_trades = select_ftx_trades_by_table(&self.ftx_pool, qc_table.as_str())
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
            &self.ig_pool,
            &validation.exchange_name,
            &market.market_id,
            candle_start,
            candle_start + self.hbtf.as_dur(),
        )
        .await
        .expect("Failed to select candle from db.")
        .pop()
        .unwrap();
        original_candle
    }

    pub async fn recreate_gdax_candle(
        &self,
        validation: &CandleValidation,
        market: &MarketDetail,
        start_ts: DateTime<Utc>,
    ) -> Candle {
        println!(
            "Recreating GDAX {} candle for {:?}",
            market.market_name, start_ts
        );
        // Get original candle that is not validated
        let original_candle = select_candles_by_daterange(
            &self.ig_pool,
            &validation.exchange_name,
            &market.market_id,
            start_ts,
            start_ts + self.hbtf.as_dur(),
        )
        .await
        .expect("Failed to select candle from db.")
        .pop()
        .unwrap();
        // Create temp tables to store new trades. Re-download trades for candle timeperiod
        // Return new candle to be evaluated
        let trade_table = format!("qc_{}", validation.validation_type.as_str());
        drop_trade_table(
            &self.gdax_pool,
            &validation.exchange_name,
            market,
            &trade_table,
        )
        .await
        .expect("Failed to drop qc table.");
        create_gdax_trade_table(
            &self.gdax_pool,
            &validation.exchange_name,
            market,
            &trade_table,
        )
        .await
        .expect("Failed to create qc table.");
        // Set start and end for candle period
        let mut after_ts = start_ts;
        let end_ts = start_ts + self.hbtf.as_dur();
        let mut start_id = original_candle.first_trade_id.parse::<i32>().unwrap();
        // Set flag that trades have covered start of period
        let mut start_is_covered = false;
        // Download trades for candle period
        while after_ts < end_ts {
            // Prevent 429 errors by only requesting 1 per second
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            let mut new_trades = match self.clients[&validation.exchange_name]
                .get_gdax_trades(
                    market.market_name.as_str(),
                    Some(1000),
                    None,
                    Some(start_id),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    continue;
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            if !new_trades.is_empty() {
                new_trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                // Can unwrap first and last because there is at least one trade
                let oldest_trade = new_trades.first().unwrap();
                let newest_trade = new_trades.last().unwrap();
                // Update start id and ts
                if oldest_trade.time() > start_ts && !start_is_covered {
                    // The earliest trade is greater than the candle start. This can happen if a
                    // candle is not created entirely and the candle first trade is at 00:05 but the
                    // candle begins at 00:00 and there are trades from 00:00 to 00:05. Push the
                    // trade id back until the oldest trade returned is < the candle start ts
                    after_ts = oldest_trade.time();
                    start_id = oldest_trade.trade_id() as i32 - 1000;
                } else {
                    // The earliest trade is greater than the candle start ts or the start of the
                    // candle has been covered. Continue forward filling the trades until completed.
                    after_ts = newest_trade.time();
                    start_id = newest_trade.trade_id() as i32 + 1000;
                    start_is_covered = true;
                }
                println!(
                    "{} {} trades returned. First: {}, Last: {}",
                    new_trades.len(),
                    market.market_name,
                    oldest_trade.time(),
                    newest_trade.time()
                );
                // Temp table name can be used instead of exhcange name as logic for table name is
                // located in the insert function
                insert_gdax_trades(
                    &self.gdax_pool,
                    &validation.exchange_name,
                    market,
                    &trade_table,
                    new_trades,
                )
                .await
                .expect("Failed to insert tmp gdax trades.");
            }
        }
        let interval_trades =
            select_gdax_trades_by_time(&self.gdax_pool, market, &trade_table, start_ts, end_ts)
                .await
                .expect("Could not fetch trades from temp table.");
        // Create candle from interval trades, trades are already sorted and deduped
        // from select query and primary key uniqueness
        if !interval_trades.is_empty() {
            Candle::new_from_trades(market.market_id, start_ts, &interval_trades)
        } else {
            original_candle
        }
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
                delete_trades_by_time(
                    &self.ftx_pool,
                    &validation.exchange_name,
                    market,
                    "processed",
                    validation.datetime,
                    validation.datetime + Duration::seconds(900),
                )
                .await
                .expect("Could not delete processed trades.");
            }
            ExchangeName::Gdax => {
                delete_trades_by_time(
                    &self.gdax_pool,
                    &validation.exchange_name,
                    market,
                    "processed",
                    validation.datetime,
                    validation.datetime + Duration::seconds(900),
                )
                .await
                .expect("Could not delete processed trades.");
            }
        };
        // Delete existing candle
        delete_candle(
            &self.ig_pool,
            &validation.exchange_name,
            &market.market_id,
            &validation.datetime,
        )
        .await
        .expect("Could not delete old candle.");
        // Insert candle with validated status
        insert_candle(
            &self.ig_pool,
            &validation.exchange_name,
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
            market.as_strip(),
            validation.validation_type.as_str(),
        );
        match validation.exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                let validated_trades =
                    select_ftx_trades_by_table(&self.ftx_pool, qc_table.as_str())
                        .await
                        .expect("Failed to select trades from temp table.");
                // Insert trades into _validated
                insert_ftx_trades(
                    &self.ftx_pool,
                    &validation.exchange_name,
                    market,
                    "validated",
                    validated_trades,
                )
                .await
                .expect("Could not insert validated trades.");
            }
            ExchangeName::Gdax => {
                // Gdax needs to grab trades by time as the temp table has trades before and after
                // the start and end to qc and recreate the candle
                let trade_table = format!("qc_{}", validation.validation_type.as_str());
                let validated_trades = select_gdax_trades_by_time(
                    &self.gdax_pool,
                    market,
                    &trade_table,
                    validation.datetime,
                    validation.datetime + Duration::seconds(900),
                )
                .await
                .expect("Failed to select trades from temp table.");
                // Insert trades into _validated
                insert_gdax_trades(
                    &self.gdax_pool,
                    &validation.exchange_name,
                    market,
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
        delete_candle_01d(&self.ig_pool, &validation.market_id, &validation.datetime)
            .await
            .expect("Failed to delete 01D candle.");
        // Insert candle with validated status
        insert_candles_01d(&self.ig_pool, &validation.market_id, &vec![candle], true)
            .await
            .expect("Failed to insert 01D candle.");
    }
}

pub async fn insert_candle_validation(
    pool: &PgPool,
    exchange: &ExchangeName,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
    duration: i64,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO candle_validations (
            exchange_name, market_id, datetime, duration, validation_type, created_ts,
            validation_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (exchange_name, market_id, datetime, duration) DO NOTHING
        "#;
    sqlx::query(sql)
        .bind(exchange.as_str())
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

pub async fn insert_candle_count_validation(
    pool: &PgPool,
    exchange: &ExchangeName,
    market_id: &Uuid,
    datetime: &DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO candle_validations (
            exchange_name, market_id, datetime, duration, validation_type, created_ts,
            validation_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (exchange_name, market_id, datetime, duration) DO NOTHING
        "#;
    sqlx::query(sql)
        .bind(exchange.as_str())
        .bind(market_id)
        .bind(datetime)
        .bind(0_i64)
        .bind(ValidationType::Count.as_str())
        .bind(Utc::now())
        .bind(ValidationStatus::New.as_str())
        .bind("Trade count QC failed on archive.")
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
    use crate::candles::{create_exchange_candle_table, insert_candle, Candle};
    use crate::configuration::get_configuration;
    use crate::exchanges::ExchangeName;
    use crate::inquisidor::Inquisidor;
    use crate::markets::select_market_details;
    use crate::trades::{create_ftx_trade_table, create_gdax_trade_table, drop_trade_table};
    use crate::validation::{
        insert_candle_validation, CandleValidation, ValidationStatus, ValidationType,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;
    use sqlx::PgPool;
    use uuid::Uuid;

    pub async fn prep_ftx_candle_validation(validation: CandleValidation) {
        // Load configuration and db connection to dev
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
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
        create_exchange_candle_table(&pool, &ExchangeName::Ftx)
            .await
            .expect("Failed to create table.");

        // Get markets to extract BTC-PERP market
        let market_details = select_market_details(&pool)
            .await
            .expect("Could not fetch market detail.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "BTC-PERP")
            .unwrap();

        // Create trades table if it does not exist
        drop_trade_table(&pool, &ExchangeName::Ftx, market, "processed")
            .await
            .expect("Failed to drop table.");
        create_ftx_trade_table(&pool, &ExchangeName::Ftx, market, "processed")
            .await
            .expect("Failed to create trade table.");
        drop_trade_table(&pool, &ExchangeName::Ftx, market, "validated")
            .await
            .expect("Failed to drop table.");
        create_ftx_trade_table(&pool, &ExchangeName::Ftx, market, "validated")
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
            &validation.exchange_name,
            &validation.market_id,
            &validation.datetime,
            validation.duration,
        )
        .await
        .expect("Failed to insert candle validation.");
    }

    pub async fn prep_gdax_candle_validation(validation: CandleValidation) {
        // Load configuration and db connection to dev
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.gdax_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Update GDAX BCH-USD market_id to 34266818-0d34-4e71-b9ad-ef93723d7497 to match prod val
        let sql = r#"
            UPDATE markets set market_id = '34266818-0d34-4e71-b9ad-ef93723d7497'
            WHERE market_name = 'BCH-USD'
            AND exchange_name = 'gdax'
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to update id.");

        // Update GDAX BTC-USD market_id to 1fdb5971-fe5b-4db1-8269-8d34d4f1c7d1 to match prod val
        let sql = r#"
            UPDATE markets set market_id = '1fdb5971-fe5b-4db1-8269-8d34d4f1c7d1'
            WHERE market_name = 'BTC-USD'
            AND exchange_name = 'gdax'
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to update id.");

        // Create candles table if it does not exists
        let sql = r#"
            DROP TABLE IF EXISTS candles_15t_gdax
            "#;
        sqlx::query(sql)
            .execute(&pool)
            .await
            .expect("Failed to drop table.");
        create_exchange_candle_table(&pool, &ExchangeName::Gdax)
            .await
            .expect("Failed to create table.");

        // Get markets to extract BTC-PERP market
        let market_details = select_market_details(&pool)
            .await
            .expect("Could not fetch market detail.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "BTC-USD")
            .unwrap();

        // Create trades table if it does not exist
        drop_trade_table(&pool, &ExchangeName::Gdax, market, "processed")
            .await
            .expect("Failed to drop table.");
        create_gdax_trade_table(&pool, &ExchangeName::Gdax, market, "processed")
            .await
            .expect("Failed to create trade table.");
        drop_trade_table(&pool, &ExchangeName::Gdax, market, "validated")
            .await
            .expect("Failed to drop table.");
        create_gdax_trade_table(&pool, &ExchangeName::Gdax, market, "validated")
            .await
            .expect("Failed to create trade table.");

        // Get markets to extract BCH-PERP market
        let market_details = select_market_details(&pool)
            .await
            .expect("Could not fetch market detail.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "BCH-USD")
            .unwrap();

        // Create trades table if it does not exist
        drop_trade_table(&pool, &ExchangeName::Gdax, market, "processed")
            .await
            .expect("Failed to drop table.");
        create_gdax_trade_table(&pool, &ExchangeName::Gdax, market, "processed")
            .await
            .expect("Failed to create trade table.");
        drop_trade_table(&pool, &ExchangeName::Gdax, market, "validated")
            .await
            .expect("Failed to drop table.");
        create_gdax_trade_table(&pool, &ExchangeName::Gdax, market, "validated")
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
            &validation.exchange_name,
            &validation.market_id,
            &validation.datetime,
            validation.duration,
        )
        .await
        .expect("Failed to insert candle validation.");

        // Insert candle - needed for GDAX recreation to get trade id to start with
        let candle = Candle {
            datetime: Utc.ymd(2021, 12, 18).and_hms(7, 45, 0),
            open: dec!(46149),
            high: dec!(46268),
            low: dec!(45778),
            close: dec!(45889),
            volume: dec!(1559.2075),
            volume_net: dec!(-333.4519),
            volume_liquidation: dec!(0.5508),
            value: dec!(71748093.1406),
            trade_count: 9801,
            liquidation_count: 20,
            last_trade_ts: Utc.ymd(2021, 12, 18).and_hms(7, 59, 43),
            last_trade_id: "252564379".to_string(),
            first_trade_ts: Utc.ymd(2021, 12, 18).and_hms(7, 45, 1),
            first_trade_id: "252562781".to_string(),
            is_validated: false,
            market_id: Uuid::parse_str("1fdb5971-fe5b-4db1-8269-8d34d4f1c7d1").unwrap(),
        };

        insert_candle(
            &pool,
            &ExchangeName::Gdax,
            &Uuid::parse_str("1fdb5971-fe5b-4db1-8269-8d34d4f1c7d1").unwrap(),
            candle,
            true,
        )
        .await
        .expect("Failed to insert hb candle.");

        // Insert candle - needed for GDAX recreation to get trade id to start with
        let candle = Candle {
            datetime: Utc.ymd(2021, 12, 18).and_hms(7, 45, 0),
            open: dec!(46149),
            high: dec!(46268),
            low: dec!(45778),
            close: dec!(45889),
            volume: dec!(1559.2075),
            volume_net: dec!(-333.4519),
            volume_liquidation: dec!(0.5508),
            value: dec!(71748093.1406),
            trade_count: 9801,
            liquidation_count: 20,
            last_trade_ts: Utc.ymd(2021, 12, 18).and_hms(7, 59, 43),
            last_trade_id: "252564379".to_string(),
            first_trade_ts: Utc.ymd(2021, 12, 18).and_hms(7, 45, 1),
            first_trade_id: "252562781".to_string(),
            is_validated: false,
            market_id: Uuid::parse_str("34266818-0d34-4e71-b9ad-ef93723d7497").unwrap(),
        };

        insert_candle(
            &pool,
            &ExchangeName::Gdax,
            &Uuid::parse_str("34266818-0d34-4e71-b9ad-ef93723d7497").unwrap(),
            candle,
            true,
        )
        .await
        .expect("Failed to insert hb candle.");
    }

    #[tokio::test]
    pub async fn auto_revalidate_ftx_candle() {
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
        prep_ftx_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }

    #[tokio::test]
    pub async fn auto_revalidate_gdax_candle() {
        // Create validation that can be auto revalidated
        let validation = CandleValidation {
            exchange_name: ExchangeName::Gdax,
            market_id: Uuid::parse_str("1fdb5971-fe5b-4db1-8269-8d34d4f1c7d1").unwrap(),
            datetime: Utc.ymd(2021, 12, 18).and_hms(7, 45, 00),
            duration: 900,
            validation_type: ValidationType::Auto,
            created_ts: Utc::now(),
            processed_ts: None,
            validation_status: ValidationStatus::New,
            notes: None,
        };
        prep_gdax_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }

    #[tokio::test]
    pub async fn manual_revalidate_ftx_candle() {
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
        prep_ftx_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
        println!("Sleeping 5 seconds before starting manual validation.");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        ig.process_candle_validations(ValidationStatus::Open).await;
    }

    #[tokio::test]
    pub async fn manual_revalidate_gdax_candle() {
        // Create validation that can be auto revalidated
        let validation = CandleValidation {
            exchange_name: ExchangeName::Gdax,
            market_id: Uuid::parse_str("34266818-0d34-4e71-b9ad-ef93723d7497").unwrap(),
            datetime: Utc.ymd(2022, 6, 20).and_hms(0, 0, 00),
            duration: 900,
            validation_type: ValidationType::Auto,
            created_ts: Utc::now(),
            processed_ts: None,
            validation_status: ValidationStatus::New,
            notes: None,
        };
        prep_gdax_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }

    #[tokio::test]
    pub async fn auto_revalidate_01d_ftx_candle_no_heartbeats() {
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
        prep_ftx_candle_validation(validation).await;
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        ig.process_candle_validations(ValidationStatus::New).await;
    }

    #[tokio::test]
    pub async fn manual_revalidate_01d_candle_w_bad_heartbeatsy() {
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
        prep_ftx_candle_validation(validation).await;
        // Insert hb candles into db
        let candle = Candle {
            datetime: Utc.ymd(2021, 12, 18).and_hms(0, 0, 0),
            open: dec!(46149),
            high: dec!(46268),
            low: dec!(45778),
            close: dec!(45889),
            volume: dec!(1559.2075),
            volume_net: dec!(-333.4519),
            volume_liquidation: dec!(0.5508),
            value: dec!(71748093.1406),
            trade_count: 9801,
            liquidation_count: 20,
            last_trade_ts: Utc.ymd(2021, 12, 18).and_hms(0, 0, 0),
            last_trade_id: "123".to_string(),
            first_trade_ts: Utc.ymd(2021, 12, 18).and_hms(0, 0, 0),
            first_trade_id: "567".to_string(),
            is_validated: false,
            market_id: Uuid::parse_str("b3bf21db-92bb-4613-972a-1d0f1aab1e95").unwrap(),
        };
        // Create ig instance and process new validation
        let ig = Inquisidor::new().await;
        insert_candle(
            &ig.ig_pool,
            &ExchangeName::Ftx,
            &Uuid::parse_str("b3bf21db-92bb-4613-972a-1d0f1aab1e95").unwrap(),
            candle,
            true,
        )
        .await
        .expect("Failed to insert hb candle.");
        ig.process_candle_validations(ValidationStatus::New).await;
        println!("Sleeping 5 seconds before starting manual validation.");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        ig.process_candle_validations(ValidationStatus::Open).await;
    }
}
