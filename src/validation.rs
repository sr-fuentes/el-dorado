use sqlx::PgPool;
use std::convert::TryFrom;

#[derive(Debug, Clone, sqlx::Type)]
pub struct CandleValidation {
    pub exchange_name: ExchangeName,
    pub market_name: String,
    pub datetime: DateTime<Utc>,
    pub duration: i32,
    pub validation_type: ValidationType,
    pub created_ts: DateTime<Utc>,
    pub processed_ts: Option<Datetime<Utc>>,
    pub validation_status: ValidationStatus,
    pub notes: Option<String>,
}
