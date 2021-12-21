use sqlx::PgPool;
use std::convert::TryFrom;

#[derive(Debug, Clone)]
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