use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize, Debug, sqlx::FromRow)]
pub struct Trade {
    #[serde(rename = "T")]
    #[serde(with = "ts_milliseconds")]
    pub time: DateTime<Utc>,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "v")]
    pub size: Decimal,
    #[serde(rename = "p")]
    pub price: Decimal,
    #[serde(rename = "L")]
    pub tick: String,
    #[serde(rename = "i")]
    pub trade_id: Uuid,
    #[serde(rename = "BT")]
    pub block_trade: bool,
}
