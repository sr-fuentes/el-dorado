use serde::{Deserialize, Serialize};
use rust_decimal::prelude::*;

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all="snake_case")]
pub struct Product {
    pub id: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub base_min_size: Decimal,
    pub base_max_size: Decimal,
    pub quote_increment: Decimal,
    pub base_increment: Decimal,
    pub display_name: String,
    pub min_market_funds: Decimal, // Can this be int?
    pub max_market_funds: Decimal, // Can this be int?
    pub margin_enabled: bool,
    pub fx_stablecoind: Option<bool>,
    pub max_slippage_percentage: Option<Decimal>,
    pub post_only: bool,
    pub limit_only: bool,
    pub cancel_only: bool,
    pub trading_disabled: Option<bool>,
    pub status: String,
    pub status_message: String,
    pub auction_mode: bool,

}