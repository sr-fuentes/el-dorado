use super::{RestClient, RestError};
use chrono::{serde::ts_seconds, DateTime, Utc};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
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
    pub fx_stablecoin: Option<bool>,
    pub max_slippage_percentage: Option<Decimal>,
    pub post_only: bool,
    pub limit_only: bool,
    pub cancel_only: bool,
    pub trading_disabled: Option<bool>,
    pub status: String,
    pub status_message: String,
    pub auction_mode: bool,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Trade {
    pub trade_id: i64,
    pub side: String,
    pub size: Decimal,
    pub price: Decimal,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Candle {
    #[serde(with = "ts_seconds")]
    pub time: DateTime<Utc>,
    pub low: Decimal,
    pub high: Decimal,
    pub open: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl RestClient {
    pub async fn get_products(&self) -> Result<Vec<Product>, RestError> {
        self.get("/products", None).await
    }

    pub async fn get_product(&self, product_name: &str) -> Result<Product, RestError> {
        self.get(&format!("/products/{}", product_name), None).await
    }

    pub async fn get_trades(
        &self,
        product_name: &str,
        limit: Option<i32>,
        before: Option<i32>,
        after: Option<i32>,
    ) -> Result<Vec<Trade>, RestError> {
        self.get(
            &format!("/products/{}/trades", product_name),
            Some(json!({
                "limit": limit,
                "before": before,
                "after": after,
            })),
        )
        .await
    }

    // API will return 300 candles maximum, if start and end are used, both fields need to be
    // provided. Granularity can be 60, 300, 900, 3600, 21600, 86400 only. If there are no trades
    // in a bucket there will be no candle returned. Start and End are inclusive. To get one candle
    // set Start = End
    pub async fn get_candles(
        &self,
        product_name: &str,
        granularity: Option<i32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<Candle>, RestError> {
        self.get(
            &format!("/products/{}/candles", product_name),
            Some(json!({
                "granularity": granularity,
                "start": start.map(|t| t.format("%+").to_string()),
                "end": end.map(|t| t.format("%+").to_string()),
            })),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::gdax::*;
    use chrono::{TimeZone, Utc};

    #[tokio::test]
    async fn get_product_returns_all_products() {
        let client = RestClient::new();
        let products = client
            .get_products()
            .await
            .expect("Failed to get all products.");
        println!("Products: {:?}", products)
    }

    #[tokio::test]
    async fn get_products_returns_specific_product() {
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let product = client
            .get_product(&product_name)
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Product: {:?}", product)
    }

    #[tokio::test]
    async fn reqwest_to_gdax_works() {
        let response = reqwest::get("https://api.pro.coinbase.com/products")
            .await
            // each response is wrapped in a `Result` type
            // we'll unwrap here for simplicity
            .unwrap()
            .text()
            .await;
        println!("{:?}", response);
    }

    #[tokio::test]
    async fn get_trades_returns_array_of_trades() {
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let trades = client
            .get_trades(&product_name, None, None, None)
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Trades: {:?}", trades)
    }

    #[tokio::test]
    async fn get_trades_after_returns_array_of_trades() {
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let trades = client
            .get_trades(&product_name, None, None, Some(375128017))
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Trades: {:?}", trades);
        println!("N Trades: {:?}", trades.len());
    }

    #[tokio::test]
    async fn get_candles_returns_array_of_candles() {
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let candles = client
            .get_candles(&product_name, Some(86400), None, None)
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Candles: {:?}", candles)
    }

    #[tokio::test]
    async fn get_candles_daterange_returns_array_of_candles() {
        // Start and end are inclusive. For 1 candle set start = end
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let candles = client
            .get_candles(
                &product_name,
                Some(86400),
                Some(Utc.ymd(2022, 1, 1).and_hms(0, 0, 0)),
                Some(Utc.ymd(2022, 1, 1).and_hms(0, 0, 0)),
            )
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Candles: {:?}", candles)
    }
}
