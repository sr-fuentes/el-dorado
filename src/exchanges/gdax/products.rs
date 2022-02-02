use super::{RestClient, RestError};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

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

impl RestClient {
    pub async fn get_products(&self) -> Result<Vec<Product>, RestError> {
        self.get("/products", None).await
    }

    pub async fn get_product(&self, product_name: &str) -> Result<Product, RestError> {
        self.get(&format!("/products/{}", product_name), None).await
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::gdax::*;

    #[tokio::test]
    async fn get_product_returns_all_products() {
        let client = RestClient::new();
        let products = client.get_products().await.expect("Failed to get all products.");
        println!("Products: {:?}", products) 
    }

    #[tokio::test]
    async fn get_products_returns_specific_product() {
        let client = RestClient::new();
        let product_name = "BTC-USD";
        let product = client.get_product(&product_name).await.expect("Failed to get BTC-USD product.");
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
}