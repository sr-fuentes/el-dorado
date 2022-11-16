use crate::exchanges::{client::RestClient, error::RestError};
use crate::markets::MarketDetail;
use crate::trades::PrIdTi;
use async_trait::async_trait;
use chrono::{serde::ts_seconds, DateTime, Duration, DurationRound, Utc};
use rust_decimal::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Product {
    pub id: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub base_min_size: Option<Decimal>,
    pub base_max_size: Option<Decimal>,
    pub quote_increment: Decimal,
    pub base_increment: Decimal,
    pub display_name: String,
    pub min_market_funds: Decimal,         // Can this be int?
    pub max_market_funds: Option<Decimal>, // Can this be int?
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

impl crate::utilities::Market for Product {
    fn name(&self) -> String {
        self.id.clone()
    }
    fn market_type(&self) -> String {
        "spot".to_string() // GDAX markets are all spot as of 2/2022
    }
    fn dp_quantity(&self) -> i32 {
        crate::utilities::min_to_dp(self.base_increment)
    }
    fn dp_price(&self) -> i32 {
        crate::utilities::min_to_dp(self.quote_increment)
    }
    fn min_quantity(&self) -> Option<Decimal> {
        self.base_min_size
    }
    fn base_currency(&self) -> Option<String> {
        Some(self.base_currency.clone())
    }
    fn quote_currency(&self) -> Option<String> {
        Some(self.quote_currency.clone())
    }
    fn underlying(&self) -> Option<String> {
        None
    }
    fn usd_volume_24h(&self) -> Option<Decimal> {
        None
    }
}

#[derive(Clone, Deserialize, Serialize, Debug, sqlx::FromRow)]
#[serde(rename_all = "snake_case")]
pub struct Trade {
    pub trade_id: i64,
    pub side: String,
    #[serde(alias = "last_size")]
    pub size: Decimal,
    pub price: Decimal,
    pub time: DateTime<Utc>,
}

#[async_trait]
impl crate::trades::Trade for Trade {
    fn trade_id(&self) -> i64 {
        self.trade_id
    }

    fn price(&self) -> Decimal {
        self.price
    }

    fn size(&self) -> Decimal {
        self.size
    }

    fn side(&self) -> String {
        self.side.clone()
    }

    fn liquidation(&self) -> bool {
        false
    }

    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: Some(self.trade_id),
            dt: self.time,
            price: Some(self.price),
        }
    }

    async fn create_table(
        pool: &PgPool,
        market: &MarketDetail,
        dt: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS trades.{}_{}_{} (
                market_id uuid NOT NULL,
                trade_id BIGINT NOT NULL,
                PRIMARY KEY (trade_id),
                price NUMERIC NOT NULL,
                size NUMERIC NOT NULL,
                side TEXT NOT NULL,
                time timestamptz NOT NULL
            )
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        let index_sql = format!(
            r#"
            CREATE INDEX IF NOT EXISTS {e}_{m}_{t}_time_asc
            ON trades.{e}_{m}_{t} (time)
            "#,
            e = market.exchange_name.as_str(),
            m = market.as_strip(),
            t = dt.format("%Y%m%d")
        );
        sqlx::query(&table_sql).execute(pool).await?;
        sqlx::query(&index_sql).execute(pool).await?;
        Ok(())
    }

    async fn insert(&self, pool: &PgPool, market: &MarketDetail) -> Result<(), sqlx::Error> {
        let insert_sql = format!(
            r#"
            INSERT INTO trades.{}_{}_{} (
                market_id, trade_id, price, size, side, time)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (trade_id) DO NOTHING
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            self.time()
                .duration_trunc(Duration::days(1))
                .unwrap()
                .format("%Y%m%d")
        );
        sqlx::query(&insert_sql)
            .bind(market.market_id)
            .bind(self.trade_id)
            .bind(self.price)
            .bind(self.size)
            .bind(self.side.clone())
            .bind(self.time)
            .execute(pool)
            .await?;
        Ok(())
    }
}

impl Trade {
    // Select the first trade greater than the given date time. Assume and check that the first
    // trade will occur on the same day as the datetime. If there is no trade, try checking the next
    // day. This can occur in edge cases where the given datetime is near the end of the day and
    // there are no further trades for the day and the next trade occurs the next day.
    pub async fn select_one_gt_dt(
        pool: &PgPool,
        market: &MarketDetail,
        dt: DateTime<Utc>,
    ) -> Result<Self, sqlx::Error> {
        let d1 = dt.duration_trunc(Duration::days(1)).unwrap();
        let d2 = d1 + Duration::days(1);
        let sql = format!(
            r#"
            SELECT trade_id, price, size, side, time
            FROM trades.{e}_{m}_{d1}
            WHERE time > $1
            UNION
            SELECT trade_id, price, size, side, time
            FROM trades.{e}_{m}_{d2}
            ORDER BY trade_id ASC
            "#,
            e = market.exchange_name.as_str(),
            m = market.as_strip(),
            d1 = d1.format("%Y%m%d"),
            d2 = d2.format("%Y%m%d"),
        );
        // Try current day table for trade
        let row = sqlx::query_as::<_, Trade>(&sql)
            .bind(dt)
            .fetch_one(pool)
            .await?;
        Ok(row)
    }

    pub async fn select_all(
        pool: &PgPool,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT trade_id, price, size, side, time
            FROM trades.{}_{}_{}
            ORDER BY trade_id ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        let rows = sqlx::query_as::<_, Trade>(&sql).fetch_all(pool).await?;
        Ok(rows)
    }
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

impl crate::candles::Candle for Candle {
    fn datetime(&self) -> DateTime<Utc> {
        self.time
    }

    fn volume(&self) -> Decimal {
        self.volume
    }
}

impl RestClient {
    pub async fn get_gdax_products<T: DeserializeOwned>(&self) -> Result<Vec<T>, RestError> {
        self.get("/products", None).await
    }

    pub async fn get_gdax_product(&self, product_name: &str) -> Result<Product, RestError> {
        self.get(&format!("/products/{}", product_name), None).await
    }

    pub async fn get_gdax_trades(
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

    // Get the next trade AFTER the given trade id for a product
    pub async fn get_gdax_next_trade(
        &self,
        product_name: &str,
        after: i32,
    ) -> Result<Vec<Trade>, RestError> {
        self.get_gdax_trades(product_name, Some(1), None, Some(after + 2))
            .await
    }

    // Get the next trade AFTER the given trade id for a product
    pub async fn get_gdax_previous_trade(
        &self,
        product_name: &str,
        after: i32,
    ) -> Result<Vec<Trade>, RestError> {
        self.get_gdax_trades(product_name, Some(1), None, Some(after))
            .await
    }

    // API will return 300 candles maximum, if start and end are used, both fields need to be
    // provided. Granularity can be 60, 300, 900, 3600, 21600, 86400 only. If there are no trades
    // in a bucket there will be no candle returned. Start and End are inclusive. To get one candle
    // set Start = End
    pub async fn get_gdax_candles<T: DeserializeOwned>(
        &self,
        product_name: &str,
        granularity: Option<i32>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Vec<T>, RestError> {
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
    use crate::exchanges::{client::RestClient, ExchangeName};
    use chrono::{TimeZone, Utc};

    #[tokio::test]
    async fn get_product_returns_all_products() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let products = client
            .get_gdax_products::<crate::exchanges::gdax::Product>()
            .await
            .expect("Failed to get all products.");
        println!("Products: {:?}", products)
    }

    #[tokio::test]
    async fn get_products_returns_specific_product() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "BTC-USD";
        let product = client
            .get_gdax_product(&product_name)
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
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "BTC-USD";
        let trades = client
            .get_gdax_trades(&product_name, None, None, None)
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Trades: {:?}", trades)
    }

    #[tokio::test]
    async fn get_trades_after_returns_array_of_trades() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "BTC-USD";
        let trades = client
            .get_gdax_trades(&product_name, None, None, Some(375128017))
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Trades: {:?}", trades);
        println!("N Trades: {:?}", trades.len());
    }

    #[tokio::test]
    async fn get_trades_before_after_comp() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "AAVE-USD";
        let before_trades = client
            .get_gdax_trades(&product_name, Some(5), Some(13183395), None)
            .await
            .expect("Failed to get before trades.");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let after_trades = client
            .get_gdax_trades(&product_name, Some(5), None, Some(13183395))
            .await
            .expect("Failed to get before trades.");
        println!("Getting AAVE-PERP trades before and after trade id 13183395");
        println!("Before trades:");
        for bt in before_trades.iter() {
            println!("{:?}", bt);
        }
        println!("After trades:");
        for at in after_trades.iter() {
            println!("{:?}", at);
        }
    }

    #[tokio::test]
    async fn get_next_trade_returns_next_trade_id() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "AAVE-USD";
        let trade_id = 17637569;
        let next_trade = client
            .get_gdax_next_trade(product_name, trade_id)
            .await
            .expect("Failed to get next trade.");
        println!("{} trade. Next: {:?}", trade_id, next_trade);
    }

    #[tokio::test]
    async fn get_previous_trade_returns_next_trade_id() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "AAVE-USD";
        let trade_id = 17637569;
        let next_trade = client
            .get_gdax_previous_trade(product_name, trade_id)
            .await
            .expect("Failed to get next trade.");
        println!("{} trade. Next: {:?}", trade_id, next_trade);
    }

    #[tokio::test]
    async fn get_candles_returns_array_of_candles() {
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "BTC-USD";
        let _candles = client
            .get_gdax_candles::<crate::exchanges::gdax::Candle>(
                &product_name,
                Some(86400),
                None,
                None,
            )
            .await
            .expect("Failed to get BTC-USD product.");
        // println!::<T>("Candles: {:?}", candles)
    }

    #[tokio::test]
    async fn get_candles_daterange_returns_array_of_candles() {
        // Start and end are inclusive. For 1 candle set start = end
        let client = RestClient::new(&ExchangeName::Gdax);
        let product_name = "ATOM-USD";
        let candles = client
            .get_gdax_candles::<crate::exchanges::gdax::Candle>(
                &product_name,
                Some(900),
                Some(Utc.ymd(2021, 11, 15).and_hms(21, 0, 0)),
                Some(Utc.ymd(2021, 11, 15).and_hms(21, 0, 0)),
            )
            .await
            .expect("Failed to get BTC-USD product.");
        println!("Candles: {:?}", candles)
    }
}
