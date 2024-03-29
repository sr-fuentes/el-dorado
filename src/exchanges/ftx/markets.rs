use crate::exchanges::{client::RestClient, error::RestError};
use crate::markets::MarketDetail;
use crate::trades::PrIdTi;
use async_trait::async_trait;
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub name: String,
    pub enabled: bool,
    pub post_only: bool,
    pub price_increment: Decimal,
    pub size_increment: Decimal,
    pub min_provide_size: Decimal,
    pub last: Option<f64>,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub price: Option<f64>,
    #[serde(rename = "type")]
    pub market_type: String,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub underlying: Option<String>,
    pub restricted: bool,
    pub high_leverage_fee_exempt: bool,
    pub change1h: f64,
    pub change24h: f64,
    pub change_bod: f64,
    pub quote_volume24h: f64,
    pub volume_usd24h: Decimal,
}

impl crate::utilities::Market for Market {
    fn name(&self) -> String {
        self.name.clone()
    }
    fn market_type(&self) -> String {
        self.market_type.clone()
    }
    fn dp_quantity(&self) -> i32 {
        crate::utilities::min_to_dp(self.size_increment)
    }
    fn dp_price(&self) -> i32 {
        crate::utilities::min_to_dp(self.price_increment)
    }
    fn min_quantity(&self) -> Option<Decimal> {
        Some(self.min_provide_size)
    }
    fn base_currency(&self) -> Option<String> {
        self.base_currency.clone()
    }
    fn quote_currency(&self) -> Option<String> {
        self.quote_currency.clone()
    }
    fn underlying(&self) -> Option<String> {
        self.underlying.clone()
    }
    fn usd_volume_24h(&self) -> Option<Decimal> {
        Some(self.volume_usd24h)
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Orderbook {
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
}

#[derive(Clone, Deserialize, Serialize, Debug, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub id: i64,
    pub price: Decimal,
    pub size: Decimal,
    pub side: String,
    pub liquidation: bool,
    pub time: DateTime<Utc>,
}

#[async_trait]
impl crate::trades::Trade for Trade {
    fn trade_id(&self) -> i64 {
        self.id
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
        self.liquidation
    }

    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.id,
            dt: self.time,
            price: self.price,
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
                liquidation BOOLEAN NOT NULL,
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
                market_id, trade_id, price, size, side, liquidation, time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
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
            .bind(self.id)
            .bind(self.price)
            .bind(self.size)
            .bind(self.side.clone())
            .bind(self.liquidation)
            .bind(self.time)
            .execute(pool)
            .await?;
        Ok(())
    }

    async fn drop_table(
        pool: &PgPool,
        market: &MarketDetail,
        dt: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let sql = format!(
            r#"
            DROP TABLE IF EXISTS trades.{}_{}_{}
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        sqlx::query(&sql).execute(pool).await?;
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
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM trades.{e}_{m}_{d1}
            WHERE time > $1
            UNION
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM trades.{e}_{m}_{d2}
            ORDER BY id ASC
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
        // Ok(Box::new(row))
        Ok(row)
    }

    pub async fn select_all(
        pool: &PgPool,
        market: &MarketDetail,
        dt: &DateTime<Utc>,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let sql = format!(
            r#"
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM trades.{}_{}_{}
            ORDER BY id ASC
            "#,
            market.exchange_name.as_str(),
            market.as_strip(),
            dt.format("%Y%m%d")
        );
        let rows = sqlx::query_as::<_, Trade>(&sql).fetch_all(pool).await?;
        Ok(rows)
    }
}

#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    pub start_time: DateTime<Utc>,
    #[serde(with = "ts_micro_fractions")]
    pub time: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl crate::candles::Candle for Candle {
    fn datetime(&self) -> DateTime<Utc> {
        self.start_time
    }

    fn close(&self) -> Decimal {
        self.close
    }

    fn volume(&self) -> Decimal {
        self.volume
    }
}

mod ts_micro_fractions {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micro_fracts = f64::deserialize(d)?;
        let nts = (micro_fracts * 1_000_f64) as i64 * 1_000;
        Ok(Utc.timestamp_nanos(nts))
    }
}

impl RestClient {
    // Add `/market` specific API endpoints
    pub async fn get_ftx_markets<T: DeserializeOwned>(&self) -> Result<Vec<T>, RestError> {
        self.get("/markets", None).await
    }

    pub async fn get_ftx_market(&self, market_name: &str) -> Result<Market, RestError> {
        self.get(&format!("/markets/{}", market_name), None).await
    }

    pub async fn get_ftx_orderbook(
        &self,
        market_name: &str,
        depth: Option<u32>,
    ) -> Result<Orderbook, RestError> {
        self.get(
            &format!("/markets/{}/orderbook", market_name),
            Some(json!({
                "depth": depth, // Max 100, default 20. API will cap at 100 if higher value supplied
            })),
        )
        .await
    }

    pub async fn get_ftx_trades(
        &self,
        market_name: &str,
        limit: Option<u32>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<Trade>, RestError> {
        self.get(
            &format!("/markets/{}/trades", market_name),
            Some(json!({
                "limit": limit, // Supports pagination for values over 100
                "start_time": start_time.map(|t| t.timestamp()), // API takes time in unix seconds
                "end_time": end_time.map(|t| Decimal::new(t.timestamp() * 1000000 + t.timestamp_subsec_micros() as i64, 6)), // API takes time in unix seconds
            })),
        )
        .await
    }

    pub async fn get_ftx_candles<T: DeserializeOwned>(
        &self,
        market_name: &str,
        resolution: Option<i32>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<T>, RestError> {
        self.get(
            &format!("/markets/{}/candles", market_name),
            Some(json!({
                "resolution": resolution, // Window length in seconds (15, 60, 300, 900, 3600, 14400, 86400)
                "start_time": start_time.map(|t| t.timestamp()), // Timestamp in seconds
                "end_time": end_time.map(|t| t.timestamp()), // Timestamp in seconds
            })),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::{
        client::RestClient, ftx::Candle, ftx::Market, ftx::Orderbook, ftx::Trade, ExchangeName,
    };
    use chrono::{TimeZone, Utc};

    #[test]
    fn serde_deserializes_the_market_struct() {
        let market_json = r#"
        {
            "name":"AAVE/USD",
            "enabled":true,
            "postOnly":false,
            "priceIncrement":0.01,
            "sizeIncrement":0.01,
            "minProvideSize":0.01,
            "last":303.56,
            "bid":304.98,
            "ask":305.21,
            "price":304.98,
            "type":"spot",
            "baseCurrency":"AAVE",
            "quoteCurrency":"USD",
            "underlying":null,
            "restricted":false,
            "highLeverageFeeExempt":true,
            "change1h":-0.00029501425902251943,
            "change24h":-0.011025358324145534,
            "changeBod":-0.025186984593748,
            "quoteVolume24h":224063.1118,
            "volumeUsd24h":224063.1118
        }
        "#;
        let deserialized: Market = serde_json::from_str(market_json).unwrap();
        println!("deserialized: {:?}", deserialized);
    }

    #[tokio::test]
    async fn get_markets_returns_all_markets() {
        let client = RestClient::new(&ExchangeName::FtxUs);
        let _markets = client
            .get_ftx_markets::<crate::exchanges::ftx::Market>()
            .await
            .expect("Failed to get markets.");
    }

    #[tokio::test]
    async fn get_market_returns_specific_market() {
        let client = RestClient::new(&ExchangeName::Ftx);
        let market_name = "BTC-PERP ";
        let market = client
            .get_ftx_market(&market_name)
            .await
            .expect("Failed to get BTC/USD market.");
        println!("{:?}", market);
        assert_eq!(market.name, market_name);
    }

    #[test]
    fn serde_deserializes_the_orderbook_struct() {
        let orderbook_json = r#"
        {
            "bids": [
                [39790.0, 0.2513],
                [39787.0, 0.3205],
                [39778.0, 0.39],
                [39777.0, 4.2278],
                [39774.0, 0.0503],
                [39770.0, 0.2514],
                [39761.0, 0.0528],
                [39759.0, 0.2513],
                [39755.0, 13.3299],
                [39731.0, 0.056],
                [39711.0, 1.0],
                [39687.0, 0.0596],
                [39666.0, 12.6464],
                [39647.0, 0.0008],
                [39646.0, 22.4362],
                [39631.0, 0.0634],
                [39618.0, 41.2044],
                [39606.0, 19.4511],
                [39597.0, 23.0947],
                [39594.0, 18.1437]
            ],
            "asks": [
                [39800.0, 3.7185],
                [39805.0, 0.2513],
                [39813.0, 0.0527],
                [39814.0, 13.7056],
                [39821.0, 0.2514],
                [39824.0, 0.0527],
                [39837.0, 0.2513],
                [39850.0, 0.0003],
                [39853.0, 0.0527],
                [39896.0, 15.9235],
                [39898.0, 19.312],
                [39899.0, 18.25],
                [39900.0, 0.0003],
                [39915.0, 28.6153],
                [39932.0, 0.0629],
                [39950.0, 0.0003],
                [39955.0, 0.0629],
                [39994.0, 25.688],
                [40000.0, 0.0398],
                [40011.0, 28.1362]
            ]
        }
        "#;
        let deserialized: Orderbook = serde_json::from_str(orderbook_json).unwrap();
        println!("deserialized: {:?}", deserialized);
    }

    #[tokio::test]
    async fn get_orderbook_without_params_returns_orderbook() {
        let client = RestClient::new(&ExchangeName::FtxUs);
        let orderbook = client
            .get_ftx_orderbook("BTC/USD", None)
            .await
            .expect("Failed to get orderbook.");
        println!("Orderbook: {:?}", orderbook);
    }

    #[tokio::test]
    async fn get_orderbook_with_params_returns_orderbook() {
        let client = RestClient::new(&ExchangeName::FtxUs);
        let orderbook = client
            .get_ftx_orderbook("BTC/USD", Some(300))
            .await
            .expect("Failed to load BTC/USD orderbook.");
        println!("Orderbook: {:?}", orderbook);
    }

    #[test]
    fn serde_deserializes_the_trade_struct() {
        let trade_json = r#"
        {
            "id": 6371425,
            "price": 39737.0,
            "size": 0.0006,
            "side": "sell",
            "liquidation": false,
            "time": "2021-08-02T03:37:32.722725+00:00"
        }
        "#;
        let deserialized: Trade = serde_json::from_str(trade_json).unwrap();
        println!("deserialized: {:?}", deserialized);
        let ts = deserialized.time;
        println!("Timestamp seconds: {:?}", ts.timestamp());
        println!("Timestamp micros: {:?}", ts.timestamp_subsec_micros());
        println!("Timestamp nanos: {:?}", ts.timestamp_subsec_nanos());
        println!("Timestamp millis: {:?}", ts.timestamp_subsec_millis());
    }

    #[tokio::test]
    async fn get_trades_without_params_returns_trades() {
        let client = RestClient::new(&ExchangeName::FtxUs);
        let trades = client
            .get_ftx_trades("BTC/USD", None, None, None)
            .await
            .expect("Failed to load BTC/USD trades.");
        println!("Trades: {:?}", trades);
    }

    #[tokio::test]
    async fn get_trades_with_params_returns_trades() {
        let client = RestClient::new(&ExchangeName::FtxUs);
        let trades = client
            .get_ftx_trades(
                "SOL/USD",
                Some(100),
                None,
                Some(Utc.timestamp_opt(1631666701, 0).unwrap()),
            )
            .await
            .expect("Failed to get last 10 BTC/USD trades.");
        println!("Trades: {:?}", trades);
    }

    #[test]
    fn serde_deserializes_the_candle_struct() {
        let candles_json = r#"
        {
            "startTime": "2020-03-24T00:00:00+00:00",
            "time": 1585008000000.0,
            "open": 6399.75,
            "high": 6399.75,
            "low": 6395.0,
            "close": 6399.75,
            "volume": 0.0
        }
        "#;
        let deserialized: Candle = serde_json::from_str(candles_json).unwrap();
        println!("deserialized: {:?}", deserialized);
    }

    use rust_decimal::prelude::*;
    #[test]
    fn timestamp_to_decimal() {
        let curr_time = Utc::now();
        println!("Current Time: {:?}", curr_time);
        println!("Current Timestamp: {:?}", curr_time.timestamp());
        println!(
            "Current TS in Micros {:?}",
            curr_time.timestamp_subsec_micros()
        );
        let seconds_in_micros = curr_time.timestamp() * 1000000;
        let ts_in_micros = seconds_in_micros + curr_time.timestamp_subsec_micros() as i64;
        let dec_time = Decimal::new(ts_in_micros, 6);
        println!("Current TS in Decimal {:?}", dec_time);
    }
}
