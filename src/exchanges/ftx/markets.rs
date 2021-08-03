use super::{RestClient, RestError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub name: String,
    pub enabled: bool,
    pub post_only: bool,
    pub price_increment: f64,
    pub size_increment: f64,
    pub min_provide_size: f64,
    pub last: f64,
    pub bid: f64,
    pub ask: f64,
    pub price: f64,
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
    pub volume_usd24h: f64,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Orderbook {
    pub asks: Vec<(f64, f64)>,
    pub bids: Vec<(f64, f64)>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub id: i64,
    pub price: f64,
    pub size: f64,
    pub side: String,
    pub liquidation: bool,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Candle {
    start_time: DateTime<Utc>,
    #[serde(with = "ts_micro_fractions")]
    time: DateTime<Utc>,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
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
    pub async fn get_markets(&self) -> Result<Vec<Market>, RestError> {
        self.get("/markets", None).await
    }

    pub async fn get_market(&self, market_name: &str) -> Result<Market, RestError> {
        self.get(&format!("/markets/{}", market_name), None).await
    }

    pub async fn get_orderbook(
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

    pub async fn get_trades(
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
                "end_time": end_time.map(|t| t.timestamp()), // API takes time in unix seconds
            }))
        )
        .await
    }

    pub async fn get_candles(
        &self,
        market_name: &str,
        resolution: Option<u32>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<Candle>, RestError> {
        self.get(
            &format!("/markets/{}/candles", market_name),
            Some(json!({
                "resolution": resolution, // Window length in seconds (15, 60, 300, 900, 3600, 14400, 86400)
                "start_time": start_time.map(|t| t.timestamp()), // Timestamp in seconds
                "end_time": end_time.map(|t| t.timestamp()), // Timestamp in seconds
            }))
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::ftx::*;
    use chrono::Utc;

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
        let client = RestClient::new_us();
        let _markets = client.get_markets().await.expect("Failed to get markets.");
    }

    #[tokio::test]
    async fn get_market_returns_specific_market() {
        let client = RestClient::new_us();
        let market_name = "BTC/USD";
        let market = client
            .get_market(&market_name)
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
        let client = RestClient::new_us();
        let orderbook = client
            .get_orderbook("BTC/USD", None)
            .await
            .expect("Failed to get orderbook.");
        println!("Orderbook: {:?}", orderbook);
    }

    #[tokio::test]
    async fn get_orderbook_with_params_returns_orderbook() {
        let client = RestClient::new_us();
        let orderbook = client
            .get_orderbook("BTC/USD", Some(300))
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
        let client = RestClient::new_us();
        let trades = client
            .get_trades("BTC/USD", None, None, None)
            .await
            .expect("Failed to load BTC/USD trades.");
        println!("Trades: {:?}", trades);
    }

    #[tokio::test]
    async fn get_trades_with_params_returns_trades() {
        let client = RestClient::new_us();
        let trades = client
            .get_trades("BTC/USD", Some(10), None, Some(Utc::now()))
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
}
