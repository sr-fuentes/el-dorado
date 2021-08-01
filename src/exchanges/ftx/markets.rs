use serde::{Deserialize, Serialize};
//use super::RestClient;

// Sample 
// '{"success":true,"result":[
// {
// "name":"AAVE/USD",
// "enabled":true,
// "postOnly":false,
// "priceIncrement":0.01,
// "sizeIncrement":0.01,
// "minProvideSize":0.01,
// "last":303.56,
// "bid":304.98,
// "ask":305.21,
// "price":304.98,
// "type":"spot",
// "baseCurrency":"AAVE",
// "quoteCurrency":"USD",
// "underlying":null,
// "restricted":false,
// "highLeverageFeeExempt":true,
// "change1h":-0.00029501425902251943,
// "change24h":-0.011025358324145534,
// "changeBod":-0.025186984593748,
// "quoteVolume24h":224063.1118,
// "volumeUsd24h":224063.1118
// }

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    name: String,
    enabled: bool,
    post_only: bool,
    price_increment: f64,
    size_increment: f64,
    min_provide_size: f64,
    last: f64,
    bid: f64,
    ask: f64,
    price: f64,
    #[serde(rename="type")]
    market_type: String,
    base_currency: Option<String>,
    quote_currency: Option<String>,
    underlying: Option<String>,
    restricted: bool,
    high_leverage_fee_exempt: bool,
    change1h: f64,
    change24h: f64,
    change_bod: f64,
    quote_volume24h: f64,
    volume_usd24h: f64,
}

pub type Markets = Vec<Market>;
// impl RestClient {
//     // Add `/market` specific API endpoints
//     pub async fn get_markets(&self) -> Vec<Market> {
//         self.get("/markets", None).await
//     }
// }

#[cfg(test)]
mod tests {
    use crate::exchanges::ftx::*;

    #[test]
    fn serde_deserializes_the_market_struct () {
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
}