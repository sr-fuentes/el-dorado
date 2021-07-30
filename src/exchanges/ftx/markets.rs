use serde::Deserialize;

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

#[derive(Deserialize, Debug)]
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
    base_currency: String,
    quote_currency: String,
    underlying: String,
    restricted: bool,
    high_leverage_fee_exemption: bool,
    change_1h: f64,
    change_24h: f64,
    change_bod: f64,
    quote_volume_24h: f64,
    volume_usd_24h: f64,
}