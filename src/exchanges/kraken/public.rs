use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::exchanges::{client::RestClient, error::RestError};

#[derive(Debug, Serialize, Deserialize)]
pub struct AssetPair {
    pub alt_name: Option<String>,
    pub wsname: Option<String>,
    pub aclass_base: String,
    pub base: String,
    pub aclass_quote: String,
    pub quote: String,
    pub pair_decimals: u64,
    pub lot_decimals: u64,
    pub lot_multiplier: u64,
    pub fees: Vec<Vec<Decimal>>,
    pub ordermin: Option<Decimal>,
    pub tick_size: Option<Decimal>,
}

impl RestClient {
    pub async fn get_kraken_tradable_asset_pairs(
        &self,
    ) -> Result<HashMap<String, AssetPair>, RestError> {
        self.get("/0/public/AssetPairs", None).await
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::{client::RestClient, ExchangeName};

    #[tokio::test]
    async fn get_tradable_asset_pairs() {
        let client = RestClient::new(&ExchangeName::Kraken);
        let tas = client
            .get_kraken_tradable_asset_pairs()
            .await
            .expect("Failed to get all asset paires.");
        for (k, v) in tas.iter() {
            if v.quote == "ZUSD" {
                println!("{} - {:?}", k, v);
            }
            // println!("{} - {:?}", k, v);
        }
        // println!("AssetPairs: {:?}", tas);
    }
}
