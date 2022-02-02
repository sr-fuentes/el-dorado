use super::{RestError};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::Value;

#[derive(Debug)]
pub struct RestClient {
    pub endpoint: &'static str,
    pub client: Client,
}

impl RestClient {
    pub const ENDPOINT: &'static str = "https://api.pro.coinbase.com";

    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            endpoint: Self::ENDPOINT,
            client,
        }
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        self.request(Method::GET, path, params).await
    }
}