use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use super::{Market, RestError};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum RestResponse {
    Result {success: bool, result: Vec<Market>},
    Error {success: bool, error: String},
}

pub struct RestClient { 
    pub header: &'static str,
    pub endpoint: &'static str,
    pub client: Client,
}

impl RestClient {
    pub const INTL_ENDPOINT: &'static str = "https://ftx.com/api";
    pub const US_ENDPOINT: &'static str = "https://ftx.us/api";
    pub const INTL_HEADER: &'static str = "FTX";
    pub const US_HEADER: &'static str = "FTXUS";

    pub fn new(endpoint: &'static str, header: &'static str) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            header,
            endpoint,
            client,
        }
    }

    pub fn new_intl() -> Self {
        Self::new(Self::INTL_ENDPOINT, Self::INTL_HEADER)
    }

    pub fn new_us() -> Self {
        Self::new(Self::US_ENDPOINT, Self::US_HEADER)
    }

    //async fn get() -> Result<()> {}

    pub fn post() {}

    pub async fn request(&self) -> Result<Vec<Market>, RestError> {
        let response = self.client
            .request(Method::GET, "https://ftx.us/api/markets")
            .send()
            .await?; // reqwest::Error if request fails

        let markets: RestResponse = response
            .json()
            .await?; // reqwest::Error if serde deserialize fails

        match markets {
            RestResponse::Result {result, .. } => Ok(result),
            RestResponse::Error { error, .. } => Err(RestError::Api(error)),
        }
        
        // Write text response to file to derive struct fields:
        //
        // let response: String = self
        //     .client
        //     .request(Method::GET, "https://ftx.us/api/markets")
        //     .send()
        //     .await?
        //     .text()
        //     .await?;

        // use std::fs::File;
        // use std::io::prelude::*;
        // let mut file = File::create("response.json").unwrap();
        // file.write_all(response.as_bytes()).unwrap();
        // panic!("{:#?}", response);
    }
}


#[cfg(test)]
mod tests {
    use serde::de::{DeserializeOwned, DeserializeSeed};

    use crate::exchanges::ftx::{Market, Markets, RestClient, markets};

    #[test]
    fn new_intl_fn_returns_client_with_intl_header_and_endpoint() {
        let client = RestClient::new_intl();
        assert_eq!(client.header, "FTX");
        assert_eq!(client.endpoint, "https://ftx.com/api");
    }

    #[test]
    fn new_us_fn_returns_client_with_us_header_and_endpoint() {
        let client = RestClient::new_us();
        assert_eq!(client.header, "FTXUS");
        assert_eq!(client.endpoint, "https://ftx.us/api");
    }

    #[tokio::test]
    async fn request_fn_prints_response() {
        let client = RestClient::new_us();
        let markets = client.request().await.expect("Reqwest error.");
        println!("markets: {:?}", markets);
    }
}