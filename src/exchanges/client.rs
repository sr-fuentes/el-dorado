use crate::exchanges::{error::RestError, ExchangeName};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::{from_reader, Map, Value};

#[derive(Debug)]
pub struct RestClient {
    pub header: &'static str,
    pub endpoint: &'static str,
    pub client: Client,
}

impl RestClient {
    pub const FTX_ENDPOINT: &'static str = "https://ftx.com/api";
    pub const FTXUS_ENDPOINT: &'static str = "https://ftx.us/api";
    pub const GDAX_ENDPOINT: &'static str = "https://api.exchange.coinbase.com";
    pub const FTX_HEADER: &'static str = "FTX";
    pub const FTXUS_HEADER: &'static str = "FTXUS";
    pub const GDAX_HEADER: &'static str = "GDAX"; // Not needed for GDAX requests

    pub fn new(exchange: &ExchangeName) -> Self {
        let client = match exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap(),
            ExchangeName::Gdax => {
                Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .user_agent("ALMEJAL") // GDAX API requires user agent
                    .build()
                    .unwrap()
            }
        };
        let (header, endpoint) = match exchange {
            ExchangeName::Ftx => (Self::FTX_HEADER, Self::FTX_ENDPOINT),
            ExchangeName::FtxUs => (Self::FTXUS_HEADER, Self::FTXUS_ENDPOINT),
            ExchangeName::Gdax => (Self::GDAX_HEADER, Self::GDAX_ENDPOINT),
        };
        Self {
            header,
            endpoint,
            client,
        }
    }
}
