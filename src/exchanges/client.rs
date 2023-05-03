use crate::exchanges::{error::RestError, ExchangeName};
use reqwest::{Client, Method, Response};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{from_reader, Map, Value};
use std::collections::HashMap;

#[derive(Debug)]
pub struct RestClient {
    pub header: &'static str,
    pub endpoint: &'static str,
    pub client: Client,
    pub exchange: ExchangeName,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FtxSuccessResponse<T> {
    pub success: bool,
    pub result: T,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FtxErrorResponse {
    pub success: bool,
    pub error: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct KrakenResponse<T> {
    pub error: Vec<String>,
    pub result: Option<T>,
}

impl RestClient {
    pub const FTX_ENDPOINT: &'static str = "https://ftx.com/api";
    pub const FTXUS_ENDPOINT: &'static str = "https://ftx.us/api";
    pub const GDAX_ENDPOINT: &'static str = "https://api.exchange.coinbase.com";
    pub const KRAKEN_ENDPOINT: &'static str = "https://api.kraken.com";
    pub const FTX_HEADER: &'static str = "FTX";
    pub const FTXUS_HEADER: &'static str = "FTXUS";
    pub const GDAX_HEADER: &'static str = "GDAX"; // Not needed for GDAX requests
    pub const KRAKEN_HEADER: &'static str = "KRAKEN"; // Not needed for GDAX requests

    pub fn initialize_client_map() -> HashMap<ExchangeName, Self> {
        let mut clients = HashMap::new();
        clients.insert(ExchangeName::Ftx, RestClient::new(&ExchangeName::Ftx));
        clients.insert(ExchangeName::FtxUs, RestClient::new(&ExchangeName::FtxUs));
        clients.insert(ExchangeName::Gdax, RestClient::new(&ExchangeName::Gdax));
        clients.insert(ExchangeName::Kraken, RestClient::new(&ExchangeName::Kraken));
        clients
    }

    pub fn new(exchange: &ExchangeName) -> Self {
        let client = match exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs | ExchangeName::Kraken => Client::builder()
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
            ExchangeName::Kraken => (Self::KRAKEN_HEADER, Self::KRAKEN_ENDPOINT),
        };
        Self {
            header,
            endpoint,
            client,
            exchange: *exchange,
        }
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        self.request(Method::GET, path, params).await
    }

    pub async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        let params = params.map(|value| {
            if let Value::Object(map) = value {
                map.into_iter()
                    .filter(|(_, v)| v != &Value::Null)
                    .collect::<Map<String, Value>>()
            } else {
                panic!("Invalid params.");
            }
        });

        let response = self
            .client
            .request(method, format!("{}{}", self.endpoint, path))
            .query(&params)
            .header("ContentType", "application/json")
            .send()
            .await?;

        // println!("Text: {:?}", response.text().await?);
        // panic!();
        match self.exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => self.handle_ftx_response(response).await,
            ExchangeName::Gdax => self.handle_gdax_response(response).await,
            ExchangeName::Kraken => self.handle_kraken_response(response).await,
        }
    }

    pub async fn handle_ftx_response<T: DeserializeOwned>(
        &self,
        response: Response,
    ) -> Result<T, RestError> {
        match response.error_for_status() {
            Ok(res) => {
                let res_bytes = res.bytes().await?;
                match from_reader(&*res_bytes) {
                    Ok(FtxSuccessResponse { result, .. }) => Ok(result),
                    Err(e) => {
                        if let Ok(FtxErrorResponse { error, .. }) = from_reader(&*res_bytes) {
                            Err(RestError::Api(error))
                        } else {
                            println!(
                                "Reqwest resp: {:?}",
                                std::str::from_utf8(&res_bytes).unwrap()
                            );
                            println!("Error: {:?}", e.to_string());
                            eprintln!("Errorpl: {:?}", e);
                            Err(e.into())
                        }
                    }
                }
            }
            Err(e) => {
                println!("Reqwest status error: {:?}", e.status());
                Err(e.into())
            }
        }
    }

    pub async fn handle_gdax_response<T: DeserializeOwned>(
        &self,
        response: Response,
    ) -> Result<T, RestError> {
        match response.error_for_status() {
            Ok(res) => {
                let res_bytes = res.bytes().await?;
                match from_reader(&*res_bytes) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        println!(
                            "Reqwest resp: {:?}",
                            std::str::from_utf8(&res_bytes).unwrap()
                        );
                        println!("Error: {:?}", e.to_string());
                        eprintln!("Errorpl: {:?}", e);
                        Err(e.into())
                    }
                }
            }
            Err(e) => {
                println!("Reqwest status error: {:?}", e.status());
                println!("URL: {:?}", e.url());
                Err(e.into())
            }
        }
    }

    pub async fn handle_kraken_response<T: DeserializeOwned>(
        &self,
        response: Response,
    ) -> Result<T, RestError> {
        match response.error_for_status() {
            Ok(res) => {
                let res_bytes = res.bytes().await?;
                match from_reader(&*res_bytes) {
                    Ok(KrakenResponse { result, .. }) => Ok(result.unwrap()),
                    Err(e) => {
                        println!(
                            "Reqwest resp: {:?}",
                            std::str::from_utf8(&res_bytes).unwrap()
                        );
                        println!("Error: {:?}", e.to_string());
                        eprintln!("Errorpl: {:?}", e);
                        Err(e.into())
                    }
                }
            }
            Err(e) => {
                println!("Reqwest status error: {:?}", e.status());
                Err(e.into())
            }
        }
    }
}
