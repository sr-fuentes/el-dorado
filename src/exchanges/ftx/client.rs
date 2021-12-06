use super::{RestError, WsError, Trade};
use reqwest::{Client, Method};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{from_reader, Map, Value};
use std::collections::VecDeque;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::{Interval, Duration};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub struct RestClient {
    pub header: &'static str,
    pub endpoint: &'static str,
    pub client: Client,
}

pub struct WsClient {
    channels: Vec<Channel>,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    buf: VecDeque<(Option<String>, Data)>,
    ping_timer: Interval,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SuccessResponse<T> {
    pub success: bool,
    pub result: T,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorResponse {
    pub success: bool,
    pub error: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Channel {
    Orderbook(String),
    Trades(String),
    Ticker(String),
    Fills,
    Orders,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub market: Option<String>,
    pub r#type: Type,
    pub data: Option<ResponseData>,
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Subscribed,
    Unsubscribed,
    Update,
    Error,
    Partial,
    Pong,
    Info,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum ResponseData {
    Trades(Vec<Trade>),
}

/// Represents the data we return to the user
#[derive(Clone, Debug)]
pub enum Data {
    Trade(Trade),
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

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        self.request(Method::GET, path, params).await
    }

    // pub fn post(&self, &url, &params) {}

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
            .send()
            .await?;
        // .bytes()
        // .await?;

        match response.error_for_status() {
            Ok(res) => {
                let res_bytes = res.bytes().await?;
                match from_reader(&*res_bytes) {
                    Ok(SuccessResponse { result, .. }) => Ok(result),
                    Err(e) => {
                        if let Ok(ErrorResponse { error, .. }) = from_reader(&*res_bytes) {
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
}

impl WsClient {
    pub const INTL_ENDPOINT: &'static str = "wss://ftx.com/ws";
    pub const US_ENDPOINT: &'static str = "wss://ftx.us/ws";

    pub async fn connect(endpoint: &'static str) -> Result<Self, WsError> {
        let (mut stream, _) = connect_async(endpoint).await?;
        Ok(Self {
            channels: Vec::new(),
            stream,
            buf: VecDeque::new(),
            ping_timer: time::interval(Duration::from_secs(15)),
        })
    }

    pub async fn connect_intl() -> Result<Self, WsError> {
        Ok(Self::connect(Self::INTL_ENDPOINT).await?)
    }

    pub async fn connect_us() -> Result<Self, WsError> {
        Ok(Self::connect(Self::US_ENDPOINT).await?)
    }
}
#[cfg(test)]
mod tests {
    use crate::exchanges::ftx::RestClient;

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
}
