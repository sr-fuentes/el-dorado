use super::{RestError, Trade, WsError};
use futures::{
    ready,
    task::{Context, Poll},
    Future, SinkExt, Stream, StreamExt,
};
use reqwest::{Client, Method};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{from_reader, json, Map, Value};
use std::collections::VecDeque;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::{Duration, Interval};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

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
        let (stream, _) = connect_async(endpoint).await?;
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

    pub async fn ping(&mut self) -> Result<(), WsError> {
        self.stream
            .send(Message::Text(json!({"op": "ping",}).to_string()))
            .await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, channels: Vec<Channel>) -> Result<(), WsError> {
        for channel in channels.iter() {
            self.channels.push(channel.clone());
        }
        'channels: for channel in channels {
            let (channel, symbol) = match channel {
                Channel::Trades(symbol) => ("trades", symbol),
                Channel::Orderbook(symbol) => ("orderbook", symbol),
                Channel::Ticker(symbol) => ("ticker", symbol),
                Channel::Fills => ("fills", "".to_string()),
                Channel::Orders => ("orders", "".to_string()),
            };
            self.stream
                .send(Message::Text(
                    json!({"op": "subscribe", "channel": channel, "market": symbol}).to_string(),
                ))
                .await?;

            // Confirmation should arrive within 100 updates
            for _ in 0..100 {
                let response = self.next_response().await?;
                match response {
                    Response {
                        r#type: Type::Subscribed,
                        ..
                    } => {
                        continue 'channels;
                    }
                    _ => {
                        self.handle_response(response);
                    }
                }
            }
            return Err(WsError::MissingSubscriptionConfirmation);
        }
        Ok(())
    }

    async fn next_response(&mut self) -> Result<Response, WsError> {
        loop {
            tokio::select! {
                _ = self.ping_timer.tick() => {
                    self.ping().await?;
                },
                Some(msg) = self.stream.next() => {
                    let msg = msg?;
                    if let Message::Text(text) = msg {
                        // println!("Text: {}", text);
                        let response: Response = serde_json::from_str(&text)?;
                        // Don't return pong responses
                        if let Response {r#type: Type::Pong, .. } = response { continue;}
                        return Ok(response)
                    }
                },
            }
        }
    }

    fn handle_response(&mut self, response: Response) {
        if let Some(data) = response.data {
            match data {
                ResponseData::Trades(trades) => {
                    for trade in trades {
                        self.buf
                            .push_back((response.market.clone(), Data::Trade(trade)))
                    }
                }
            }
        }
    }
}

impl Stream for WsClient {
    type Item = Result<(Option<String>, Data), WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(data) = self.buf.pop_front() {
                return Poll::Ready(Some(Ok(data)));
            }
            let response = {
                // Fetch new response if buffer is empty.
                // safety: this is ok because the future from self.next_response() will only live
                // in this function. It won't be moved anymore.
                let mut next_response = self.next_response();
                let pinned = unsafe { Pin::new_unchecked(&mut next_response) };
                match ready!(pinned.poll(cx)) {
                    Ok(response) => response,
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            };
            self.handle_response(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn trades() {
        let mut ws = WsClient::connect_intl()
            .await
            .expect("Could not connect ws");
        let market = "BTC-PERP".to_string();
        ws.subscribe(vec![Channel::Trades(market.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::Trade(trade))) => {
                    println!(
                        "\n{:?} {} {} at {} - liquidation = {}",
                        trade.side, trade.size, market, trade.price, trade.liquidation
                    );
                }
                _ => panic!("Unexpected data type"),
            }
        }
    }
}
