use crate::exchanges::{
    bybit::Trade as BybitTrade, error::WsError, ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade,
    ExchangeName,
};
use chrono::{serde::ts_milliseconds, DateTime, Duration as CDuration, Utc};
use futures::{
    ready,
    task::{Context, Poll},
    Future, SinkExt, Stream, StreamExt,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::pin::Pin;
use tokio::net::TcpStream;
use tokio::time;
use tokio::time::{Duration, Interval};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

use super::gdax::Heartbeat;

pub struct WebSocket {
    channels: Vec<Channel>,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    buf: VecDeque<(Option<String>, Data)>,
    ping_timer: Interval,
    exchange: ExchangeName,
    last_msg: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Trades(String),
    Ticker(String),
    Heartbeat(String),
    Args(String),
}

pub enum Response {
    Ftx(FtxResponse),
    Gdax(Value),
    BybitOp(BybitOpResponse),
    BybitTopic(BybitTopicResponse),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtxResponse {
    pub market: Option<String>,
    pub r#type: Type,
    pub data: Option<ResponseData>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BybitOpResponse {
    op: String,
    _success: bool,
    _ret_msg: Option<String>,
    _conn_id: Uuid,
    _req_id: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BybitTopicResponse {
    topic: String,
    _type: String,
    #[serde(with = "ts_milliseconds")]
    _ts: DateTime<Utc>,
    data: Vec<BybitTrade>,
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    Subscribed,
    Unsubscribed,
    Subscriptions, // GDAX Type - not all GDAX types mapped
    Update,
    Error, // FTX and GDAX Type
    Partial,
    Pong,
    Info,
    Ticker, // GDAX Type - not all GDAX types mapped
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum ResponseData {
    FtxTrades(Vec<FtxTrade>),
}

/// Represents the data we return to the user
#[derive(Clone, Debug)]
pub enum Data {
    FtxTrade(FtxTrade),
    GdaxTrade(GdaxTrade),
    GdaxHb(Heartbeat),
    BybitTrade(BybitTrade),
}

impl WebSocket {
    pub const FTX_ENDPOINT: &'static str = "wss://ftx.com/ws";
    pub const FTXUS_ENDPOINT: &'static str = "wss://ftx.us/ws";
    pub const GDAX_ENDPOINT: &'static str = "wss://ws-feed.pro.coinbase.com";
    pub const BYBIT_ENDPOINT: &'static str = "wss://stream.bybit.com/v5/public/linear";

    pub async fn connect(exchange: &ExchangeName) -> Result<Self, WsError> {
        let endpoint = match exchange {
            ExchangeName::Ftx => Self::FTX_ENDPOINT,
            ExchangeName::FtxUs => Self::FTXUS_ENDPOINT,
            ExchangeName::Gdax => Self::GDAX_ENDPOINT,
            ExchangeName::Bybit => Self::BYBIT_ENDPOINT,
            name => panic!("{:?} not supported for ws.", name),
        };
        let (stream, _) = connect_async(endpoint).await?;
        Ok(Self {
            channels: Vec::new(),
            stream,
            buf: VecDeque::new(),
            ping_timer: time::interval(Duration::from_secs(15)),
            exchange: *exchange,
            last_msg: None,
        })
    }

    pub fn time_since_msg(&self) -> Option<CDuration> {
        self.last_msg.map(|t| Utc::now() - t)
    }

    pub async fn ping(&mut self) -> Result<(), WsError> {
        self.stream
            .send(Message::Text(json!({"op": "ping",}).to_string()))
            .await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, channels: Vec<Channel>) -> Result<(), WsError> {
        println!("Channels: {:?}", channels);
        for channel in channels.iter() {
            self.channels.push(channel.clone());
        }
        'channels: for channel in channels {
            let (channel, symbol) = match channel {
                Channel::Trades(s) => ("trades", s),
                Channel::Ticker(s) => ("ticker", s),
                Channel::Heartbeat(s) => ("heartbeat", s),
                Channel::Args(s) => ("args", s),
            };
            let message = match self.exchange {
                ExchangeName::Ftx | ExchangeName::FtxUs => Message::Text(
                    json!({"op": "subscribe", "channel": channel, "market": symbol}).to_string(),
                ),
                ExchangeName::Gdax => Message::Text(
                    json!(
                    {"type": "subscribe",
                    "channels":
                        [{"name": channel,
                        "product_ids": [symbol]}
                        ]
                    })
                    .to_string(),
                ),
                ExchangeName::Bybit => Message::Text(
                    json!({"op": "subscribe", channel: [ format!("publicTrade.{}",symbol) ]})
                        .to_string(),
                ),
                name => panic!("{:?} not supported for ws.", name),
            };
            println!("Message: {}", message);
            self.stream.send(message).await?;
            self.last_msg = Some(Utc::now());
            // Confirmation should arrive within 100 updates
            for _ in 0..100 {
                let response = self.next_response().await?;
                match response {
                    Response::Ftx(r) => match r.r#type {
                        Type::Subscribed => continue 'channels,
                        _ => self.handle_response(Response::Ftx(r))?,
                    },
                    Response::Gdax(v) => {
                        if v["type"] == "subscriptions" {
                            continue 'channels;
                        } else {
                            self.handle_response(Response::Gdax(v))?
                        }
                    }
                    Response::BybitOp(r) => {
                        println!("R: {:?}", r);
                        if r.op == "subscribe".to_string() {
                            continue 'channels;
                        } else {
                            self.handle_response(Response::BybitOp(r))?
                        }
                    }
                    Response::BybitTopic(r) => self.handle_response(Response::BybitTopic(r))?,
                }
            }
            return Err(WsError::MissingSubscriptionConfirmation);
        }
        Ok(())
    }

    async fn next_response(&mut self) -> Result<Response, WsError> {
        loop {
            match self.exchange {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    tokio::select! {
                        _ = self.ping_timer.tick() => {
                            self.ping().await?;
                        },
                        Some(msg) = self.stream.next() => {
                            self.last_msg = Some(Utc::now());
                            let msg = msg?;
                            if let Message::Text(text) = msg {
                                // println!("Text: {}", text);
                                let response: FtxResponse = serde_json::from_str(&text)?;
                                // Don't return pong responses
                                if let FtxResponse {r#type: Type::Pong, ..} = response { continue;}
                                return Ok(Response::Ftx(response))
                            }
                        },
                    }
                }
                ExchangeName::Bybit => {
                    tokio::select! {
                        _ = self.ping_timer.tick() => {
                            self.ping().await?;
                        },
                        Some(msg) = self.stream.next() => {
                            self.last_msg = Some(Utc::now());
                            let msg = msg?;
                            if let Message::Text(text) = msg {
                                let response_val: Value = serde_json::from_str(&text)?;
                                    match response_val.get("op") {
                                    Some(_) => {
                                        let op_response: BybitOpResponse = serde_json::from_value(response_val)?;
                                        return Ok(Response::BybitOp(op_response))
                                    },
                                    None => {
                                        let top_response: BybitTopicResponse = serde_json::from_value(response_val)?;
                                        return Ok(Response::BybitTopic(top_response))
                                    },
                                }
                            }
                        },
                    }
                }
                ExchangeName::Gdax => {
                    tokio::select! {
                        _ = self.ping_timer.tick() => {
                            // Check that message has been received in last 5 seconds
                            match self.time_since_msg() {
                                Some(t) => {
                                    if t > CDuration::seconds(5) {
                                        println!("{} since last message.", t);
                                        match self.stream.close(None).await {
                                            Ok(_) => (),
                                            Err(tokio_tungstenite::tungstenite::Error::AlreadyClosed) => (),
                                            Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => (),
                                            Err(e) => return Err(WsError::Tungstenite(e)),
                                        }
                                        return Err(WsError::TimeSinceLastMsg)}
                                }
                                None => {
                                    println!("No last message.");
                                    match self.stream.close(None).await {
                                        Ok(_) => (),
                                        Err(tokio_tungstenite::tungstenite::Error::AlreadyClosed) => (),
                                        Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => (),
                                        Err(e) => return Err(WsError::Tungstenite(e)),
                                    }
                                    return Err(WsError::TimeSinceLastMsg)
                                }
                            };
                        },
                        Some(msg) = self.stream.next() => {
                            self.last_msg = Some(Utc::now());
                            let msg = msg?;
                            if let Message::Text(text) = msg {
                                // println!("Text: {}", text);
                                let response: Value = serde_json::from_str(&text)?;
                                return Ok(Response::Gdax(response));
                            }
                        },
                    }
                }
                name => panic!("{:?} not supported for ws.", name),
            }
        }
    }

    fn handle_response(&mut self, response: Response) -> Result<(), WsError> {
        match response {
            Response::Ftx(r) => {
                if let Some(data) = r.data {
                    match data {
                        ResponseData::FtxTrades(trades) => {
                            for trade in trades {
                                self.buf
                                    .push_back((r.market.clone(), Data::FtxTrade(trade)))
                            }
                        }
                    }
                }
            }
            Response::Gdax(v) => {
                if v["type"] == "ticker" {
                    let product_id: String =
                        serde_json::from_value(v["product_id"].clone()).unwrap();
                    // println!("V: {:?}", v);
                    // println!("Product Id: {:?}", product_id);
                    // let trade: GdaxTrade = serde_json::from_value(v).unwrap();
                    // self.buf
                    //     .push_back((Some(product_id), Data::GdaxTrade(trade)))
                    let v2 = v.clone();
                    match serde_json::from_value::<GdaxTrade>(v) {
                        Ok(t) => self.buf.push_back((Some(product_id), Data::GdaxTrade(t))),
                        Err(e) => {
                            println!("Failed to parse gdax trade from serde json value.");
                            println!("Value: {:?}", v2);
                            println!("Error: {:?}", e);
                        }
                    }
                } else if v["type"] == "heartbeat" {
                    let product_id: String =
                        serde_json::from_value(v["product_id"].clone()).unwrap();
                    // println!("V: {:?}", v);
                    // println!("Product Id: {:?}", product_id);
                    // let trade: GdaxTrade = serde_json::from_value(v).unwrap();
                    // self.buf
                    //     .push_back((Some(product_id), Data::GdaxTrade(trade)))
                    let v2 = v.clone();
                    match serde_json::from_value::<Heartbeat>(v) {
                        Ok(hb) => self.buf.push_back((Some(product_id), Data::GdaxHb(hb))),
                        Err(e) => {
                            println!("Failed to parse gdax trade from serde json value.");
                            println!("Value: {:?}", v2);
                            println!("Error: {:?}", e);
                        }
                    }
                } else {
                    println!("Other message: {:?}", v);
                    return Err(WsError::TimeSinceLastMsg);
                }
            }
            Response::BybitTopic(r) => {
                let ticker = r.topic.strip_prefix("publicTrade.").unwrap().clone();
                for trade in r.data {
                    self.buf
                        .push_back((Some(ticker.to_string()), Data::BybitTrade(trade)))
                }
            }
            Response::BybitOp(r) => {
                println!("{:?}", r);
            }
        }
        Ok(())
    }
}

impl Stream for WebSocket {
    type Item = Result<(Option<String>, Data), WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(data) = self.buf.pop_front() {
                return Poll::Ready(Some(Ok(data)));
            }
            let response = {
                // Fetch new response if buffer is empty.
                // safety: this is ok because the future from self.next_response() will only live
                // in this function. It will not be moved anymore.
                let mut next_response = self.next_response();
                let pinned = unsafe { Pin::new_unchecked(&mut next_response) };
                match ready!(pinned.poll(cx)) {
                    Ok(response) => response,
                    Err(e) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            };
            self.handle_response(response)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exchanges::ws::{Channel, Data, WebSocket};
    use crate::exchanges::ExchangeName;
    use futures::StreamExt;

    #[tokio::test]
    async fn stream_ftx_trades() {
        let mut ws = WebSocket::connect(&ExchangeName::Ftx)
            .await
            .expect("Could not connect ws");
        let market = "BTC-PERP".to_string();
        ws.subscribe(vec![Channel::Trades(market.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::FtxTrade(trade))) => {
                    println!(
                        "\n{:?} {} {} at {} - liquidation = {}",
                        trade.side, trade.size, market, trade.price, trade.liquidation
                    );
                }
                _ => panic!("Unexpected data type"),
            }
        }
    }

    #[tokio::test]
    async fn stream_gdax_trades() {
        let mut ws = WebSocket::connect(&ExchangeName::Gdax)
            .await
            .expect("Could not connect ws");
        let market = "BTC-USD".to_string();
        ws.subscribe(vec![Channel::Ticker(market.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::GdaxTrade(trade))) => {
                    println!(
                        "\n{:?} {} {} at {}",
                        trade.side, trade.size, market, trade.price
                    );
                }
                _ => panic!("Unexpected data type"),
            }
        }
    }

    #[tokio::test]
    async fn stream_bybit_trades() {
        let mut ws = WebSocket::connect(&ExchangeName::Bybit)
            .await
            .expect("Could not connect ws");
        let market = "BTCUSDT".to_string();
        ws.subscribe(vec![Channel::Args(market.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::BybitTrade(trade))) => {
                    println!(
                        "\n{:?} {} {} at {}",
                        trade.side, trade.size, market, trade.price
                    );
                }
                _ => {
                    println!("{:?}", data);
                    // panic!("Unexpected data type");
                }
            }
        }
    }
}
