use crate::exchanges::{
    error::WsError, ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade, ExchangeName,
};
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

pub struct WebSocket {
    channels: Vec<Channel>,
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    buf: VecDeque<(Option<String>, Data)>,
    ping_timer: Interval,
    exchange: ExchangeName,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Channel {
    Trades(String),
    Ticker(String),
    Heartbeat(String),
}

pub enum Response {
    Ftx(FtxResponse),
    Gdax(Value),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FtxResponse {
    pub market: Option<String>,
    pub r#type: Type,
    pub data: Option<ResponseData>,
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
}

impl WebSocket {
    pub const FTX_ENDPOINT: &'static str = "wss://ftx.com/ws";
    pub const FTXUS_ENDPOINT: &'static str = "wss://ftx.us/ws";
    pub const GDAX_ENDPOINT: &'static str = "wss://ws-feed.pro.coinbase.com";

    pub async fn connect(exchange: &ExchangeName) -> Result<Self, WsError> {
        let endpoint = match exchange {
            ExchangeName::Ftx => Self::FTX_ENDPOINT,
            ExchangeName::FtxUs => Self::FTXUS_ENDPOINT,
            ExchangeName::Gdax => Self::GDAX_ENDPOINT,
        };
        let (stream, _) = connect_async(endpoint).await?;
        Ok(Self {
            channels: Vec::new(),
            stream,
            buf: VecDeque::new(),
            ping_timer: time::interval(Duration::from_secs(15)),
            exchange: *exchange,
        })
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
            };
            println!("Message: {}", message);
            self.stream.send(message).await?;
            // Confirmation should arrive within 100 updates
            for _ in 0..100 {
                let response = self.next_response().await?;
                match response {
                    Response::Ftx(r) => match r.r#type {
                        Type::Subscribed => continue 'channels,
                        _ => self.handle_response(Response::Ftx(r)),
                    },
                    Response::Gdax(v) => {
                        if v["type"] == "subscriptions" {
                            continue 'channels;
                        } else {
                            self.handle_response(Response::Gdax(v))
                        }
                    }
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
                ExchangeName::Gdax => {
                    if let Some(msg) = self.stream.next().await {
                        // println!("msg: {:?}", msg);
                        let msg = msg?;
                        if let Message::Text(text) = msg {
                            // println!("Text: {}", text);
                            let response: Value = serde_json::from_str(&text)?;
                            return Ok(Response::Gdax(response));
                        }
                    }
                }
            }
        }
    }

    fn handle_response(&mut self, response: Response) {
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
                }
            }
        }
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
            self.handle_response(response);
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
}
