use crate::{
    configuration::Database,
    eldorado::{ElDorado, ElDoradoError},
    exchanges::{error::WsError, ws::Channel, ws::Data, ws::WebSocket, ExchangeName},
    trades::Trade,
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{Arc, Mutex},
};
use tokio_tungstenite::tungstenite::error::ProtocolError;
use uuid::Uuid;

type Db = Arc<Mutex<HashMap<Uuid, (DateTime<Utc>, i64)>>>;

impl ElDorado {
    pub async fn stream(&self, db: Db) -> Result<(), ElDoradoError> {
        // Configure schema and create trade tables for the current day
        let today = self.initialize_trade_schema_and_tables().await?;
        // Migration of MITA stream. Write from websocket to trades database
        self.stream_to_db(db, today).await?;
        Ok(())
    }

    pub async fn stream_to_db(&self, db: Db, mut dt: DateTime<Utc>) -> Result<(), ElDoradoError> {
        // Initiate channels for websocket and tables for database
        let channels = self.initialize_channels().await;
        // Create ws client
        let mut ws = WebSocket::connect(&self.instance.exchange_name.unwrap()).await?;
        // Subscribe to trades channels for each market
        ws.subscribe(channels).await?;
        // Loop forever writing each trade to the database and create new trade tables for each day
        loop {
            // Check the working date and create the next day's table if needed
            dt = self.create_future_trade_tables_all_markets(dt).await?;
            // Get next websocket item
            let data = ws.next().await.expect("No data received.")?;
            // Process next item
            self.process_ws_data(db.clone(), data).await?
        }
    }

    pub async fn process_ws_data(
        &self,
        db: Db,
        d: (Option<String>, Data),
    ) -> Result<(), sqlx::Error> {
        match d {
            (Some(market), Data::FtxTrade(trade)) => {
                trade
                    .insert(
                        &self.pools[&Database::Ftx],
                        &self.market_names[&self.instance.exchange_name.unwrap()][&market],
                    )
                    .await
            }
            (Some(market), Data::GdaxTrade(trade)) => {
                trade
                    .insert(
                        &self.pools[&Database::Gdax],
                        &self.market_names[&self.instance.exchange_name.unwrap()][&market],
                    )
                    .await
            }
            (Some(market), Data::GdaxHb(hb)) => {
                let mut db = db.lock().unwrap();
                db.insert(
                    self.market_names[&self.instance.exchange_name.unwrap()][&market].market_id,
                    (hb.time, hb.last_trade_id),
                );
                Ok(())
            }
            (None, d) => panic!("Market missing from ws data: {:?}", d),
        }
    }

    pub fn handle_ws_error_for_restart(&self, e: WsError) -> bool {
        match e {
            WsError::Tungstenite(e) => match e {
                tokio_tungstenite::tungstenite::Error::Io(ioerr) => match ioerr.kind() {
                    ErrorKind::ConnectionReset => {
                        println!("Error Kind: ConnectionReset.");
                        println!("to_string(): {:?}", ioerr.to_string());
                        true
                    }
                    ErrorKind::UnexpectedEof => {
                        println!("Error Kind: UnexpectedEof.");
                        println!("to_string(): {:?}", ioerr.to_string());
                        true
                    }
                    _ => {
                        println!("Other Error Kind: {:?}.", ioerr.kind());
                        println!("to_string(): {:?}", ioerr.to_string());
                        false
                    }
                },
                tokio_tungstenite::tungstenite::Error::Protocol(perr) => match perr {
                    ProtocolError::SendAfterClosing => {
                        println!("Error Kind: Protocol SendAfterClosing.");
                        println!("to_string(): {:?}", perr.to_string());
                        true
                    }
                    ProtocolError::ResetWithoutClosingHandshake => {
                        println!("Error Kind: Protocol ResetWithoutClosingHandshake.");
                        println!("to_string(): {:?}", perr.to_string());
                        true
                    }
                    _ => {
                        println!("Other WSError::Tungstenite protocol error {:?}", perr);
                        println!("to_string(): {:?}", perr.to_string());
                        panic!();
                    }
                },
                tokio_tungstenite::tungstenite::Error::Http(r) => {
                    if r.status().is_server_error() {
                        println!("Server error - retry");
                        true
                    } else {
                        println!("Other http error {:?}", r);
                        false
                    }
                }
                tokio_tungstenite::tungstenite::Error::AlreadyClosed => {
                    println!("Connection AlreadyClosed");
                    true
                }
                _ => {
                    println!("Other WSError::Tungstenite error {:?}", e);
                    println!("to_string(): {:?}", e.to_string());
                    panic!();
                }
            },
            WsError::TimeSinceLastMsg => true,
            WsError::MissingSubscriptionConfirmation => {
                println!("Missing subscription confirmation, sleep 5s and restart.");
                true
            }
            _ => panic!("Other WsError {:?}", e),
        }
    }

    pub async fn initialize_channels(&self) -> Vec<Channel> {
        let mut channels = Vec::new();
        for market in self.markets.iter() {
            match &self.instance.exchange_name.unwrap() {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    channels.push(Channel::Trades(market.market_name.to_owned()));
                }
                ExchangeName::Gdax => {
                    channels.push(Channel::Heartbeat(market.market_name.to_owned()));
                    channels.push(Channel::Ticker(market.market_name.to_owned()));
                }
                name => panic!("{:?} not supported for stream.", name),
            };
        }
        channels
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::configuration::get_configuration;
//     use crate::exchanges::ws::{Channel, Data, WebSocket};
//     use crate::exchanges::{Exchange, ExchangeName};
//     use crate::markets::MarketDetail;
//     use futures::StreamExt;
//     use sqlx::PgPool;

//     // #[tokio::test]
//     // async fn stream_trades_to_db() {
//     //     // Grab config settings and stream those trades to db table
//     //     let configuration = get_configuration().expect("Could not get configuration.");
//     //     let pool = PgPool::connect_with(configuration.ftx_db.with_db())
//     //         .await
//     //         .expect("Failed to connect to Postgres.");
//     //     let exchange = Exchange::new_sample_exchange(&ExchangeName::Ftx);
//     //     let market_ids = MarketDetail::select_all(&pool)
//     //         .await
//     //         .expect("Could not fetch markets.");
//     //     let market_id = market_ids
//     //         .iter()
//     //         .find(|m| m.market_name == configuration.application.market)
//     //         .unwrap();
//     //     let market_detail = MarketDetail::select_by_id(&pool, &market_id.market_id)
//     //         .await
//     //         .expect("Could not fetch market detail.");
//     //     drop_trade_table(&pool, &exchange.name, &market_detail, "ws_test")
//     //         .await
//     //         .expect("Could not drop test ws table.");
//     //     create_ftx_trade_table(&pool, &exchange.name, &market_detail, "ws_test")
//     //         .await
//     //         .expect("Could not create test ws table.");
//     //     let mut ws = WebSocket::connect(&exchange.name)
//     //         .await
//     //         .expect("Failed to connect to ws.");
//     //     ws.subscribe(vec![Channel::Trades(market_id.market_name.to_owned())])
//     //         .await
//     //         .expect("Could not subscribe to market.");
//     //     loop {
//     //         let data = ws.next().await.expect("No data received.");
//     //         match data {
//     //             Ok((_, Data::FtxTrade(trade))) => {
//     //                 insert_ftx_trade(&pool, &exchange.name, &market_detail, "ws_test", trade)
//     //                     .await
//     //                     .expect("Could not insert trade into db.");
//     //             }
//     //             _ => panic!("Unexpected data type."),
//     //         }
//     //     }
//     // }
// }
