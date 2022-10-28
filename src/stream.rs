use crate::configuration::Database;
use crate::eldorado::ElDorado;
use crate::exchanges::{error::WsError, ws::Channel, ws::Data, ws::WebSocket, ExchangeName};
use crate::mita::Mita;
use crate::trades::{insert_ftx_trade, insert_gdax_trade, Trade};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use std::collections::HashMap;
use std::io::ErrorKind;
use tokio_tungstenite::tungstenite::error::ProtocolError;

impl ElDorado {
    pub async fn stream(&self) -> bool {
        // Configure schema and create trade tables for the current day
        let today = self.initialize_trade_schema_and_tables().await;
        // Migration of MITA stream. Write from websocket to trades database
        self.stream_to_db(today).await
        // REFACTOR to shared hashmap instead of writing to db
        // self.stream_to_hm().await
    }

    pub async fn stream_to_db(&self, mut dt: DateTime<Utc>) -> bool {
        // Initiate channels for websocket and tables for database
        let channels = self.initialize_channels().await;
        // Create ws client
        let mut ws = WebSocket::connect(&self.instance.exchange_name.unwrap())
            .await
            .expect("Failed to connect to ws.");
        // Subscribe to trades channels for each market
        match ws.subscribe(channels).await {
            Ok(_) => {}
            Err(WsError::MissingSubscriptionConfirmation) => {
                println!("Missing subscription confirmation, sleep 5s and restart.");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                return true;
            }
            Err(e) => {
                println!("Subscription error: {:?}", e);
                return false;
            }
        };
        // Loop forever writing each trade to the database and create new trade tables for each day
        loop {
            // Check the working date and create the next day's table if needed
            dt = self.create_future_trade_tables(dt).await;
            // Get next websocket item
            let data = ws.next().await.expect("No data received.");
            // Process next item
            match data {
                Ok(d) => self.process_ws_data(d).await,
                Err(e) => {
                    let restart = self.handle_ws_error(e).await;
                    break restart;
                }
            }
        }
    }

    pub async fn process_ws_data(&self, d: (Option<String>, Data)) {
        match d {
            (Some(market), Data::FtxTrade(trade)) => trade
                .insert(
                    &self.pools[&Database::Ftx],
                    &self.market_names[&self.instance.exchange_name.unwrap()][&market],
                )
                .await
                .expect("Failed to insert trade into db."),
            (Some(market), Data::GdaxTrade(trade)) => trade
                .insert(
                    &self.pools[&Database::Gdax],
                    &self.market_names[&self.instance.exchange_name.unwrap()][&market],
                )
                .await
                .expect("Failed to insert trade into db."),
            (None, d) => panic!("Market missing from ws data: {:?}", d),
        }
    }

    pub async fn handle_ws_error(&self, e: WsError) -> bool {
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
                    _ => {
                        println!("Other WSError::Tungstenite protocol error {:?}", perr);
                        println!("to_string(): {:?}", perr.to_string());
                        panic!();
                    }
                },
                _ => {
                    println!("Other WSError::Tungstenite error {:?}", e);
                    println!("to_string(): {:?}", e.to_string());
                    panic!();
                }
            },
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
                ExchangeName::Gdax => channels.push(Channel::Ticker(market.market_name.to_owned())),
            };
        }
        channels
    }
}

impl Mita {
    pub async fn stream(&self) -> bool {
        // Run ::reset_trade_tables for ws before calling this function or the table
        // may not exist that the streamed trades write to.
        // Initiate channel and map data structures
        let mut channels = Vec::new();
        let mut market_details = HashMap::new();
        // Drop and re-create _ws table for markets and populate data structures
        for market in self.markets.iter() {
            match &self.exchange.name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    channels.push(Channel::Trades(market.market_name.to_owned()))
                }
                ExchangeName::Gdax => channels.push(Channel::Ticker(market.market_name.to_owned())),
            };
            market_details.insert(market.market_name.as_str(), market);
        }
        // println!("Market details map: {:?}", market_details);
        // Create ws client
        let mut ws = WebSocket::connect(&self.exchange.name)
            .await
            .expect("Failed to connect to ws.");
        // Subscribe to trades channels for each market
        match ws.subscribe(channels).await {
            Ok(_) => {}
            Err(WsError::MissingSubscriptionConfirmation) => {
                println!("Missing subscription confirmation, sleep 5s and restart.");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                return true;
            }
            Err(e) => {
                println!("Subscription error: {:?}", e);
                return false;
            }
        };
        // Loop forever writing each trade to the database
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((Some(market), Data::FtxTrade(trade))) => {
                    insert_ftx_trade(
                        &self.trade_pool,
                        &self.exchange.name,
                        market_details[market.as_str()],
                        "ws",
                        trade,
                    )
                    .await
                    .expect("Could not insert trade from ws into db.");
                }
                Ok((Some(market), Data::GdaxTrade(trade))) => {
                    // println!("Market: {:?}", market);
                    insert_gdax_trade(
                        &self.trade_pool,
                        &self.exchange.name,
                        market_details[market.as_str()],
                        "ws",
                        trade,
                    )
                    .await
                    .expect("Failed to insert ws trade to db.");
                }
                Ok((None, d)) => {
                    panic!("Market missing from ws data: {:?}", d);
                }
                Err(WsError::Tungstenite(e)) => match e {
                    tokio_tungstenite::tungstenite::Error::Io(ioerr) => match ioerr.kind() {
                        ErrorKind::ConnectionReset => {
                            println!("Error Kind: ConnectionReset.");
                            println!("to_string(): {:?}", ioerr.to_string());
                            break true;
                        }
                        ErrorKind::UnexpectedEof => {
                            println!("Error Kind: UnexpectedEof.");
                            println!("to_string(): {:?}", ioerr.to_string());
                            break true;
                        }
                        _ => {
                            println!("Other Error Kind: {:?}.", ioerr.kind());
                            println!("to_string(): {:?}", ioerr.to_string());
                            break false;
                        }
                    },
                    tokio_tungstenite::tungstenite::Error::Protocol(err) => match err {
                        ProtocolError::SendAfterClosing => {
                            println!("Error Kind: Protocol SendAfterClosing.");
                            println!("to_string(): {:?}", err.to_string());
                            break true;
                        }
                        _ => {
                            println!("Other WSError::Tungstenite protocol error {:?}", err);
                            println!("to_string(): {:?}", err.to_string());
                            panic!();
                        }
                    },
                    _ => {
                        println!("Other WSError::Tungstenite error {:?}", e);
                        println!("to_string(): {:?}", e.to_string());
                        panic!();
                    }
                },
                // Err(WsError::Tungstenite(tokio_tungstenite::tungstenite::Error::Io(std::io::Error(ErrorKind)))) => {
                Err(e) => {
                    panic!("Other WsError {:?}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::configuration::get_configuration;
    use crate::exchanges::select_exchanges;
    use crate::exchanges::ws::{Channel, Data, WebSocket};
    use crate::markets::{select_market_detail, select_market_ids_by_exchange};
    use crate::trades::{create_ftx_trade_table, drop_trade_table, insert_ftx_trade};
    use futures::StreamExt;
    use sqlx::PgPool;

    #[tokio::test]
    async fn stream_trades_to_db() {
        // Grab config settings and stream those trades to db table
        let configuration = get_configuration().expect("Could not get configuration.");
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges from db.");
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == configuration.application.exchange)
            .unwrap();
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
            .await
            .expect("Could not fetch markets.");
        let market_id = market_ids
            .iter()
            .find(|m| m.market_name == configuration.application.market)
            .unwrap();
        let market_detail = select_market_detail(&pool, market_id)
            .await
            .expect("Could not fetch market detail.");
        drop_trade_table(&pool, &exchange.name, &market_detail, "ws_test")
            .await
            .expect("Could not drop test ws table.");
        create_ftx_trade_table(&pool, &exchange.name, &market_detail, "ws_test")
            .await
            .expect("Could not create test ws table.");
        let mut ws = WebSocket::connect(&exchange.name)
            .await
            .expect("Failed to connect to ws.");
        ws.subscribe(vec![Channel::Trades(market_id.market_name.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::FtxTrade(trade))) => {
                    insert_ftx_trade(&pool, &exchange.name, &market_detail, "ws_test", trade)
                        .await
                        .expect("Could not insert trade into db.");
                }
                _ => panic!("Unexpected data type."),
            }
        }
    }
}
