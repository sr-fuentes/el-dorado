use crate::exchanges::{
    ftx::Channel, ftx::Data, ftx::WsClient, ftx::WsError, ExchangeName,
};
use crate::mita::Mita;
use crate::trades::insert_ftx_trade;
use futures::StreamExt;
use std::collections::HashMap;
use std::io::ErrorKind;

impl Mita {
    pub async fn stream(&self) {
        // Run ::reset_trade_tables for ws before calling this function or the table
        // may not exist that the streamed trades write to.
        // Initiate channel and map data structures
        let mut channels = Vec::new();
        let mut map_ids = HashMap::new();
        let mut map_strip_names = HashMap::new();
        // Drop and re-create _ws table for markets and populate data structures
        for market in self.markets.iter() {
            channels.push(Channel::Trades(market.market_name.to_owned()));
            map_ids.insert(market.market_name.as_str(), market.market_id);
            map_strip_names.insert(market.market_name.as_str(), market.strip_name());
        }
        // Create ws client
        let mut ws = match self.exchange.name {
            ExchangeName::FtxUs => WsClient::connect_us()
                .await
                .expect("Could not connect to ws."),
            ExchangeName::Ftx => WsClient::connect_intl()
                .await
                .expect("Could not connect ws."),
        };
        // Subscribe to trades channels for each market
        ws.subscribe(channels)
            .await
            .expect("Could not subscribe to each market.");
        // Loop forever writing each trade to the database
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((Some(market), Data::Trade(trade))) => {
                    insert_ftx_trade(
                        &self.pool,
                        &map_ids[market.as_str()],
                        self.exchange.name.as_str(),
                        map_strip_names[market.as_str()].as_str(),
                        "ws",
                        trade,
                    )
                    .await
                    .expect("Could not insert trade from ws into db.");
                }
                Ok((None, Data::Trade(trade))) => {
                    panic!("Market missing from ws trade: {:?}", trade);
                }
                Err(WsError::Tungstenite(e)) => match e {
                    tokio_tungstenite::tungstenite::Error::Io(ioerr) => match ioerr.kind() {
                        ErrorKind::ConnectionReset => {
                            println!("Error Kind: ConnectionReset.");
                            println!("to_string(): {:?}", ioerr.to_string());
                            break;
                        }
                        ErrorKind::UnexpectedEof => {
                            println!("Error Kind: UnexpectedEof.");
                            println!("to_string(): {:?}", ioerr.to_string());
                            break;
                        }
                        _ => {
                            println!("Other Error Kind: {:?}.", ioerr.kind());
                            println!("to_string(): {:?}", ioerr.to_string());
                            break;
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
    use crate::exchanges::{
        ftx::{Channel, Data, WsClient},
        ExchangeName,
    };
    use crate::markets::{select_market_detail, select_market_ids_by_exchange};
    use crate::trades::{create_ftx_trade_table, drop_ftx_trade_table, insert_ftx_trade};
    use futures::StreamExt;
    use sqlx::PgPool;

    #[tokio::test]
    async fn stream_trades_to_db() {
        // Grab config settings and stream those trades to db table
        let configuration = get_configuration().expect("Could not get configuration.");
        let pool = PgPool::connect_with(configuration.database.with_db())
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
        let _market_detail = select_market_detail(&pool, market_id)
            .await
            .expect("Could not fetch market detail.");
        drop_ftx_trade_table(
            &pool,
            &exchange.name.as_str(),
            market_id.strip_name().as_str(),
            "ws_test",
        )
        .await
        .expect("Could not drop test ws table.");
        create_ftx_trade_table(
            &pool,
            &exchange.name.as_str(),
            market_id.strip_name().as_str(),
            "ws_test",
        )
        .await
        .expect("Could not create test ws table.");
        let mut ws = match exchange.name {
            ExchangeName::FtxUs => WsClient::connect_us().await.expect("Could not connect ws."),
            ExchangeName::Ftx => WsClient::connect_intl()
                .await
                .expect("Could not connect ws."),
        };
        ws.subscribe(vec![Channel::Trades(market_id.market_name.to_owned())])
            .await
            .expect("Could not subscribe to market.");
        loop {
            let data = ws.next().await.expect("No data received.");
            match data {
                Ok((_, Data::Trade(trade))) => {
                    insert_ftx_trade(
                        &pool,
                        &market_id.market_id,
                        &exchange.name.as_str(),
                        market_id.strip_name().as_str(),
                        "ws_test",
                        trade,
                    )
                    .await
                    .expect("Could not insert trade into db.");
                }
                _ => panic!("Unexpected data type."),
            }
        }
    }
}
