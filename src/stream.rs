#[cfg(test)]
mod tests {
    use crate::configuration::get_configuration;
    use crate::exchanges::fetch_exchanges;
    use crate::exchanges::ftx::{Channel, Data, WsClient};
    use crate::markets::{fetch_markets, select_market_detail};
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
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges from db.");
        let exchange = exchanges
            .iter()
            .find(|e| e.exchange_name == configuration.application.exchange)
            .unwrap();
        let market_ids = fetch_markets(&pool, exchange)
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
            &exchange.exchange_name,
            market_id.strip_name().as_str(),
            "ws_test",
        )
        .await
        .expect("Could not drop test ws table.");
        create_ftx_trade_table(
            &pool,
            &exchange.exchange_name,
            market_id.strip_name().as_str(),
            "ws_test",
        )
        .await
        .expect("Could not create test ws table.");
        let mut ws = match exchange.exchange_name.as_str() {
            "ftxus" => WsClient::connect_us().await.expect("Could not connect ws."),
            "ftx" => WsClient::connect_intl()
                .await
                .expect("Could not connect ws."),
            _ => panic!("No ws client exists for this exchange."),
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
                        &exchange.exchange_name,
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
