use chrono::{DateTime, Utc};
use rust_decimal::prelude::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trade {
    pub datetime: DateTime<Utc>,
    pub id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: String,
    pub liquidation: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Trades(Vec<Trade>);

impl Trade {
    pub const BUY: &'static str = "buy";
    pub const SELL: &'static str = "sell";

    pub fn new(
        datetime: DateTime<Utc>,
        id: String,
        price: Decimal,
        size: Decimal,
        side: &str,
        liquidation: bool,
    ) -> Self {
        Self {
            datetime,
            id,
            price,
            size,
            side: side.to_string(),
            liquidation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;
    use crate::configuration::*;
    use sqlx::PgPool;

    #[test]
    pub fn new_trade_returns_trade() {
        let new_trade = Trade::new(
            Utc.timestamp(1524886322, 0),
            1.to_string(),
            dec!(70.2),
            dec!(23.1),
            Trade::BUY,
            false,
        );
        println!("New Trade: {:?}", new_trade);
    }

    #[tokio::test]
    pub async fn insert_dup_trades_returns_error() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        
        // Create table
        let sql = r#"
            CREATE TABLE trades_test (
                trade_id BIGINT NOT NULL,
                PRIMARY KEY (trade_id)
            )    
        "#;
        sqlx::query(&sql).execute(&pool).await.expect("Could not create table.");


        // Create trade
        let sql_insert = r#"
            INSERT INTO trades_test (trade_id)
            VALUES ($1)
            ON CONFLICT (trade_id) DO NOTHING
        "#;
        let trade_id = 1;
        
        // Insert trade once
        sqlx::query(&sql_insert).bind(trade_id).execute(&pool).await.expect("Could not insert trade first time.");
        // INsert trade a second time
        sqlx::query(&sql_insert).bind(trade_id).execute(&pool).await.expect("Could not insert trade second time.");
        // match sqlx::query(&sql_insert).bind(trade_id).execute(&pool).await {
        //     Ok(_) => (),
        //     Err(e) => panic!(),
        // };
    }

}
