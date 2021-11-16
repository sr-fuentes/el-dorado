use crate::exchanges::ftx::Trade;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

pub async fn insert_ftx_trades(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    trades: Vec<Trade>,
    table: &str,
) -> Result<(), sqlx::Error> {
    for trade in trades.iter() {
        // Cannot user sqlx query! macro because table may not exist at
        // compile time and table name is dynamic to ftx and ftxus.
        let sql = match exchange_name {
            "temp" => format!(
                r#"
                INSERT INTO {} (
                        market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (trade_id) DO NOTHING
                "#,
                table
            ),
            _ => format!(
                r#"
                INSERT INTO trades_{}_{} (
                    market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (trade_id) DO NOTHING
                "#,
                exchange_name, table
            ),
        };
        sqlx::query(&sql)
            .bind(market_id)
            .bind(trade.id)
            .bind(trade.price)
            .bind(trade.size)
            .bind(&trade.side)
            .bind(trade.liquidation)
            .bind(trade.time)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn select_ftx_trades_by_time(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    is_processed: bool,
    is_validated: bool,
) -> Result<Vec<Trade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = if is_processed {
        format!(
            r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_processed
        WHERE market_id = $1 AND time >= $2 and time < $3
        "#,
            exchange_name
        )
    } else if is_validated {
        format!(
            r#"
          SELECT trade_id as id, price, size, side, liquidation, time
          FROM trades_{}_validated
          WHERE market_id = $1 AND time >= $2 AND time < $3
          "#,
            exchange_name
        )
    } else {
        format!(
            r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{table}_rest
        WHERE market_id = $1 AND time >= $2 and time < $3
        UNION
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{table}_ws
        WHERE market_id = $1 AND time >= $2 and time < $3
        "#,
            table = exchange_name
        )
    };
    let rows = sqlx::query_as::<_, Trade>(&sql)
        .bind(market_id)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_ftx_trades_by_table(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<Trade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM {}
        ORDER BY time
        "#,
        table
    );
    let rows = sqlx::query_as::<_, Trade>(&sql).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn delete_ftx_trades_by_time(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
    is_processed: bool,
    is_validated: bool,
) -> Result<(), sqlx::Error> {
    let mut tables = Vec::new();
    if is_processed {
        tables.push("processed");
    } else if is_validated {
        tables.push("validated");
    } else {
        tables.push("rest");
        tables.push("ws");
    };
    for table in tables {
        let sql = format!(
            r#"
                DELETE FROM trades_{}_{}
                WHERE market_id = $1 AND time >= $2 and time <$3
            "#,
            exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market_id)
            .bind(interval_start)
            .bind(interval_end)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn delete_ftx_trades_by_id(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    first_trade_id: i64,
    last_trade_id: i64,
    is_processed: bool,
    is_validated: bool,
) -> Result<(), sqlx::Error> {
    let mut tables = Vec::new();
    if is_processed {
        tables.push("processed");
    } else if is_validated {
        tables.push("validated");
    } else {
        tables.push("rest");
        tables.push("ws");
    };
    for table in tables {
        let sql = format!(
            r#"
                DELETE FROM trades_{}_{}
                WHERE market_id = $1 AND trade_id >= $2 and trade_id <= $3
            "#,
            exchange_name, table
        );
        sqlx::query(&sql)
            .bind(market_id)
            .bind(first_trade_id)
            .bind(last_trade_id)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn delete_trades_by_market_table(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            DELETE FROM trades_{}_{}
            WHERE market_id = $1
        "#,
        exchange_name, trade_table
    );
    sqlx::query(&sql).bind(market_id).execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::configuration::*;
    use sqlx::PgPool;

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
        sqlx::query(&sql)
            .execute(&pool)
            .await
            .expect("Could not create table.");

        // Create trade
        let sql_insert = r#"
            INSERT INTO trades_test (trade_id)
            VALUES ($1)
            ON CONFLICT (trade_id) DO NOTHING
        "#;
        let trade_id = 1;

        // Insert trade once
        sqlx::query(&sql_insert)
            .bind(trade_id)
            .execute(&pool)
            .await
            .expect("Could not insert trade first time.");
        // INsert trade a second time
        sqlx::query(&sql_insert)
            .bind(trade_id)
            .execute(&pool)
            .await
            .expect("Could not insert trade second time.");
        // match sqlx::query(&sql_insert).bind(trade_id).execute(&pool).await {
        //     Ok(_) => (),
        //     Err(e) => panic!(),
        // };
    }
}
