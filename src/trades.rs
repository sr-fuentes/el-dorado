use crate::exchanges::ftx::Trade;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

pub async fn create_ftx_trade_table(
    pool: &PgPool,
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Create trade table
    // trades_EXCHANGE_MARKET_SOURCE
    // trades_ftx_btcperp_rest
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS trades_{}_{}_{} (
            market_id uuid NOT NULL,
            trade_id BIGINT NOT NULL,
            PRIMARY KEY (trade_id),
            price NUMERIC NOT NULL,
            size NUMERIC NOT NULL,
            side TEXT NOT NULL,
            liquidation BOOLEAN NOT NULL,
            time timestamptz NOT NULL
        )
        "#,
        exchange_name, market_table_name, trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    // Create index on time
    let sql = format!(
        r#"
        CREATE INDEX IF NOT EXISTS trades_{e}_{m}_{t}_time_asc
        ON trades_{e}_{m}_{t} (time)
        "#,
        e = exchange_name,
        m = market_table_name,
        t = trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_ftx_trade_table(
    pool: &PgPool,
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Drop the table if it exists
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS trades_{}_{}_{}
        "#,
        exchange_name, market_table_name, trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn insert_ftx_trades(
    pool: &PgPool,
    market_id: &Uuid,
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
    trades: Vec<Trade>,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, liquidation, time)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name, market_table_name, trade_table
    );
    for trade in trades.iter() {
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
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<Trade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        "#,
        exchange_name, market_table_name, trade_table,
    );
    let rows = sqlx::query_as::<_, Trade>(&sql)
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
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DELETE FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        "#,
        exchange_name, market_table_name, trade_table,
    );
    sqlx::query(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_ftx_trades_by_id(
    pool: &PgPool,
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
    first_trade_id: i64,
    last_trade_id: i64,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DELETE FROM trades_{}_{}_{}
        WHERE trade_id >= $1 and trade_id <= $2
        "#,
        exchange_name, market_table_name, trade_table,
    );
    sqlx::query(&sql)
        .bind(first_trade_id)
        .bind(last_trade_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_trades_by_market_table(
    pool: &PgPool,
    exchange_name: &str,
    market_table_name: &str,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DELETE FROM trades_{}_{}_{}
        WHERE 1 = 1
        "#,
        exchange_name, market_table_name, trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
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
