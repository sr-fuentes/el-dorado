use crate::events::Event;
use crate::exchanges::{
    error::RestError, ftx::Trade as FtxTrade, gdax::Trade as GdaxTrade, ExchangeName,
};
use crate::inquisidor::Inquisidor;
use crate::markets::MarketDetail;
use crate::mita::Mita;
use crate::utilities::{TimeFrame, Trade};
use chrono::{DateTime, DurationRound, Utc};
use csv::Reader;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs::File;

impl Mita {
    pub async fn reset_trade_tables(&self, tables: &[&str]) {
        for market in self.markets.iter() {
            for table in tables.iter() {
                if *table == "processed" || *table == "validated" {
                    // Alter table, create, migrate, drop
                    alter_create_migrate_drop_trade_table(
                        &self.trade_pool,
                        &self.exchange.name,
                        market,
                        *table,
                    )
                    .await
                    .expect("Failed to alter create migrate drop trade table.");
                } else {
                    // "ws" or "rest", just drop and re-create each time
                    drop_create_trade_table(&self.trade_pool, &self.exchange.name, market, *table)
                        .await
                        .expect("Failed to drop and create table.");
                }
            }
        }
    }

    pub async fn create_trade_tables(&self, tables: &[&str]) {
        for market in self.markets.iter() {
            for table in tables.iter() {
                // Create the trade table to exchange specifications
                match self.exchange.name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        create_ftx_trade_table(
                            &self.trade_pool,
                            &self.exchange.name,
                            market,
                            *table,
                        )
                        .await
                        .expect("Failed to create trade table.");
                    }
                    ExchangeName::Gdax => {
                        create_gdax_trade_table(
                            &self.trade_pool,
                            &self.exchange.name,
                            market,
                            *table,
                        )
                        .await
                        .expect("Failed to create trade table.");
                    }
                }
            }
        }
    }
}

impl Inquisidor {
    pub async fn get_ftx_trades_dr_into_table(
        &self,
        event: &Event,
        table_suf: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) {
        let market = self.market(&event.market_id);
        create_ftx_trade_table(&self.ftx_pool, &event.exchange_name, market, table_suf)
            .await
            .expect("Failed to create trade table.");
        // Fill trade table with trades
        let mut end_or_last_trade = end;
        let mut total_trades: usize = 0;
        while start < end_or_last_trade {
            // Prevent 429 errors by only requesting 4 per second
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = match self.clients[&event.exchange_name]
                .get_ftx_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(start),
                    Some(end_or_last_trade),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() || e.is_connect() || e.is_request() {
                        println!(
                            "Timeout/Connect/Request error. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_status() {
                        match e.status() {
                            Some(s) => match s.as_u16() {
                                500 | 502 | 503 | 504 | 520 | 522 | 530 => {
                                    println!(
                                        "{} status code. Waiting 30 seconds before retry {:?}",
                                        s, e
                                    );
                                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                                    continue;
                                }
                                _ => {
                                    panic!("Status code not handled: {:?} {:?}", s, e)
                                }
                            },
                            None => panic!("No status code for request error: {:?}", e),
                        }
                    } else {
                        panic!("Error (not timeout / connect / request): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            let num_trades = new_trades.len();
            total_trades += num_trades; // Add to running total of trades
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                end_or_last_trade = new_trades.first().unwrap().time;
                let first_trade = new_trades.last().unwrap().time;
                println!(
                    "{} trade returned. First: {}, Last: {}",
                    num_trades, end_or_last_trade, first_trade
                );
                insert_ftx_trades(
                    &self.ftx_pool,
                    &event.exchange_name,
                    market,
                    table_suf,
                    new_trades,
                )
                .await
                .expect("Failed to insert backfill ftx trades.");
            };
            if num_trades < 5000 && total_trades > 0 {
                // Trades returned less than REST API limit. No more trades to retreive.
                break;
            };
        }
    }

    pub async fn select_ftx_trades_by_table(
        &self,
        table: &str,
    ) -> Result<Vec<FtxTrade>, sqlx::Error> {
        // Cannot user query_as! macro because table may not exist at compile time
        let sql = format!(
            r#"
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM {}
            ORDER BY trade_id
            "#,
            table
        );
        let rows = sqlx::query_as::<_, FtxTrade>(&sql)
            .fetch_all(&self.ftx_pool)
            .await?;
        Ok(rows)
    }

    pub async fn load_trades_for_dr(
        &self,
        market: &MarketDetail,
        dr: &[DateTime<Utc>],
    ) -> HashMap<DateTime<Utc>, Vec<FtxTrade>> {
        // For each date - load the trades and append to vec of trades
        // Create hashmap of candle datetimes to store trades
        let mut candle_dr_map: HashMap<DateTime<Utc>, Vec<FtxTrade>> = HashMap::new();
        for d in dr.iter() {
            // Load trades for day
            println!("{:?} - Loading trades for {:?}", Utc::now(), d);
            let archive_path = format!(
                "{}/trades/{}/{}/{}/{}",
                &self.settings.application.archive_path,
                &market.exchange_name.as_str(),
                &market.as_strip(),
                d.format("%Y"),
                d.format("%m")
            );
            let f = format!("{}_{}.csv", market.as_strip(), d.format("%F"));
            let a_path = std::path::Path::new(&archive_path).join(f.clone());
            // Set file
            let file = File::open(a_path).expect("Failed to open file.");
            let mut rdr = Reader::from_reader(file);
            for result in rdr.deserialize() {
                let record: FtxTrade = result.expect("Faile to deserialize record.");
                candle_dr_map
                    .entry(record.time.duration_trunc(TimeFrame::S15.as_dur()).unwrap())
                    .and_modify(|v| v.push(record.clone()))
                    .or_insert_with(|| vec![record.clone()]);
            }
        }
        candle_dr_map
    }
}

// ALTER, DROP, MIGRATE actions are the same regardless of exchange
// CREATE, INSERT, SELECT actions are unique to each exchange trades struct

pub async fn select_insert_delete_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            // Select ftx trades according to params
            let trades =
                select_ftx_trades_by_time(pool, exchange_name, market, source, start, end).await?;
            // Insert ftx trades into destination table
            insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
        }
        ExchangeName::Gdax => {
            let trades = select_gdax_trades_by_time(pool, market, source, start, end).await?;
            insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
        }
    }
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

pub async fn select_insert_drop_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            // Select ftx trades according to params
            let trades =
                select_ftx_trades_by_time(pool, exchange_name, market, source, start, end).await?;
            // Insert ftx trades into destination table
            insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
        }
        ExchangeName::Gdax => {
            let trades = select_gdax_trades_by_time(pool, market, source, start, end).await?;
            insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
        }
    }
    // Drop source table
    drop_trade_table(pool, exchange_name, market, source).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_delete_ftx_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
    trades: Vec<FtxTrade>,
) -> Result<(), sqlx::Error> {
    // Insert ftx trades into destination table
    insert_ftx_trades(pool, exchange_name, market, destination, trades).await?;
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn insert_delete_gdax_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    source: &str,
    destination: &str,
    trades: Vec<GdaxTrade>,
) -> Result<(), sqlx::Error> {
    // Insert ftx trades into destination table
    insert_gdax_trades(pool, exchange_name, market, destination, trades).await?;
    // Delete trades form source table
    delete_trades_by_time(pool, exchange_name, market, source, start, end).await?;
    Ok(())
}

pub async fn drop_create_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Drop the table if it exists
    drop_trade_table(pool, exchange_name, market, trade_table).await?;
    // Create the trade table to exchange specifications
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            create_ftx_trade_table(pool, exchange_name, market, trade_table).await?
        }
        ExchangeName::Gdax => {
            create_gdax_trade_table(pool, exchange_name, market, trade_table).await?
        }
    }
    Ok(())
}

pub async fn alter_create_migrate_drop_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Alter table to temp name
    alter_trade_table_to_temp(pool, exchange_name, market, trade_table).await?;
    // Create both the original trade table AND the temp table. There is a
    // scenario where the original table does not exists, thus the altered
    // table is not created. This will cause a panic in the migrate
    // function when it tries to select from a temp table that does not exists.
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            create_ftx_trade_table(pool, exchange_name, market, trade_table).await?;
            create_ftx_trade_table(
                pool,
                exchange_name,
                market,
                format!("{}_temp", trade_table).as_str(),
            )
            .await?;
        }
        ExchangeName::Gdax => {
            create_gdax_trade_table(pool, exchange_name, market, trade_table).await?;
            create_gdax_trade_table(
                pool,
                exchange_name,
                market,
                format!("{}_temp", trade_table).as_str(),
            )
            .await?;
        }
    }
    // Migrate trades fromm temp
    migrate_trades_from_temp(pool, exchange_name, market, trade_table).await?;
    // Finally drop the temp table
    drop_trade_table(
        pool,
        exchange_name,
        market,
        format!("{}_temp", trade_table).as_str(),
    )
    .await?;
    Ok(())
}

pub async fn create_ftx_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
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
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    // Create index on time
    let sql = format!(
        r#"
        CREATE INDEX IF NOT EXISTS trades_{e}_{m}_{t}_time_asc
        ON trades_{e}_{m}_{t} (time)
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn create_gdax_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
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
            time timestamptz NOT NULL
        )
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    // Create index on time
    let sql = format!(
        r#"
        CREATE INDEX IF NOT EXISTS trades_{e}_{m}_{t}_time_asc
        ON trades_{e}_{m}_{t} (time)
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_trade_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    // Drop the table if it exists
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS trades_{}_{}_{}
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_table(pool: &PgPool, table: &str) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS {}
        "#,
        table
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn alter_trade_table_to_temp(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        ALTER TABLE IF EXISTS trades_{e}_{m}_{t}
        RENAME TO trades_{e}_{m}_{t}_temp
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn migrate_trades_from_temp(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        INSERT INTO trades_{e}_{m}_{t}
        SELECT * FROM trades_{e}_{m}_{t}_temp
        "#,
        e = exchange_name.as_str(),
        m = market.as_strip(),
        t = trade_table,
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn insert_ftx_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trades: Vec<FtxTrade>,
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
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    for trade in trades.iter() {
        sqlx::query(&sql)
            .bind(market.market_id)
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

pub async fn insert_gdax_trades(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trades: Vec<GdaxTrade>,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, time)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    for trade in trades.iter() {
        sqlx::query(&sql)
            .bind(market.market_id)
            .bind(trade.trade_id)
            .bind(trade.price)
            .bind(trade.size)
            .bind(&trade.side)
            .bind(trade.time)
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn insert_gdax_trade(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trade: GdaxTrade,
) -> Result<(), sqlx::Error> {
    // Cannot user sqlx query! macro because table may not exist at
    // compile time and table name is dynamic to ftx and ftxus.
    let sql = format!(
        r#"
        INSERT INTO trades_{}_{}_{} (
            market_id, trade_id, price, size, side, time)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (trade_id) DO NOTHING
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql)
        .bind(market.market_id)
        .bind(trade.trade_id)
        .bind(trade.price)
        .bind(trade.size)
        .bind(&trade.side)
        .bind(trade.time)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_ftx_trade(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    trade: FtxTrade,
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
        exchange_name.as_str(),
        market.as_strip(),
        trade_table
    );
    sqlx::query(&sql)
        .bind(market.market_id)
        .bind(trade.id)
        .bind(trade.price)
        .bind(trade.size)
        .bind(&trade.side)
        .bind(trade.liquidation)
        .bind(trade.time)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_ftx_trades_by_time(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<FtxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        ORDER BY trade_id
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    let rows = sqlx::query_as::<_, FtxTrade>(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_gdax_trades_by_time(
    pool: &PgPool,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<Vec<GdaxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        ORDER BY trade_id
        "#,
        ExchangeName::Gdax.as_str(),
        market.as_strip(),
        trade_table,
    );
    let rows = sqlx::query_as::<_, GdaxTrade>(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn select_ftx_trades_by_table(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<FtxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM {}
        ORDER BY trade_id
        "#,
        table
    );
    let rows = sqlx::query_as::<_, FtxTrade>(&sql).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn select_gdax_trades_by_table(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<GdaxTrade>, sqlx::Error> {
    // Cannot user query_as! macro because table may not exist at compile time
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM {}
        ORDER BY trade_id
        "#,
        table
    );
    let rows = sqlx::query_as::<_, GdaxTrade>(&sql).fetch_all(pool).await?;
    Ok(rows)
}

pub async fn select_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<(DateTime<Utc>, Option<i64>), sqlx::Error> {
    match exchange_name {
        ExchangeName::Ftx | ExchangeName::FtxUs => {
            let ftx_trade = select_ftx_trade_first_stream(pool, exchange_name, market).await?;
            Ok((ftx_trade.time(), Some(ftx_trade.trade_id())))
        }
        ExchangeName::Gdax => {
            let gdax_trade = select_gdax_trade_first_stream(pool, exchange_name, market).await?;
            Ok((gdax_trade.time(), Some(gdax_trade.trade_id())))
        }
    }
}

pub async fn select_ftx_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<FtxTrade, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT trade_id as id, price, size, side, liquidation, time
        FROM trades_{}_{}_ws
        ORDER BY trade_id
        LIMIT 1
        "#,
        exchange_name.as_str(),
        market.as_strip()
    );
    let row = sqlx::query_as::<_, FtxTrade>(&sql).fetch_one(pool).await?;
    Ok(row)
}

pub async fn select_gdax_trade_first_stream(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
) -> Result<GdaxTrade, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT trade_id, price, size, side, time
        FROM trades_{}_{}_ws
        ORDER BY trade_id
        LIMIT 1
        "#,
        exchange_name.as_str(),
        market.as_strip()
    );
    let row = sqlx::query_as::<_, GdaxTrade>(&sql).fetch_one(pool).await?;
    Ok(row)
}

pub async fn delete_trades_by_time(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    market: &MarketDetail,
    trade_table: &str,
    interval_start: DateTime<Utc>,
    interval_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DELETE FROM trades_{}_{}_{}
        WHERE time >= $1 and time < $2
        "#,
        exchange_name.as_str(),
        market.as_strip(),
        trade_table,
    );
    sqlx::query(&sql)
        .bind(interval_start)
        .bind(interval_end)
        .execute(pool)
        .await?;
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
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
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
