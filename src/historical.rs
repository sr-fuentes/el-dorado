use crate::exchanges::ftx::*;
use crate::exchanges::Exchange;
use crate::markets::MarketId;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub async fn insert_ftx_trades(
    pool: &PgPool,
    market: &MarketId,
    trades: Vec<Trade>,
) -> Result<(), sqlx::Error> {
    for trade in trades.iter() {
        sqlx::query!(
            r#"
                INSERT INTO ftx_trades (
                    market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
            market.market_id,
            trade.id,
            trade.price,
            trade.size,
            trade.side,
            trade.liquidation,
            trade.time
        )
        .execute(pool)
        .await?;
    }
    Ok(())
}

pub async fn insert_ftxus_trades(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    trades: Vec<Trade>,
) -> Result<(), sqlx::Error> {
    for trade in trades.iter() {
        let sql = format!(
            r#"
                INSERT INTO {}_trades (
                    market_id, trade_id, price, size, side, liquidation, time)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
            "#,
            exchange.exchange_name,
        );
        println!("Query: {}", sql);
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