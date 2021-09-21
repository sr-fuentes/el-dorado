use crate::exchanges::{ftx::Candle as FtxCandle, ftx::RestClient, ftx::Trade, Exchange};
use crate::markets::{MarketDetail, MarketId};
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use sqlx::PgPool;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, sqlx::FromRow)]
pub struct Candle {
    pub datetime: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub volume_net: Decimal,
    pub volume_liquidation: Decimal,
    pub value: Decimal,
    pub trade_count: i64,
    pub liquidation_count: i64,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct Candles(Vec<Candle>);

impl Candle {
    // Takes a Vec of Trade and aggregates into a Candle with the Datetime = the
    // datetime passed as argument. Candle built from trades in the order they are in
    // the Vec, sort before calling this function otherwise Open / Close / Datetime may
    // be incorrect.
    pub fn new_from_trades(datetime: DateTime<Utc>, trades: &Vec<Trade>) -> Self {
        let candle_tuple = trades.iter().fold(
            (
                trades.first().expect("No trade to make candle.").price, // open
                Decimal::MIN,                                            // high
                Decimal::MAX,                                            // low
                dec!(0),                                                 // close
                dec!(0),                                                 // volume
                dec!(0),                                                 // volume_net
                dec!(0),                                                 // volume_liquidation
                dec!(0),                                                 // value
                0,                                                       // count
                0,                                                       // liquidation_count,
                datetime,                                                // last_trade_ts
                "".to_string(),                                          // last_trade_id
            ),
            |(o, h, l, _c, v, vn, vl, a, n, ln, _ts, _id), t| {
                (
                    o,
                    h.max(t.price),
                    l.min(t.price),
                    t.price,
                    v + t.size,
                    if t.side == "sell" {
                        vn + (t.size * dec!(-1))
                    } else {
                        vn + t.size
                    },
                    if t.liquidation { vl + t.size } else { vl },
                    a + (t.size * t.price),
                    n + 1,
                    if t.liquidation { ln + 1 } else { ln },
                    t.time,
                    t.id.to_string(),
                )
            },
        );
        Self {
            datetime,
            open: candle_tuple.0,
            high: candle_tuple.1,
            low: candle_tuple.2,
            close: candle_tuple.3,
            volume: candle_tuple.4,
            volume_net: candle_tuple.5,
            volume_liquidation: candle_tuple.6,
            value: candle_tuple.7,
            trade_count: candle_tuple.8,
            liquidation_count: candle_tuple.9,
            last_trade_ts: candle_tuple.10,
            last_trade_id: candle_tuple.11,
        }
    }

    // This function will build a placeholder trade with 0 volume and
    // will populate OHLC from the last trade provided.
    pub fn new_from_last(
        datetime: DateTime<Utc>,
        last_trade_price: Decimal,
        last_trade_ts: DateTime<Utc>,
        last_trade_id: String,
    ) -> Self {
        Self {
            datetime,
            open: last_trade_price, // All OHLC are = last trade price
            high: last_trade_price,
            low: last_trade_price,
            close: last_trade_price,
            volume: dec!(0),
            volume_net: dec!(0),
            volume_liquidation: dec!(0),
            value: dec!(0),
            trade_count: 0,
            liquidation_count: 0,
            last_trade_ts,
            last_trade_id,
        }
    }
}

pub async fn insert_candle(
    pool: &PgPool,
    market: &MarketId,
    exchange: &Exchange,
    candle: Candle,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
            INSERT INTO candles_15T_{} (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                market_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        "#,
        exchange.exchange_name,
    );
    sqlx::query(&sql)
        .bind(candle.datetime)
        .bind(candle.open)
        .bind(candle.high)
        .bind(candle.low)
        .bind(candle.close)
        .bind(candle.volume)
        .bind(candle.volume_net)
        .bind(candle.volume_liquidation)
        .bind(candle.value)
        .bind(candle.trade_count)
        .bind(candle.liquidation_count)
        .bind(candle.last_trade_ts)
        .bind(candle.last_trade_id)
        .bind(false)
        .bind(market.market_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_unvalidated_candles(
    pool: &PgPool,
    exchange: &Exchange,
    market: &MarketId,
) -> Result<Vec<Candle>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT * FROM candles_15t_{}
        WHERE market_id = $1 and not is_validated
        ORDER BY datetime
       "#,
        exchange.exchange_name
    );
    let rows = sqlx::query_as::<_, Candle>(&sql)
        .bind(market.market_id)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn validate_candle(
    client: &RestClient,
    exchange: &Exchange,
    market: &MarketId,
    candle: &Candle,
) -> bool {
    // Get candle from exchange, start and end = candle start to return one candle
    let mut exchange_candles = client
        .get_candles(
            &market.market_name,
            Some(900),
            Some(candle.datetime),
            Some(candle.datetime),
        )
        .await
        .expect("Failed to fetch candles.");
    println!("Unvalidated Candle: {:?}", candle);
    println!("Exchange candles: {:?}", exchange_candles);
    let delta = (candle.value / exchange_candles.pop().unwrap().volume) - dec!(1.0);
    println!("Candle delta: {:?}", delta);
    // FTX candle validation on FTX Volume = ED Value, FTX sets open = last trade event if the
    // last trades was in the prior time period. Consider valid if w/in 5 bps of candle value.
    if delta < dec!(0.0005) {
        true
    } else {
        false
    }
}

pub async fn update_candle_validation(
    pool: &PgPool,
    market: &MarketId,
    candle: &Candle,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
            UPDATE candles_15t_ftxus
            SET is_validated = True
            WHERE datetime = $1
            AND market_id = $2
        "#,
        candle.datetime,
        market.market_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    pub fn sample_trades() -> Vec<Trade> {
        let mut trades: Vec<Trade> = Vec::new();
        trades.push(Trade {
            id: 1,
            price: Decimal::new(702, 1),
            size: Decimal::new(23, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524886322, 0),
        });
        trades.push(Trade {
            id: 2,
            price: Decimal::new(752, 1),
            size: Decimal::new(64, 1),
            side: "buy".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524887322, 0),
        });
        trades.push(Trade {
            id: 3,
            price: Decimal::new(810, 1),
            size: Decimal::new(4, 1),
            side: "buy".to_string(),
            liquidation: true,
            time: Utc.timestamp(1524888322, 0),
        });
        trades.push(Trade {
            id: 4,
            price: Decimal::new(767, 1),
            size: Decimal::new(13, 1),
            side: "sell".to_string(),
            liquidation: false,
            time: Utc.timestamp(1524889322, 0),
        });
        trades
    }

    #[test]
    pub fn new_from_last_returns_candle_populated_from_last_trade() {
        let mut trades = sample_trades();
        let last_trade = trades.pop().unwrap();
        let candle = Candle::new_from_last(
            last_trade.time,
            last_trade.price,
            last_trade.time,
            last_trade.id.to_string(),
        );
        println!("Candle: {:?}", candle);
    }

    #[test]
    pub fn new_from_trades_returns_candle() {
        let trades = sample_trades();
        let first_trade = trades.first().unwrap();
        let candle = Candle::new_from_trades(first_trade.time, &trades);
        println!("Candle: {:?}", candle);
    }
}
