use crate::{
    candles::ResearchCandle,
    configuration::Database,
    eldorado::ElDorado,
    exchanges::{ftx::Market, gdax::Product, Exchange, ExchangeName},
    trades::{PrIdTi, Trade},
    utilities::TimeFrame,
};
use chrono::{DateTime, Duration, DurationRound, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MarketDetail {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub market_name: String,
    pub market_type: String,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub underlying: Option<String>,
    pub market_status: MarketStatus,
    pub market_data_status: MarketStatus,
    pub mita: Option<String>,
    pub candle_timeframe: Option<TimeFrame>,
    pub last_candle: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct MarketRank {
    pub market_id: Uuid,
    pub market_name: String,
    pub rank: i64,
    pub rank_prev: Option<i64>,
    pub mita_current: Option<String>,
    pub mita_proposed: Option<String>,
    pub usd_volume_24h: Decimal,
    pub usd_volume_15t: Decimal,
    pub ats_v1: Decimal,
    pub ats_v2: Decimal,
    pub mps: Decimal,
    pub dp_quantity: i32,
    pub dp_price: i32,
    pub min_quantity: Decimal,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MarketTradeDetail {
    pub market_id: Uuid,
    pub market_start_ts: Option<DateTime<Utc>>,
    pub first_trade_ts: DateTime<Utc>,
    pub first_trade_id: String,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub previous_trade_day: DateTime<Utc>,
    pub previous_status: MarketDataStatus,
    pub next_trade_day: Option<DateTime<Utc>>,
    pub next_status: Option<MarketDataStatus>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MarketCandleDetail {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub market_name: String,
    pub time_frame: TimeFrame,
    pub first_candle: DateTime<Utc>,
    pub last_candle: DateTime<Utc>,
    pub last_trade_ts: DateTime<Utc>,
    pub last_trade_id: String,
    pub last_trade_price: Decimal,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MarketArchiveDetail {
    pub market_id: Uuid,
    pub exchange_name: ExchangeName,
    pub market_name: String,
    pub first_candle_dt: DateTime<Utc>,
    pub first_trade_dt: DateTime<Utc>,
    pub first_trade_price: Decimal,
    pub first_trade_id: String,
    pub last_candle_dt: DateTime<Utc>,
    pub last_trade_dt: DateTime<Utc>,
    pub last_trade_price: Decimal,
    pub last_trade_id: String,
    pub next_month: DateTime<Utc>,
    pub tf: TimeFrame,
}

impl MarketDetail {
    pub fn as_strip(&self) -> String {
        self.market_name.replace(&['/', '-'][..], "")
    }
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum MarketStatus {
    // Market is new and has never been run
    New,
    // Market is backfilling from start to current last trade
    Backfill,
    // Market is syncing between the backfill and the streamed trades collected while backfilling
    Sync,
    // Market is active in loop to stream trades and create candles
    Active,
    // Market has crashed the the program is restarting.
    Restart,
    // Market is backfilling from start to current start of day
    Historical,
    // Market is not available for streaming or backfilling. Ignore completely.
    Terminated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum MarketDataStatus {
    // Data process is complete. No other action is needed.
    Completed,
    // Next step is to get the data needed
    Get,
    // Next step is to validate whatever data exists
    Validate,
    // Next step is to archived on IG in ompressed form and sync to at least 1 external drive
    Archive,
}

impl MarketStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketStatus::New => "new",
            MarketStatus::Backfill => "backfill",
            MarketStatus::Sync => "sync",
            MarketStatus::Active => "active",
            MarketStatus::Restart => "restart",
            MarketStatus::Historical => "historical",
            MarketStatus::Terminated => "terminated",
        }
    }
}

impl MarketDataStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketDataStatus::Completed => "completed",
            MarketDataStatus::Get => "get",
            MarketDataStatus::Validate => "validate",
            MarketDataStatus::Archive => "archive",
        }
    }
}

impl TryFrom<String> for MarketStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "backfill" => Ok(Self::Backfill),
            "sync" => Ok(Self::Sync),
            "active" => Ok(Self::Active),
            "restart" => Ok(Self::Restart),
            "historical" => Ok(Self::Historical),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported market status.", other)),
        }
    }
}

impl TryFrom<String> for MarketDataStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "completed" => Ok(Self::Completed),
            "get" => Ok(Self::Get),
            "validate" => Ok(Self::Validate),
            "archive" => Ok(Self::Archive),
            other => Err(format!("{} isnot a supported market data status.", other)),
        }
    }
}

impl MarketDetail {
    pub fn new_from_gdax_product(product: &Product) -> Self {
        MarketDetail {
            market_id: Uuid::new_v4(),
            exchange_name: ExchangeName::Gdax,
            market_name: product.id.clone(),
            market_type: "spot".to_string(), // All gdax markets are spot
            base_currency: Some(product.base_currency.clone()),
            quote_currency: Some(product.quote_currency.clone()),
            underlying: None,
            market_status: MarketStatus::New,
            market_data_status: MarketStatus::New,
            mita: None,
            candle_timeframe: None,
            last_candle: None,
        }
    }

    pub fn new_from_ftx_market(exchange: &Exchange, market: &Market) -> Self {
        MarketDetail {
            market_id: Uuid::new_v4(),
            exchange_name: exchange.name,
            market_name: market.name.clone(),
            market_type: market.market_type.clone(),
            base_currency: market.base_currency.clone(),
            quote_currency: market.quote_currency.clone(),
            underlying: market.underlying.clone(),
            market_status: MarketStatus::New,
            market_data_status: MarketStatus::New,
            mita: None,
            candle_timeframe: None,
            last_candle: None,
        }
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO markets (
                market_id, exchange_name, market_name, market_type, base_currency,
                quote_currency, underlying, market_status, market_data_status, mita,
                candle_timeframe, last_candle)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)    
            "#,
            self.market_id,
            self.exchange_name.as_str(),
            self.market_name,
            self.market_type,
            self.base_currency,
            self.quote_currency,
            self.underlying,
            self.market_status.as_str(),
            self.market_data_status.as_str(),
            self.mita,
            match self.candle_timeframe {
                Some(tf) => Some(tf.as_str()),
                None => None,
            },
            self.last_candle
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_last_candle(
        &self,
        pool: &PgPool,
        dt: &DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE markets
            SET last_candle  = $1
            WHERE market_id = $2
            "#,
            dt,
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn select_all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name, market_type, base_currency, quote_currency, underlying,
                market_status as "market_status: MarketStatus",
                market_data_status as "market_data_status: MarketStatus",
                mita,
                candle_timeframe as "candle_timeframe: TimeFrame",
                last_candle
                FROM markets
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_all_join_candle_detail(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT m.market_id as "market_id!",
                m.exchange_name as "exchange_name!: ExchangeName",
                m.market_name as "market_name!", 
                m.market_type as "market_type!", 
                m.base_currency, m.quote_currency, m.underlying,
                m.market_status as "market_status!: MarketStatus",
                m.market_data_status as "market_data_status!: MarketStatus",
                m.mita,
                m.candle_timeframe as "candle_timeframe: TimeFrame",
                m.last_candle
                FROM markets m
                INNER JOIN market_candle_details mcd
                ON m.market_id = mcd.market_id
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_exchange_mita(
        pool: &PgPool,
        exchange: &ExchangeName,
        mita: &str,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name, market_type, base_currency, quote_currency, underlying,
                market_status as "market_status: MarketStatus",
                market_data_status as "market_data_status: MarketStatus",
                mita,
                candle_timeframe as "candle_timeframe: TimeFrame",
                last_candle
                FROM markets
            WHERE exchange_name = $1
            AND mita = $2
            "#,
            exchange.as_str(),
            mita
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_exchange(
        pool: &PgPool,
        exchange: &ExchangeName,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name, market_type, base_currency, quote_currency, underlying,
                market_status as "market_status: MarketStatus",
                market_data_status as "market_data_status: MarketStatus",
                mita,
                candle_timeframe as "candle_timeframe: TimeFrame",
                last_candle
                FROM markets
            WHERE exchange_name = $1
            "#,
            exchange.as_str(),
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_status(
        pool: &PgPool,
        status: &MarketStatus,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name, market_type, base_currency, quote_currency, underlying,
                market_status as "market_status: MarketStatus",
                market_data_status as "market_data_status: MarketStatus",
                mita,
                candle_timeframe as "candle_timeframe: TimeFrame",
                last_candle
                FROM markets
            WHERE market_data_status = $1
            "#,
            status.as_str(),
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_id(pool: &PgPool, market_id: &Uuid) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name, market_type, base_currency, quote_currency, underlying,
                market_status as "market_status: MarketStatus",
                market_data_status as "market_data_status: MarketStatus",
                mita,
                candle_timeframe as "candle_timeframe: TimeFrame",
                last_candle
                FROM markets
            WHERE market_id = $1
            "#,
            market_id,
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    pub async fn map_markets(
        pool: &PgPool,
    ) -> (
        HashMap<Uuid, Self>,
        HashMap<ExchangeName, HashMap<String, MarketDetail>>,
    ) {
        let mut markets_by_id = HashMap::new();
        let mut markets_by_exchange_name: HashMap<ExchangeName, HashMap<String, MarketDetail>> =
            HashMap::new();
        let markets = Self::select_all(pool)
            .await
            .expect("Failed to select markets.");
        for market in markets.iter() {
            markets_by_id.insert(market.market_id, market.clone());
            markets_by_exchange_name
                .entry(market.exchange_name)
                .and_modify(|hm| {
                    hm.insert(market.market_name.clone(), market.clone());
                })
                .or_insert_with(|| HashMap::from([(market.market_name.clone(), market.clone())]));
        }
        (markets_by_id, markets_by_exchange_name)
    }
}

impl MarketTradeDetail {
    pub async fn new(market: &MarketDetail, first_trade: &PrIdTi) -> Self {
        // TODO: Fix edge case where first candle for day has 0 volume and first_trade is from
        // previous day
        // Create new market trade detail from market give the first trade provided.
        Self {
            market_id: market.market_id,
            market_start_ts: None,
            first_trade_ts: first_trade.dt,
            first_trade_id: first_trade.id.to_string(),
            last_trade_ts: first_trade.dt, // Duplicate first id/ts as true last id/ts
            last_trade_id: first_trade.id.to_string(), // populated with step forward.
            previous_trade_day: first_trade.dt.duration_trunc(Duration::days(1)).unwrap()
                - Duration::days(1),
            previous_status: MarketDataStatus::Get,
            next_trade_day: None,
            next_status: None,
        }
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO market_trade_details (
                market_id, market_start_ts, first_trade_ts, first_trade_id, last_trade_ts,
                last_trade_id, previous_trade_day, previous_status, next_trade_day, next_status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            self.market_id,
            self.market_start_ts,
            self.first_trade_ts,
            self.first_trade_id,
            self.last_trade_ts,
            self.last_trade_id,
            self.previous_trade_day,
            self.previous_status.as_str(),
            self.next_trade_day,
            self.next_status.as_ref().map(|ns| ns.as_str()),
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_prev_status(
        &self,
        pool: &PgPool,
        status: &MarketDataStatus,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET previous_status = $1
            WHERE market_id = $2
            "#,
            status.as_str(),
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts: self.first_trade_ts,
            first_trade_id: self.first_trade_id.clone(),
            last_trade_ts: self.last_trade_ts,
            last_trade_id: self.last_trade_id.clone(),
            previous_trade_day: self.previous_trade_day,
            previous_status: *status,
            next_trade_day: self.next_trade_day,
            next_status: self.next_status,
        })
    }

    pub async fn update_next_status(
        &self,
        pool: &PgPool,
        status: &MarketDataStatus,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET next_status = $1
            WHERE market_id = $2
            "#,
            status.as_str(),
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts: self.first_trade_ts,
            first_trade_id: self.first_trade_id.clone(),
            last_trade_ts: self.last_trade_ts,
            last_trade_id: self.last_trade_id.clone(),
            previous_trade_day: self.previous_trade_day,
            previous_status: self.previous_status,
            next_trade_day: self.next_trade_day,
            next_status: Some(*status),
        })
    }

    pub async fn update_prev_day_prev_status(
        &self,
        pool: &PgPool,
        datetime: &DateTime<Utc>,
        status: &MarketDataStatus,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET (previous_trade_day, previous_status) = ($1, $2)
            WHERE market_id = $3
            "#,
            datetime,
            status.as_str(),
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts: self.first_trade_ts,
            first_trade_id: self.first_trade_id.clone(),
            last_trade_ts: self.last_trade_ts,
            last_trade_id: self.last_trade_id.clone(),
            previous_trade_day: *datetime,
            previous_status: *status,
            next_trade_day: self.next_trade_day,
            next_status: self.next_status,
        })
    }

    pub async fn update_next_day_next_status(
        &self,
        pool: &PgPool,
        datetime: &DateTime<Utc>,
        status: &MarketDataStatus,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET (next_trade_day, next_status) = ($1, $2)
            WHERE market_id = $3
            "#,
            datetime,
            status.as_str(),
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts: self.first_trade_ts,
            first_trade_id: self.first_trade_id.clone(),
            last_trade_ts: self.last_trade_ts,
            last_trade_id: self.last_trade_id.clone(),
            previous_trade_day: self.previous_trade_day,
            previous_status: self.previous_status,
            next_trade_day: Some(*datetime),
            next_status: Some(*status),
        })
    }

    pub async fn update_first_trade(
        &self,
        pool: &PgPool,
        datetime: &DateTime<Utc>,
        trade_id: &str,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET (first_trade_ts, first_trade_id, last_trade_ts, last_trade_id) = ($1, $2, $3, $4)
            WHERE market_id = $5
            "#,
            datetime,
            trade_id,
            datetime,
            trade_id,
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts: *datetime,
            first_trade_id: trade_id.to_string(),
            last_trade_ts: *datetime, // Last trade will be updated on ffill
            last_trade_id: trade_id.to_string(), // Last trade will be update on ffill
            previous_trade_day: self.previous_trade_day,
            previous_status: self.previous_status,
            next_trade_day: self.next_trade_day,
            next_status: self.next_status,
        })
    }

    pub async fn update_first_and_last_trades(
        &self,
        pool: &PgPool,
        first: &dyn Trade,
        last: &dyn Trade,
    ) -> Result<Self, sqlx::Error> {
        let first_trade_ts = first.time().min(self.first_trade_ts);
        let first_trade_id = first
            .trade_id()
            .min(self.first_trade_id.parse::<i64>().unwrap());
        sqlx::query!(
            r#"
            UPDATE market_trade_details
            SET (first_trade_ts, first_trade_id, last_trade_ts, last_trade_id) = ($1, $2, $3, $4)
            WHERE market_id = $5
            "#,
            first_trade_ts,
            first_trade_id.to_string(),
            last.time(),
            last.trade_id().to_string(),
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            market_start_ts: self.market_start_ts,
            first_trade_ts,
            first_trade_id: first_trade_id.to_string(),
            last_trade_ts: last.time(),
            last_trade_id: last.trade_id().to_string(),
            previous_trade_day: self.previous_trade_day,
            previous_status: self.previous_status,
            next_trade_day: self.next_trade_day,
            next_status: self.next_status,
        })
    }

    pub async fn select_all(pool: &PgPool) -> Result<Vec<MarketTradeDetail>, sqlx::Error> {
        let rows = sqlx::query_as!(
            MarketTradeDetail,
            r#"
            SELECT market_id, market_start_ts, first_trade_ts, first_trade_id, last_trade_ts,
                last_trade_id, previous_trade_day,
                previous_status as "previous_status: MarketDataStatus",
                next_trade_day,
                next_status as "next_status: MarketDataStatus"
            FROM market_trade_details
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select(
        pool: &PgPool,
        market: &MarketDetail,
    ) -> Result<MarketTradeDetail, sqlx::Error> {
        let row = sqlx::query_as!(
            MarketTradeDetail,
            r#"
            SELECT market_id, market_start_ts, first_trade_ts, first_trade_id, last_trade_ts,
                last_trade_id, previous_trade_day,
                previous_status as "previous_status: MarketDataStatus",
                next_trade_day,
                next_status as "next_status: MarketDataStatus"
            FROM market_trade_details
            WHERE market_id = $1
            "#,
            market.market_id
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }
}

impl MarketCandleDetail {
    pub fn new(market: &MarketDetail, tf: &TimeFrame, candles: &[ResearchCandle]) -> Self {
        // Assert that there are candles in the given vec slice
        assert!(!candles.is_empty());
        let last = candles.last().unwrap();
        Self {
            market_id: market.market_id,
            exchange_name: market.exchange_name,
            market_name: market.market_name.clone(),
            time_frame: *tf,
            first_candle: candles.first().unwrap().datetime,
            last_candle: last.datetime,
            last_trade_ts: last.last_trade_ts,
            last_trade_id: last.last_trade_id.clone(),
            last_trade_price: last.close,
        }
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO market_candle_details (
                market_id, exchange_name, market_name, time_frame, first_candle, last_candle,
                last_trade_ts, last_trade_id, last_trade_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            self.market_id,
            self.exchange_name.as_str(),
            self.market_name,
            self.time_frame.as_str(),
            self.first_candle,
            self.last_candle,
            self.last_trade_ts,
            self.last_trade_id,
            self.last_trade_price,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_last(
        &self,
        pool: &PgPool,
        candle: &ResearchCandle,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_candle_details
            SET (last_candle, last_trade_ts, last_trade_id, last_trade_price) = ($1, $2, $3, $4)
            WHERE market_id = $5
            "#,
            candle.datetime,
            candle.last_trade_ts,
            candle.last_trade_id,
            candle.close,
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            exchange_name: self.exchange_name,
            market_name: self.market_name.clone(),
            time_frame: self.time_frame,
            first_candle: self.first_candle,
            last_candle: candle.datetime,
            last_trade_ts: candle.last_trade_ts,
            last_trade_id: candle.last_trade_id.clone(),
            last_trade_price: candle.close,
        })
    }

    pub async fn select_all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name,
                time_frame as "time_frame: TimeFrame",
                first_candle, last_candle, last_trade_ts, last_trade_id, last_trade_price
            FROM market_candle_details
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select(pool: &PgPool, market: &MarketDetail) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Self,
            r#"
            SELECT market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name,
                time_frame as "time_frame: TimeFrame",
                first_candle, last_candle, last_trade_ts, last_trade_id, last_trade_price
            FROM market_candle_details
            WHERE market_id = $1
            "#,
            market.market_id
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    pub fn last_as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.last_trade_id.parse::<i64>().unwrap(),
            dt: self.last_trade_ts,
            price: self.last_trade_price,
        }
    }
}

impl MarketArchiveDetail {
    pub fn new(
        market: &MarketDetail,
        tf: &TimeFrame,
        first_candle: &ResearchCandle,
        last_candle: &ResearchCandle,
    ) -> Self {
        Self {
            market_id: market.market_id,
            exchange_name: market.exchange_name,
            market_name: market.market_name.clone(),
            tf: *tf,
            first_candle_dt: first_candle.datetime,
            first_trade_dt: first_candle.first_trade_ts,
            first_trade_price: first_candle.open,
            first_trade_id: first_candle.first_trade_id.clone(),
            last_candle_dt: last_candle.datetime,
            last_trade_dt: last_candle.last_trade_ts,
            last_trade_price: last_candle.close,
            last_trade_id: last_candle.last_trade_id.clone(),
            next_month: last_candle.datetime + tf.as_dur(),
        }
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO market_archive_details (
                market_id, exchange_name, market_name, tf, first_candle_dt, first_trade_dt,
                first_trade_price, first_trade_id, last_candle_dt, last_trade_dt, last_trade_price,
                last_trade_id, next_month)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
            self.market_id,
            self.exchange_name.as_str(),
            self.market_name,
            self.tf.as_str(),
            self.first_candle_dt,
            self.first_trade_dt,
            self.first_trade_price,
            self.first_trade_id,
            self.last_candle_dt,
            self.last_trade_dt,
            self.last_trade_price,
            self.last_trade_id,
            self.next_month,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn select(pool: &PgPool, market: &MarketDetail) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Self,
            r#"
            SELECT
                market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name,
                tf as "tf: TimeFrame",
                first_candle_dt, first_trade_dt, first_trade_price, first_trade_id,
                last_candle_dt, last_trade_dt, last_trade_price, last_trade_id,
                next_month
            FROM market_archive_details
            WHERE market_id = $1
            "#,
            market.market_id
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    pub async fn select_all(pool: &PgPool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Self,
            r#"
            SELECT
                market_id,
                exchange_name as "exchange_name: ExchangeName",
                market_name,
                tf as "tf: TimeFrame",
                first_candle_dt, first_trade_dt, first_trade_price, first_trade_id,
                last_candle_dt, last_trade_dt, last_trade_price, last_trade_id,
                next_month
            FROM market_archive_details
            "#,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub fn last_as_pridti(&self) -> PrIdTi {
        PrIdTi {
            id: self.last_trade_id.parse::<i64>().unwrap(),
            dt: self.last_trade_dt,
            price: self.last_trade_price,
        }
    }

    pub async fn update(
        &self,
        pool: &PgPool,
        next_month: &DateTime<Utc>,
        last_candle: &ResearchCandle,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE market_archive_details
            SET (last_candle_dt, last_trade_dt, last_trade_price, last_trade_id, next_month) = ($1, $2, $3, $4, $5)
            WHERE market_id = $6
            "#,
            last_candle.datetime,
            last_candle.last_trade_ts,
            last_candle.close,
            last_candle.last_trade_id,
            next_month,
            self.market_id,
        )
        .execute(pool)
        .await?;
        Ok(Self {
            market_id: self.market_id,
            exchange_name: self.exchange_name,
            market_name: self.market_name.clone(),
            tf: self.tf,
            first_candle_dt: self.first_candle_dt,
            first_trade_dt: self.first_trade_dt,
            first_trade_price: self.first_trade_price,
            first_trade_id: self.first_trade_id.clone(),
            last_candle_dt: last_candle.datetime,
            last_trade_dt: last_candle.last_trade_ts,
            last_trade_price: last_candle.close,
            last_trade_id: last_candle.last_trade_id.clone(),
            next_month: *next_month,
        })
    }
}
// impl Inquisidor {

// pub async fn update_market_ranks(&self) {
//     // Get user input for exchange to add
//     let exchange: String = get_input("Enter Exchange to Rank:");
//     // Parse input to see if there is a valid exchange
//     let exchange: ExchangeName = exchange.try_into().unwrap();
//     // Get current exchanges from db
//     let exchanges = select_exchanges(&self.ig_pool)
//         .await
//         .expect("Failed to fetch exchanges.");
//     // Compare input to existing exchanges
//     if !exchanges.iter().any(|e| e.name == exchange) {
//         // Exchange not added
//         println!("{:?} has not been added to El-Dorado.", exchange);
//         return;
//     }
//     match exchange {
//         ExchangeName::Ftx | ExchangeName::FtxUs => {
//             self.update_market_ranks_generic::<crate::exchanges::ftx::Market>(&exchange)
//                 .await
//         }

//         ExchangeName::Gdax => {
//             // I dont think this works..no volume on get products to rank
//             self.update_market_ranks_generic::<crate::exchanges::gdax::Product>(&exchange)
//                 .await
//         }
//     };
// }

// pub async fn update_market_ranks_generic<
//     T: crate::utilities::Market + DeserializeOwned + std::clone::Clone,
// >(
//     &self,
//     exchange: &ExchangeName,
// ) {
//     // Get terminated markets from database
//     let markets_terminated = select_market_details_by_status_exchange(
//         &self.ig_pool,
//         exchange,
//         &MarketStatus::Terminated,
//     )
//     .await
//     .expect("Failed to select terminated markets.");
//     // Get market details from db TODO: derive market term from this list to reduce db calls
//     let market_details = select_market_details(&self.ig_pool)
//         .await
//         .expect("Failed to select market details.");
//     // Get USD markets from exchange
//     let markets_exch: Vec<T> = get_usd_markets(&self.clients[exchange], exchange).await;
//     println!("# exchange markets: {}", markets_exch.len());
//     // Filter out non-terminated markets and non-perp markets
//     let mut filtered_markets: Vec<T> = match *exchange {
//         ExchangeName::Ftx | ExchangeName::FtxUs => markets_exch
//             .iter()
//             .filter(|m| {
//                 m.market_type() == "future"
//                     && !markets_terminated
//                         .iter()
//                         .any(|tm| tm.market_name == m.name())
//                     && m.name().split('-').last() == Some("PERP")
//             })
//             .cloned()
//             .collect(),
//         ExchangeName::Gdax => markets_exch,
//     };
//     // println!("Filtered markets: {:?}", filtered_markets);
//     // Sort by 24h volume
//     filtered_markets.sort_by_key(|m2| Reverse(m2.usd_volume_24h()));
//     // Create ranks table and select current contents
//     create_market_ranks_table(&self.ig_pool, exchange)
//         .await
//         .expect("Failed to create market ranks table.");
//     let previous_ranks = select_market_ranks(&self.ig_pool, exchange)
//         .await
//         .expect("Failed to select market ranks.");
//     // Create empty vec to hold new ranks
//     let mut new_ranks = Vec::new();
//     let proposal = self.mita_proposal();
//     // Set rank counter = 1
//     let mut rank: i64 = 1;
//     for market in filtered_markets.iter() {
//         // Check if there is a previous record for market
//         let previous_rank = previous_ranks
//             .iter()
//             .find(|pr| pr.market_name == market.name());
//         let rank_prev = previous_rank.map(|pr| pr.rank);
//         // Get MarketDetail for id and current mita fields
//         let market_detail = market_details
//             .iter()
//             .find(|md| md.market_name == market.name())
//             .unwrap();
//         let (market_id, mita_current) = (market_detail.market_id, market_detail.mita.clone());
//         let proposed_mita = proposal.get(&rank).map(|m| m.to_string());
//         // If previous mita is none AND the proposal is not none - set proposal to mita-09 as
//         // It will need time to catch up and should not be streamed immediately
//         let mita_proposed = if mita_current == None && proposed_mita != None {
//             Some("mita-09".to_string())
//         } else {
//             proposed_mita
//         };
//         let new_rank = MarketRank {
//             market_id,
//             market_name: market.name(),
//             rank,
//             rank_prev,
//             mita_current,
//             mita_proposed,
//             usd_volume_24h: market.usd_volume_24h().unwrap().round(),
//             usd_volume_15t: (market.usd_volume_24h().unwrap() / dec!(96)).round(),
//             ats_v1: (market.usd_volume_24h().unwrap() / dec!(24) * dec!(0.05)).round_dp(2),
//             ats_v2: (market.usd_volume_24h().unwrap() / dec!(96) * dec!(0.05)).round_dp(2),
//             mps: (market.usd_volume_24h().unwrap() * dec!(0.005)).round_dp(2),
//             dp_quantity: market.dp_quantity(),
//             dp_price: market.dp_price(),
//             min_quantity: market.min_quantity().unwrap(),
//         };
//         new_ranks.push(new_rank);
//         rank += 1;
//     }
//     // Drop market ranks table
//     drop_market_ranks_table(&self.ig_pool, exchange)
//         .await
//         .expect("Failed to drop market ranks.");
//     // Create market ranks table
//     create_market_ranks_table(&self.ig_pool, exchange)
//         .await
//         .expect("Failed to create market ranks table.");
//     // Insert markets
//     for new_rank in new_ranks.iter() {
//         // Insert rank
//         insert_market_rank(&self.ig_pool, exchange, new_rank)
//             .await
//             .expect("Failed to insert market rank.");
//     }
// }

// pub async fn update_market_mitas_from_ranks(&self) {
//     // Get user input for exchange to add
//     let exchange: String = get_input("Enter Exchange to Rank:");
//     // Parse input to see if there is a valid exchange
//     let exchange: ExchangeName = exchange.try_into().unwrap();
//     // Get current exchanges from db
//     let exchanges = select_exchanges(&self.ig_pool)
//         .await
//         .expect("Failed to fetch exchanges.");
//     // Compare input to existing exchanges
//     if !exchanges.iter().any(|e| e.name == exchange) {
//         // Exchange not added
//         println!("{:?} has not been added to El-Dorado.", exchange);
//         return;
//     }
//     // Get market ranks for exchange
//     let ranks = select_market_ranks(&self.ig_pool, &exchange)
//         .await
//         .expect("Failed to select market ranks.");
//     // For each rank - update market mita column, update rank set current = proposed
//     for rank in ranks.iter() {
//         if rank.mita_current != rank.mita_proposed {
//             // Update mita in markets table
//             update_market_mita(&self.ig_pool, &rank.market_id, &rank.mita_proposed)
//                 .await
//                 .expect("Failed to update mita in markets.");
//             // Update mita_current in market_ranks table
//             update_market_ranks_current_mita(
//                 &self.ig_pool,
//                 &exchange,
//                 &rank.mita_proposed,
//                 &rank.market_id,
//             )
//             .await
//             .expect("Failed to update mita in market ranks.");
//         }
//     }
// }

//     fn mita_proposal(&self) -> HashMap<i64, String> {
//         let mut proposal = HashMap::new();
//         // Create map for proposed mitas: 1-48 in streams, 49-75 in daily catchups
//         proposal.insert("mita-01", vec![1, 40, 41]);
//         proposal.insert("mita-02", vec![2, 39, 42]);
//         proposal.insert("mita-03", vec![3, 14, 15, 26, 27, 38, 43]);
//         proposal.insert("mita-04", vec![4, 13, 16, 25, 28, 37, 44]);
//         proposal.insert("mita-05", vec![5, 12, 17, 24, 29, 36, 45]);
//         proposal.insert("mita-06", vec![6, 11, 18, 23, 30, 35, 46]);
//         proposal.insert("mita-07", vec![7, 10, 19, 22, 31, 34, 47]);
//         proposal.insert("mita-08", vec![8, 9, 20, 21, 32, 33, 48]);
//         let mut mita_09 = Vec::new();
//         for i in 49..75 {
//             mita_09.push(i);
//         }
//         proposal.insert("mita-09", mita_09);
//         // Create map for return proposal (k,v) = (1,"mita-01")
//         let mut proposal_map = HashMap::new();
//         for (k, v) in proposal.iter() {
//             for i in v.iter() {
//                 proposal_map.insert(*i as i64, k.to_string());
//             }
//         }
//         proposal_map
//     }
// }

impl ElDorado {
    // Map a vec of market details to the two hashmaps needed for ElDorado instances so that lookups
    // can be made on either market id or exchange/market_name
    pub fn map_markets(
        market_details: &[MarketDetail],
    ) -> (
        HashMap<ExchangeName, HashMap<String, MarketDetail>>,
        HashMap<Uuid, MarketDetail>,
    ) {
        let mut markets = HashMap::new();
        let mut market_ids = HashMap::new();
        for md in market_details.iter() {
            markets
                .entry(md.exchange_name)
                .and_modify(|hm: &mut HashMap<String, MarketDetail>| {
                    hm.insert(md.market_name.clone(), md.clone());
                })
                .or_insert_with(|| HashMap::from([(md.market_name.clone(), md.clone())]));
            market_ids.insert(md.market_id, md.clone());
        }
        (markets, market_ids)
    }

    pub async fn select_markets_eligible_for_fill(&self) -> Option<Vec<MarketDetail>> {
        // Eligibility based on active status and existing last candle
        // Select active markets from eldorado db
        let markets =
            MarketDetail::select_by_status(&self.pools[&Database::ElDorado], &MarketStatus::Active)
                .await
                .expect("Failed to select market details.");
        // Filter for markets with a last candle
        let eligible_markets: Vec<MarketDetail> = markets
            .into_iter()
            .filter(|m| {
                m.last_candle.is_some()
                    && m.candle_timeframe.is_some()
                    && m.exchange_name == ExchangeName::Gdax
            })
            .collect();
        if !eligible_markets.is_empty() {
            Some(eligible_markets)
        } else {
            None
        }
    }

    pub async fn select_markets_eligible_for_archive(&self) -> Option<Vec<MarketDetail>> {
        // Eligibility based on if there is a market candle detail record created for market
        // Select markets with market candle detail
        let markets = MarketDetail::select_all_join_candle_detail(&self.pools[&Database::ElDorado])
            .await
            .expect("Failed to select markets.");
        // Filter for markets that are currently active
        let eligible_markets: Vec<MarketDetail> = markets
            .into_iter()
            .filter(|m| m.market_status == MarketStatus::Active)
            .collect();
        if !eligible_markets.is_empty() {
            Some(eligible_markets)
        } else {
            None
        }
    }

    pub async fn select_market_trade_detail(&self, market: &MarketDetail) -> MarketTradeDetail {
        // Try selecting record from database
        match MarketTradeDetail::select(&self.pools[&Database::ElDorado], market).await {
            Ok(mtd) => mtd,
            Err(sqlx::Error::RowNotFound) => {
                // First get the first for the market - either production candle or 01d candle if
                // legacy format. Try the 01d candle first.
                match self.select_first_eld_trade_as_pridti(market).await {
                    Some(p) => {
                        let mtd = MarketTradeDetail::new(market, &p).await;
                        mtd.insert(&self.pools[&Database::ElDorado])
                            .await
                            .expect("Failed to insert mtd.");
                        mtd
                    }
                    None => panic!("No first candle to make market trade detail."),
                }
            }
            Err(e) => panic!("SQLX Error: {:?}", e),
        }
    }

    // Get user input for market, then validate against either all markets in the db or against
    // the vec of markets passed in as a param
    pub async fn prompt_market_input(
        &self,
        markets: &Option<Vec<MarketDetail>>,
    ) -> Option<MarketDetail> {
        // Get user input
        let market: String = self.get_input("Please enter market: ").await;
        // Validate input
        match markets {
            Some(ms) => ms.iter().cloned().find(|m| m.market_name == market),
            None => {
                // Select all markets and then filter for match
                let all_markets = MarketDetail::select_all(&self.pools[&Database::ElDorado])
                    .await
                    .expect("Failed to select all markets.");
                all_markets.into_iter().find(|m| m.market_name == market)
            }
        }
    }

    pub async fn refresh_ftx_markets(&self, _exchange: &Exchange) {} // TODO: even w/dead code}

    pub async fn refresh_gdax_markets(&self) {
        // Get markets from Gdax Rest API
        let mut markets = self.clients[&ExchangeName::Gdax]
            .get_gdax_products()
            .await
            .expect("Failed to get gdax markets.");
        // Filter for USD markets - iterates twice for usd filter and match. TODO: combine in one
        markets.retain(|m| m.quote_currency == *"USD");
        // Get markets from el dorado db
        let db_markets =
            MarketDetail::select_by_exchange(&self.pools[&Database::ElDorado], &ExchangeName::Gdax)
                .await
                .expect("Failed to select markets from db.");
        // If the rest api market is not in the db, add to db
        for market in markets.iter() {
            if !db_markets.iter().any(|m| m.market_name == market.id) {
                // Add market
                println!("Adding {:?} market for Gdax", market.id);
                let new_market = MarketDetail::new_from_gdax_product(market);
                new_market
                    .insert(&self.pools[&Database::ElDorado])
                    .await
                    .expect("Failed to insert market.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{configuration::get_configuration, markets::MarketDetail};
    use sqlx::PgPool;

    #[tokio::test]
    pub async fn strip_name_removes_dash_and_slash() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get input from config for market to archive
        let market_ids = MarketDetail::select_all(&pool)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == configuration.application.market)
            .unwrap();

        // Print market and strip market
        println!("Market: {:?}", market);
        let stripped_market = market.as_strip();
        println!("Stripped Market: {}", stripped_market);
    }

    // #[tokio::test]
    // async fn select_active_markets_returns_active_markets() {
    //     let ig = Inquisidor::new().await;
    //     let markets = MarketDetail::select_by_status(&ig.ig_pool, &MarketStatus::Active)
    //         .await
    //         .expect("Failed to select markets.");
    //     println!("Acive markets: {:?}", markets);
    // }
}
