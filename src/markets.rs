use crate::candles::{select_first_01d_candle, TimeFrame};
use crate::exchanges::{client::RestClient, error::RestError, select_exchanges, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::utilities::get_input;
use chrono::{DateTime, Duration, Utc};
use core::cmp::Reverse;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
pub struct MarketId {
    pub market_id: Uuid,
    pub market_name: String,
}

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

impl MarketId {
    pub fn as_strip(&self) -> String {
        self.market_name.replace(&['/', '-'][..], "")
    }
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

impl MarketTradeDetail {
    pub async fn new(pool: &PgPool, market: &MarketDetail) -> Self {
        // Create new market trade detail from market detail.
        // First get the first 01d candle for the market
        let first_candle = select_first_01d_candle(pool, &market.market_id)
            .await
            .expect("Failed to select first candle.");
        Self {
            market_id: market.market_id,
            market_start_ts: None,
            first_trade_ts: first_candle.first_trade_ts,
            first_trade_id: first_candle.first_trade_id.clone(),
            last_trade_ts: first_candle.first_trade_ts, // Duplicate first id/ts as true last id/ts
            last_trade_id: first_candle.first_trade_id.clone(), // populated with step forward.
            previous_trade_day: first_candle.datetime - Duration::days(1),
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

    pub async fn select(pool: &PgPool, market_id: &Uuid) -> Result<MarketTradeDetail, sqlx::Error> {
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
            market_id
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }
}

impl Inquisidor {
    pub fn list_markets(&self, exchange: Option<&ExchangeName>) -> Vec<String> {
        // Takes the markets in ig and returns a vec of strings of the market names
        // Output = ['BTC-PERP','ETH-PERP'...]
        match exchange {
            Some(e) => {
                // Filter only for markets for exchange
                self.markets
                    .iter()
                    .filter(|m| m.exchange_name == *e)
                    .map(|m| m.market_name.clone())
                    .collect()
            }
            None => {
                // No exchange filter, list all markets
                self.markets.iter().map(|m| m.market_name.clone()).collect()
            }
        }
    }

    pub async fn refresh_exchange(&self) {
        // Get user input for exchange to refresh
        let exchange: String = get_input("Enter Exchange to Refresh:");
        // Parse input to see if it is a valid exchange
        let exchange: ExchangeName = exchange.try_into().unwrap();
        // Get current exchanges from db
        let exchanges = select_exchanges(&self.ig_pool)
            .await
            .expect("Failed to fetch exchanges.");
        // Compare input to existing exchanges
        if !exchanges.iter().any(|e| e.name == exchange) {
            // Exchange not added
            println!("{:?} has not been added to El-Dorado.", exchange);
            return;
        }
        // Refresh markets for new exchange (should insert all)
        match exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.refresh_exchange_markets::<crate::exchanges::ftx::Market>(&exchange)
                    .await
            }

            ExchangeName::Gdax => {
                self.refresh_exchange_markets::<crate::exchanges::gdax::Product>(&exchange)
                    .await
            }
        };
    }

    pub async fn refresh_exchange_markets<T: crate::utilities::Market + DeserializeOwned>(
        &self,
        exchange: &ExchangeName,
    ) {
        // Get USD markets from exchange
        let markets: Vec<T> = get_usd_markets(&self.clients[exchange], exchange).await;
        // Get existing markets for exchange from db.
        let market_ids = select_market_ids_by_exchange(&self.ig_pool, exchange)
            .await
            .expect("Failed to get markets from db.");
        // For each market that is not in the exchange, insert into db.
        for market in markets.iter() {
            if !market_ids.iter().any(|m| m.market_name == market.name()) {
                // Exchange market not in database.
                println!("Adding {:?} market for {:?}", market.name(), exchange);
                insert_new_market(&self.ig_pool, exchange, market)
                    .await
                    .expect("Failed to insert market.");
            }
        }
    }

    pub async fn update_market_ranks(&self) {
        // Get user input for exchange to add
        let exchange: String = get_input("Enter Exchange to Rank:");
        // Parse input to see if there is a valid exchange
        let exchange: ExchangeName = exchange.try_into().unwrap();
        // Get current exchanges from db
        let exchanges = select_exchanges(&self.ig_pool)
            .await
            .expect("Failed to fetch exchanges.");
        // Compare input to existing exchanges
        if !exchanges.iter().any(|e| e.name == exchange) {
            // Exchange not added
            println!("{:?} has not been added to El-Dorado.", exchange);
            return;
        }
        match exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.update_market_ranks_generic::<crate::exchanges::ftx::Market>(&exchange)
                    .await
            }

            ExchangeName::Gdax => {
                // I dont think this works..no volume on get products to rank
                self.update_market_ranks_generic::<crate::exchanges::gdax::Product>(&exchange)
                    .await
            }
        };
    }

    pub async fn update_market_ranks_generic<
        T: crate::utilities::Market + DeserializeOwned + std::clone::Clone,
    >(
        &self,
        exchange: &ExchangeName,
    ) {
        // Get terminated markets from database
        let markets_terminated = select_market_details_by_status_exchange(
            &self.ig_pool,
            exchange,
            &MarketStatus::Terminated,
        )
        .await
        .expect("Failed to select terminated markets.");
        // Get market details from db TODO: derive market term from this list to reduce db calls
        let market_details = select_market_details(&self.ig_pool)
            .await
            .expect("Failed to select market details.");
        // Get USD markets from exchange
        let markets_exch: Vec<T> = get_usd_markets(&self.clients[exchange], exchange).await;
        println!("# exchange markets: {}", markets_exch.len());
        // Filter out non-terminated markets and non-perp markets
        let mut filtered_markets: Vec<T> = match *exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => markets_exch
                .iter()
                .filter(|m| {
                    m.market_type() == "future"
                        && !markets_terminated
                            .iter()
                            .any(|tm| tm.market_name == m.name())
                        && m.name().split('-').last() == Some("PERP")
                })
                .cloned()
                .collect(),
            ExchangeName::Gdax => markets_exch,
        };
        // println!("Filtered markets: {:?}", filtered_markets);
        // Sort by 24h volume
        filtered_markets.sort_by_key(|m2| Reverse(m2.usd_volume_24h()));
        // Create ranks table and select current contents
        create_market_ranks_table(&self.ig_pool, exchange)
            .await
            .expect("Failed to create market ranks table.");
        let previous_ranks = select_market_ranks(&self.ig_pool, exchange)
            .await
            .expect("Failed to select market ranks.");
        // Create empty vec to hold new ranks
        let mut new_ranks = Vec::new();
        let proposal = self.mita_proposal();
        // Set rank counter = 1
        let mut rank: i64 = 1;
        for market in filtered_markets.iter() {
            // Check if there is a previous record for market
            let previous_rank = previous_ranks
                .iter()
                .find(|pr| pr.market_name == market.name());
            let rank_prev = previous_rank.map(|pr| pr.rank);
            // Get MarketDetail for id and current mita fields
            let market_detail = market_details
                .iter()
                .find(|md| md.market_name == market.name())
                .unwrap();
            let (market_id, mita_current) = (market_detail.market_id, market_detail.mita.clone());
            let proposed_mita = proposal.get(&rank).map(|m| m.to_string());
            // If previous mita is none AND the proposal is not none - set proposal to mita-09 as
            // It will need time to catch up and should not be streamed immediately
            let mita_proposed = if mita_current == None && proposed_mita != None {
                Some("mita-09".to_string())
            } else {
                proposed_mita
            };
            let new_rank = MarketRank {
                market_id,
                market_name: market.name(),
                rank,
                rank_prev,
                mita_current,
                mita_proposed,
                usd_volume_24h: market.usd_volume_24h().unwrap().round(),
                usd_volume_15t: (market.usd_volume_24h().unwrap() / dec!(96)).round(),
                ats_v1: (market.usd_volume_24h().unwrap() / dec!(24) * dec!(0.05)).round_dp(2),
                ats_v2: (market.usd_volume_24h().unwrap() / dec!(96) * dec!(0.05)).round_dp(2),
                mps: (market.usd_volume_24h().unwrap() * dec!(0.005)).round_dp(2),
                dp_quantity: market.dp_quantity(),
                dp_price: market.dp_price(),
                min_quantity: market.min_quantity(),
            };
            new_ranks.push(new_rank);
            rank += 1;
        }
        // Drop market ranks table
        drop_market_ranks_table(&self.ig_pool, exchange)
            .await
            .expect("Failed to drop market ranks.");
        // Create market ranks table
        create_market_ranks_table(&self.ig_pool, exchange)
            .await
            .expect("Failed to create market ranks table.");
        // Insert markets
        for new_rank in new_ranks.iter() {
            // Insert rank
            insert_market_rank(&self.ig_pool, exchange, new_rank)
                .await
                .expect("Failed to insert market rank.");
        }
    }

    pub async fn update_market_mitas_from_ranks(&self) {
        // Get user input for exchange to add
        let exchange: String = get_input("Enter Exchange to Rank:");
        // Parse input to see if there is a valid exchange
        let exchange: ExchangeName = exchange.try_into().unwrap();
        // Get current exchanges from db
        let exchanges = select_exchanges(&self.ig_pool)
            .await
            .expect("Failed to fetch exchanges.");
        // Compare input to existing exchanges
        if !exchanges.iter().any(|e| e.name == exchange) {
            // Exchange not added
            println!("{:?} has not been added to El-Dorado.", exchange);
            return;
        }
        // Get market ranks for exchange
        let ranks = select_market_ranks(&self.ig_pool, &exchange)
            .await
            .expect("Failed to select market ranks.");
        // For each rank - update market mita column, update rank set current = proposed
        for rank in ranks.iter() {
            if rank.mita_current != rank.mita_proposed {
                // Update mita in markets table
                update_market_mita(&self.ig_pool, &rank.market_id, &rank.mita_proposed)
                    .await
                    .expect("Failed to update mita in markets.");
                // Update mita_current in market_ranks table
                update_market_ranks_current_mita(
                    &self.ig_pool,
                    &exchange,
                    &rank.mita_proposed,
                    &rank.market_id,
                )
                .await
                .expect("Failed to update mita in market ranks.");
            }
        }
    }

    fn mita_proposal(&self) -> HashMap<i64, String> {
        let mut proposal = HashMap::new();
        // Create map for proposed mitas: 1-48 in streams, 49-75 in daily catchups
        proposal.insert("mita-01", vec![1, 40, 41]);
        proposal.insert("mita-02", vec![2, 39, 42]);
        proposal.insert("mita-03", vec![3, 14, 15, 26, 27, 38, 43]);
        proposal.insert("mita-04", vec![4, 13, 16, 25, 28, 37, 44]);
        proposal.insert("mita-05", vec![5, 12, 17, 24, 29, 36, 45]);
        proposal.insert("mita-06", vec![6, 11, 18, 23, 30, 35, 46]);
        proposal.insert("mita-07", vec![7, 10, 19, 22, 31, 34, 47]);
        proposal.insert("mita-08", vec![8, 9, 20, 21, 32, 33, 48]);
        let mut mita_09 = Vec::new();
        for i in 49..75 {
            mita_09.push(i);
        }
        proposal.insert("mita-09", mita_09);
        // Create map for return proposal (k,v) = (1,"mita-01")
        let mut proposal_map = HashMap::new();
        for (k, v) in proposal.iter() {
            for i in v.iter() {
                proposal_map.insert(*i as i64, k.to_string());
            }
        }
        proposal_map
    }
}

pub async fn get_usd_markets<T: crate::utilities::Market + DeserializeOwned>(
    client: &RestClient,
    exchange: &ExchangeName,
) -> Vec<T> {
    let markets = match exchange {
        ExchangeName::FtxUs => get_ftx_usd_markets(client).await,
        ExchangeName::Ftx => get_ftx_usd_markets(client).await,
        ExchangeName::Gdax => get_gdax_usd_markets(client).await,
    };
    match markets {
        Ok(markets) => markets,
        Err(err) => panic!("Failed to fetch markets. RestError {:?}", err),
    }
}

pub async fn get_ftx_usd_markets<T: crate::utilities::Market + DeserializeOwned>(
    client: &RestClient,
) -> Result<Vec<T>, RestError> {
    // Get markets from exchange
    let mut markets = client.get_ftx_markets().await?;
    // Filter for USD based markets
    markets.retain(|m: &T| {
        m.quote_currency() == Some("USD".to_string()) || m.market_type() == *"future"
    });
    Ok(markets)
}

pub async fn get_gdax_usd_markets<T: crate::utilities::Market + DeserializeOwned>(
    client: &RestClient,
) -> Result<Vec<T>, RestError> {
    // Get markets from exchange
    let mut markets = client.get_gdax_products().await?;
    // Filter for USD based markets
    markets.retain(|m: &T| m.quote_currency() == Some("USD".to_string()));
    Ok(markets)
}

pub async fn create_market_ranks_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS market_ranks_{} (
            market_id UUID NOT NULL,
            market_name TEXT NOT NULL,
            rank BIGINT NOT NULL,
            rank_prev BIGINT,
            mita_current TEXT,
            mita_proposed TEXT,
            usd_volume_24h NUMERIC NOT NULL,
            usd_volume_15t NUMERIC NOT NULL,
            ats_v1 NUMERIC NOT NULL,
            ats_v2 NUMERIC NOT NULL,
            mps NUMERIC NOT NULL,
            dp_quantity INT NOT NULL,
            dp_price INT NOT NULL,
            min_quantity NUMERIC NOT NULL,
            PRIMARY KEY (market_id)
        )
        "#,
        exchange_name.as_str(),
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn drop_market_ranks_table(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        DROP TABLE IF EXISTS market_ranks_{}
        "#,
        exchange_name.as_str(),
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn select_market_ranks(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<Vec<MarketRank>, sqlx::Error> {
    let sql = format!(
        r#"
        SELECT *
        FROM market_ranks_{}
        "#,
        exchange_name.as_str(),
    );
    let rows = sqlx::query_as::<_, MarketRank>(&sql)
        .fetch_all(pool)
        .await?;
    Ok(rows)
}

pub async fn insert_market_rank(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    rank: &MarketRank,
) -> Result<(), sqlx::Error> {
    // Cannot use sqlx query! macro because table is dynamic and may not be created
    let sql = format!(
        r#"
        INSERT INTO market_ranks_{} (
            market_id, market_name, rank, rank_prev, mita_current, mita_proposed, usd_volume_24h,
            usd_volume_15t, ats_v1, ats_v2, mps, dp_quantity, dp_price, min_quantity)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (market_id) DO NOTHING
        "#,
        exchange_name.as_str(),
    );
    sqlx::query(&sql)
        .bind(rank.market_id)
        .bind(&rank.market_name)
        .bind(rank.rank)
        .bind(rank.rank_prev)
        .bind(&rank.mita_current)
        .bind(&rank.mita_proposed)
        .bind(rank.usd_volume_24h)
        .bind(rank.usd_volume_15t)
        .bind(rank.ats_v1)
        .bind(rank.ats_v2)
        .bind(rank.mps)
        .bind(rank.dp_quantity)
        .bind(rank.dp_price)
        .bind(rank.min_quantity)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_market_ids_by_exchange(
    pool: &PgPool,
    exchange: &ExchangeName,
) -> Result<Vec<MarketId>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketId,
        r#"
        SELECT market_id, market_name
        FROM markets
        WHERE exchange_name = $1
        "#,
        exchange.as_str()
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn select_market_detail_by_exchange_mita(
    pool: &PgPool,
    exchange: &ExchangeName,
    mita: &str,
) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
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

pub async fn select_market_details_by_status_exchange(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    status: &MarketStatus,
) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
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
        AND exchange_name = $2
        "#,
        status.as_str(),
        exchange_name.as_str(),
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn select_market_details(pool: &PgPool) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
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

pub async fn select_market_detail(
    pool: &PgPool,
    market: &MarketId,
) -> Result<MarketDetail, sqlx::Error> {
    let row = sqlx::query_as!(
        MarketDetail,
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
        market.market_id,
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

pub async fn select_markets_by_market_data_status(
    pool: &PgPool,
    market_status: &MarketStatus,
) -> Result<Vec<MarketDetail>, sqlx::Error> {
    let rows = sqlx::query_as!(
        MarketDetail,
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
        market_status.as_str()
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn insert_new_market<T: crate::utilities::Market + DeserializeOwned>(
    pool: &PgPool,
    exchange: &ExchangeName,
    market: &T,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO markets (
            market_id, exchange_name, market_name, market_type, base_currency,
            quote_currency, underlying, market_status, market_data_status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)    
        "#,
        Uuid::new_v4(),
        exchange.as_str(),
        market.name(),
        market.market_type(),
        market.base_currency(),
        market.quote_currency(),
        market.underlying(),
        MarketStatus::New.as_str(),
        MarketStatus::New.as_str(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_data_status(
    pool: &PgPool,
    market_id: &Uuid,
    status: &MarketStatus,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        UPDATE markets
        SET market_data_status  = $1
        WHERE market_id = $2
        "#,
        status.as_str(),
        market_id
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_mita(
    pool: &PgPool,
    market_id: &Uuid,
    mita: &Option<String>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE markets
        SET mita = $1
        WHERE market_id = $2
        "#,
    )
    .bind(mita)
    .bind(market_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn update_market_ranks_current_mita(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    mita: &Option<String>,
    market_id: &Uuid,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        UPDATE market_ranks_{}
        SET mita_current = $1
        WHERE market_id = $2
        "#,
        exchange_name.as_str()
    );
    sqlx::query(&sql)
        .bind(mita)
        .bind(market_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::configuration::*;
    use crate::exchanges::{select_exchanges, ExchangeName};
    use crate::inquisidor::Inquisidor;
    use crate::markets::{
        select_market_details_by_status_exchange, select_market_ids_by_exchange, MarketStatus,
    };
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

        // Get exchanges from database
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");
        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == configuration.application.exchange)
            .unwrap();

        // Get input from config for market to archive
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
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

    #[tokio::test]
    async fn select_active_markets_returns_active_markets() {
        let ig = Inquisidor::new().await;
        let markets = select_market_details_by_status_exchange(
            &ig.ig_pool,
            &ExchangeName::FtxUs,
            &MarketStatus::Active,
        )
        .await
        .expect("Failed to select markets.");
        println!("Acive markets: {:?}", markets);
    }

    #[tokio::test]
    async fn mita_proposal_creates_map() {
        let ig = Inquisidor::new().await;
        let mita_map = ig.mita_proposal();
        println!("Mita map: {:?}", mita_map);
    }
}
