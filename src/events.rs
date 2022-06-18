use crate::candles::{
    create_01d_candles, select_candles_unvalidated_lt_datetime, validate_01d_candles,
    validate_hb_candles,
};
use crate::exchanges::{select_exchanges_by_status, ExchangeName, ExchangeStatus};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    select_market_details, select_market_details_by_status_exchange, MarketDataStatus,
    MarketDetail, MarketStatus, MarketTradeDetail,
};
use crate::mita::Mita;
use crate::trades::select_insert_delete_trades;
use crate::utilities::TimeFrame;
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Event {
    pub event_id: Uuid,
    pub droplet: String,
    pub event_type: EventType,
    pub exchange_name: ExchangeName,
    pub market_id: Uuid,
    pub start_ts: Option<DateTime<Utc>>,
    pub end_ts: Option<DateTime<Utc>>,
    pub event_ts: DateTime<Utc>,
    pub created_ts: DateTime<Utc>,
    pub processed_ts: Option<DateTime<Utc>>,
    pub event_status: EventStatus,
    pub notes: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum EventType {
    ProcessTrades,
    ValidateCandle,
    CreateDailyCandles,
    ValidateDailyCandles,
    ArchiveDailyCandles,
    BackfillTrades,
    // PurgeMetrics,
    // WeeklyClean,
    // DeepClean,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::ProcessTrades => "processtrades",
            EventType::ValidateCandle => "validatecandle",
            EventType::CreateDailyCandles => "createdailycandles",
            EventType::ValidateDailyCandles => "validatedailycandles",
            EventType::ArchiveDailyCandles => "archivedailycandles",
            EventType::BackfillTrades => "backfilltrades",
        }
    }
}

impl TryFrom<String> for EventType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "processtrades" => Ok(Self::ProcessTrades),
            "validatecandle" => Ok(Self::ValidateCandle),
            "createdailycandles" => Ok(Self::CreateDailyCandles),
            "validatedailycandles" => Ok(Self::ValidateDailyCandles),
            "archivedailycandles" => Ok(Self::ArchiveDailyCandles),
            "backfilltrades" => Ok(Self::BackfillTrades),
            other => Err(format!("{} is not a supported validation type.", other)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum EventStatus {
    New,
    Open,
    Done,
}

impl EventStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventStatus::New => "new",
            EventStatus::Open => "open",
            EventStatus::Done => "done",
        }
    }
}

impl TryFrom<String> for EventStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "new" => Ok(Self::New),
            "open" => Ok(Self::Open),
            "done" => Ok(Self::Done),
            other => Err(format!("{} is not a supported validation status.", other)),
        }
    }
}

impl Event {
    pub fn new_backfill_trades(mtd: &MarketTradeDetail, exchange: &ExchangeName) -> Option<Self> {
        // Create a new backfill event
        // Logic for determining action based on mtd (market trade detail)
        // At this point we are only interested in getting trades prior to active el-d candles
        // As those trades have already be retrieved and processed, possibly validated and archived.
        // First determine if market trade detail is backfilled completely - Prev_Status = Complete
        // If so, there is no need to continue - review for any backfill events for this market and
        // Delete them. If there is a need to continue - create an event with the market and date
        // For an FTX market (there is no spare mita - so downloading will be IG event) or start the
        // Backfill process if it is a GDAX market.
        match mtd.previous_status {
            MarketDataStatus::Completed => {
                // Add logic to look at next_status
                println!("Backfill completed, next status functions not yet implemented.");
                None
            }
            MarketDataStatus::Get | MarketDataStatus::Archive | MarketDataStatus::Validate => {
                //  Add event with prev_date
                // TODO: add logic to look forward if prev status is complete
                return Some(Self {
                    event_id: Uuid::new_v4(),
                    droplet: "ig".to_string(),
                    event_type: EventType::BackfillTrades,
                    exchange_name: *exchange,
                    market_id: mtd.market_id,
                    start_ts: Some(mtd.previous_trade_day),
                    end_ts: None,
                    event_ts: Utc::now(),
                    created_ts: Utc::now(),
                    processed_ts: None,
                    event_status: EventStatus::New,
                    notes: Some(mtd.previous_status.as_str().to_string()),
                });
            }
        }
    }

    pub async fn insert(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO events (event_id, droplet, event_type, exchange_name, market_id,
                start_ts, end_ts, event_ts, created_ts, processed_ts, event_status, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
            self.event_id,
            self.droplet,
            self.event_type.as_str(),
            self.exchange_name.as_str(),
            self.market_id,
            self.start_ts,
            self.end_ts,
            self.event_ts,
            self.created_ts,
            self.processed_ts,
            self.event_status.as_str(),
            self.notes,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_status(
        &self,
        pool: &PgPool,
        status: &EventStatus,
    ) -> Result<(), sqlx::Error> {
        match status {
            EventStatus::New | EventStatus::Open => {
                sqlx::query!(
                    r#"
                    UPDATE events
                    SET event_status = $1
                    WHERE event_id = $2
                    "#,
                    status.as_str(),
                    self.event_id,
                )
                .execute(pool)
                .await?;
            }
            EventStatus::Done => {
                sqlx::query!(
                    r#"
                    UPDATE events
                    SET (event_status, processed_ts) = ($1, $2)
                    WHERE event_id = $3
                    "#,
                    status.as_str(),
                    Utc::now(),
                    self.event_id,
                )
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn select_by_status_type(
        pool: &PgPool,
        event_status: &EventStatus,
        event_type: &EventType,
    ) -> Result<Vec<Event>, sqlx::Error> {
        let rows = sqlx::query_as!(
            Event,
            r#"
            SELECT event_id, droplet,
                event_type as "event_type: EventType",
                exchange_name as "exchange_name: ExchangeName",
                market_id, start_ts, end_ts, event_ts, created_ts, processed_ts,
                event_status as "event_status: EventStatus", 
                notes
            FROM events
            WHERE event_status = $1
            AND event_type = $2
            "#,
            event_status.as_str(),
            event_type.as_str(),
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }

    pub async fn select_by_statuses_type(
        pool: &PgPool,
        event_statuses: &[EventStatus],
        event_type: &EventType,
    ) -> Result<Vec<Event>, sqlx::Error> {
        let statuses: Vec<String> = event_statuses
            .iter()
            .map(|s| s.as_str().to_string())
            .collect();
        let rows = sqlx::query_as!(
            Event,
            r#"
            SELECT event_id, droplet,
                event_type as "event_type: EventType",
                exchange_name as "exchange_name: ExchangeName",
                market_id, start_ts, end_ts, event_ts, created_ts, processed_ts,
                event_status as "event_status: EventStatus", 
                notes
            FROM events
            WHERE event_status = ANY($1)
            AND event_type = $2
            "#,
            &statuses,
            event_type.as_str(),
        )
        .fetch_all(pool)
        .await?;
        Ok(rows)
    }
}

impl Inquisidor {
    pub async fn set_initial_event(&self) {
        // Get all active exchanges
        let exchanges = select_exchanges_by_status(&self.ig_pool, ExchangeStatus::Active)
            .await
            .expect("Failed to select active exchanges.");
        // Get all open events
        let events = select_open_events_for_droplet(&self.ig_pool, "ig")
            .await
            .expect("Failed to select events.");
        // For each exchange, check to see if there is a create daily candle event
        for exchange in exchanges.iter() {
            match events.iter().find(|e| {
                e.event_type == EventType::CreateDailyCandles && e.exchange_name == exchange.name
            }) {
                Some(_) => continue, // Event exists, nothing to do
                None => {
                    insert_event_create_daily_candles(&self.ig_pool, &exchange.name, Utc::now())
                        .await
                        .expect("Failed to insert create daily candles event.")
                }
            }
        }
    }

    pub async fn process_events(&self) {
        // Get any open events for the droplet
        let open_events =
            select_open_events_for_droplet(&self.ig_pool, &self.settings.application.droplet)
                .await
                .expect("Failed to select open events.");
        // Get all market details - for market id and strip name fn in event processing
        let markets = select_market_details(&self.ig_pool)
            .await
            .expect("Failed to select all market details.");
        // Process events
        for event in open_events.iter() {
            // Get market detail for event
            let market = markets.iter().find(|m| m.market_id == event.market_id);
            match event.event_type {
                EventType::ProcessTrades => continue, // All trade processing done by mita
                EventType::ValidateCandle => {
                    self.process_event_validate_candle(event, market.unwrap())
                        .await
                }
                EventType::CreateDailyCandles => {
                    self.process_event_create_daily_candles(event).await
                }
                EventType::ValidateDailyCandles => {
                    self.process_event_validate_daily_candles(event).await
                }
                EventType::ArchiveDailyCandles => {
                    self.process_event_archive_daily_candles(event).await
                }
                EventType::BackfillTrades => self.process_event_backfill_trades(event).await,
            }
        }
    }

    pub async fn process_event_validate_candle(&self, event: &Event, market: &MarketDetail) {
        // Get candles to validate based on event end date
        let unvalidated_candles = select_candles_unvalidated_lt_datetime(
            &self.ig_pool,
            &event.exchange_name,
            &event.market_id,
            event.end_ts.unwrap(),
            TimeFrame::T15,
        )
        .await
        .expect("Failed to select candles.");
        if !unvalidated_candles.is_empty() {
            // Validate
            match event.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    validate_hb_candles::<crate::exchanges::ftx::Candle>(
                        &self.ig_pool,
                        &self.ftx_pool,
                        &self.clients[&event.exchange_name],
                        &event.exchange_name,
                        market,
                        &unvalidated_candles,
                    )
                    .await;
                }
                ExchangeName::Gdax => {
                    validate_hb_candles::<crate::exchanges::gdax::Candle>(
                        &self.ig_pool,
                        &self.gdax_pool,
                        &self.clients[&event.exchange_name],
                        &event.exchange_name,
                        market,
                        &unvalidated_candles,
                    )
                    .await;
                }
            };
        };
        // Close event
        update_event_status_processed(&self.ig_pool, event)
            .await
            .expect("Failed to update event status to done.");
    }

    pub async fn process_event_create_daily_candles(&self, event: &Event) {
        // Select active market from each exchange
        let markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &event.exchange_name,
            &MarketStatus::Active,
        )
        .await
        .expect("Failed to select markets.");
        for market in markets.iter() {
            // Create 01d candles if needed
            create_01d_candles(&self.ig_pool, &event.exchange_name, &market.market_id).await;
        }
        // Create event for next day
        let next_event_ts = Utc::now().duration_trunc(TimeFrame::D01.as_dur()).unwrap()
            + Duration::days(1)
            + Duration::minutes(2);
        insert_event_create_daily_candles(&self.ig_pool, &event.exchange_name, next_event_ts)
            .await
            .expect("Failed insert create daily candle event.");
        // Create validation event
        insert_event_validate_daily_candles(&self.ig_pool, &event.exchange_name)
            .await
            .expect("Frailed to insert validated daily candles.");
        // Close event
        update_event_status_processed(&self.ig_pool, event)
            .await
            .expect("Failed to update event status to done.");
    }

    pub async fn process_event_validate_daily_candles(&self, event: &Event) {
        // Select active market from each exchange
        let markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &event.exchange_name,
            &MarketStatus::Active,
        )
        .await
        .expect("Failed to select markets.");
        for market in markets.iter() {
            // Create 01d candles if needed
            match event.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    validate_01d_candles::<crate::exchanges::ftx::Candle>(
                        &self.ig_pool,
                        &self.ftx_pool,
                        &self.clients[&event.exchange_name],
                        &event.exchange_name,
                        market,
                    )
                    .await;
                }
                ExchangeName::Gdax => {
                    validate_01d_candles::<crate::exchanges::gdax::Candle>(
                        &self.ig_pool,
                        &self.gdax_pool,
                        &self.clients[&event.exchange_name],
                        &event.exchange_name,
                        market,
                    )
                    .await;
                }
            };
        }
        // Close event
        update_event_status_processed(&self.ig_pool, event)
            .await
            .expect("Failed to update event status to done.");
        // Create archive event
        insert_event_archive_daily_candles(&self.ig_pool, &event.exchange_name)
            .await
            .expect("Failed to insert archive daily candles event.");
    }

    pub async fn process_event_archive_daily_candles(&self, event: &Event) {
        // Select active market from each exchange
        let markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &event.exchange_name,
            &MarketStatus::Active,
        )
        .await
        .expect("Failed to select markets.");
        for market in markets.iter() {
            // Temporarily remove GDAX from archiving
            if market.exchange_name != ExchangeName::Gdax {
                self.archive_validated_trades_for_market(market).await;
            };
        }
        // Close event
        update_event_status_processed(&self.ig_pool, event)
            .await
            .expect("Failed to update event status to done.");
    }
}

impl Mita {
    pub async fn process_events(&self, events_ts: DateTime<Utc>) -> bool {
        // Check time versus mita event_ts, if at least 1 minute has not elapsed, return
        let now = Utc::now();
        if now - events_ts < Duration::minutes(1) {
            return false;
        };
        // Get any open events for the droplet
        let open_events = select_open_events_for_droplet_exchange(
            &self.ed_pool,
            &self.exchange.name,
            &self.settings.application.droplet,
        )
        .await
        .expect("Failed to select open events.");
        // Get all market details - for market id and strip name fn in event processing
        let markets = select_market_details(&self.ed_pool)
            .await
            .expect("Failed to select all market details.");
        // Process events
        for event in open_events.iter() {
            // Get market detail for event
            let market = markets
                .iter()
                .find(|m| m.market_id == event.market_id)
                .unwrap();
            match event.event_type {
                EventType::ProcessTrades => self.process_event_process_trades(event, market).await,
                EventType::ValidateCandle => continue, // All validations will be for IG
                EventType::CreateDailyCandles => continue, // All daily creation events will be IG
                EventType::ValidateDailyCandles => continue, // IG only
                EventType::ArchiveDailyCandles => continue, // IG only
                EventType::BackfillTrades => continue, // IG only
            }
        }
        true
    }

    pub async fn process_event_process_trades(&self, event: &Event, market: &MarketDetail) {
        // Select insert delete the trades from ws to processed
        select_insert_delete_trades(
            &self.trade_pool,
            &event.exchange_name,
            market,
            event.start_ts.unwrap(),
            event.end_ts.unwrap(),
            "ws",
            "processed",
        )
        .await
        .expect("Failed to select insert delete trades.");
        // Update event status to done
        update_event_status_processed(&self.ed_pool, event)
            .await
            .expect("Failed to update event status to done.");
        // Add validation event
        insert_event_validate_candles(&self.ed_pool, "ig", event.start_ts.unwrap(), market)
            .await
            .expect("Failed in insert event - validate candle.");
    }
}

pub async fn insert_event_process_trades(
    pool: &PgPool,
    droplet: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    market: &MarketDetail,
) -> Result<(), sqlx::Error> {
    let event_time = Utc::now();
    let sql = r#"
        INSERT INTO events (
            event_id, droplet, event_type, exchange_name, market_id, start_ts, end_ts, event_ts, created_ts,
            event_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind(droplet)
        .bind(EventType::ProcessTrades.as_str())
        .bind(market.exchange_name.as_str())
        .bind(market.market_id)
        .bind(start)
        .bind(end)
        .bind(event_time)
        .bind(event_time)
        .bind(EventStatus::New.as_str())
        .bind("Process trades for new candle.")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_event_validate_candles(
    pool: &PgPool,
    droplet: &str,
    end: DateTime<Utc>,
    market: &MarketDetail,
) -> Result<(), sqlx::Error> {
    let event_time = Utc::now();
    let sql = r#"
        INSERT INTO events (
            event_id, droplet, event_type, exchange_name, market_id, end_ts, event_ts, created_ts,
            event_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind(droplet)
        .bind(EventType::ValidateCandle.as_str())
        .bind(market.exchange_name.as_str())
        .bind(market.market_id)
        .bind(end)
        .bind(event_time + Duration::seconds(15))
        .bind(event_time)
        .bind(EventStatus::New.as_str())
        .bind("Validate candles.")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_event_create_daily_candles(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    event_ts: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let event_time = Utc::now();
    let sql = r#"
        INSERT INTO events (
            event_id, droplet, event_type, exchange_name, market_id, event_ts, created_ts,
            event_status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind("ig")
        .bind(EventType::CreateDailyCandles.as_str())
        .bind(exchange_name.as_str())
        .bind(Uuid::new_v4())
        .bind(event_ts)
        .bind(event_time)
        .bind(EventStatus::New.as_str())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_event_validate_daily_candles(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<(), sqlx::Error> {
    let event_time = Utc::now();
    let sql = r#"
        INSERT INTO events (
            event_id, droplet, event_type, exchange_name, market_id, event_ts, created_ts,
            event_status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind("ig")
        .bind(EventType::ValidateDailyCandles.as_str())
        .bind(exchange_name.as_str())
        .bind(Uuid::new_v4())
        .bind(event_time)
        .bind(event_time)
        .bind(EventStatus::New.as_str())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn insert_event_archive_daily_candles(
    pool: &PgPool,
    exchange_name: &ExchangeName,
) -> Result<(), sqlx::Error> {
    let event_time = Utc::now();
    let sql = r#"
        INSERT INTO events (
            event_id, droplet, event_type, exchange_name, market_id, event_ts, created_ts,
            event_status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind("ig")
        .bind(EventType::ArchiveDailyCandles.as_str())
        .bind(exchange_name.as_str())
        .bind(Uuid::new_v4())
        .bind(event_time)
        .bind(event_time)
        .bind(EventStatus::New.as_str())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn select_open_events_for_droplet(
    pool: &PgPool,
    droplet: &str,
) -> Result<Vec<Event>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Event,
        r#"
        SELECT event_id, droplet,
            event_type as "event_type: EventType",
            exchange_name as "exchange_name: ExchangeName",
            market_id, start_ts, end_ts, event_ts, created_ts, processed_ts,
            event_status as "event_status: EventStatus", 
            notes
        FROM events
        WHERE droplet = $1
        AND event_ts < $2
        AND event_status != $3
        "#,
        droplet,
        Utc::now(),
        EventStatus::Done.as_str(),
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn select_open_events_for_droplet_exchange(
    pool: &PgPool,
    exchange_name: &ExchangeName,
    droplet: &str,
) -> Result<Vec<Event>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Event,
        r#"
        SELECT event_id, droplet,
            event_type as "event_type: EventType",
            exchange_name as "exchange_name: ExchangeName",
            market_id, start_ts, end_ts, event_ts, created_ts, processed_ts,
            event_status as "event_status: EventStatus", 
            notes
        FROM events
        WHERE droplet = $1
        AND event_ts < $2
        AND event_status != $3
        AND exchange_name = $4
        "#,
        droplet,
        Utc::now(),
        EventStatus::Done.as_str(),
        exchange_name.as_str(),
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn update_event_status_processed(
    pool: &PgPool,
    event: &Event,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        UPDATE events
        SET (processed_ts, event_status) = ($1, $2)
        WHERE event_id = $3
        "#;
    sqlx::query(sql)
        .bind(Utc::now())
        .bind(EventStatus::Done.as_str())
        .bind(event.event_id)
        .execute(pool)
        .await?;
    Ok(())
}