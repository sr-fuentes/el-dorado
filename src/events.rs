use crate::candles::{select_candles_unvalidated_lt_datetime, validate_hb_candles, TimeFrame};
use crate::exchanges::ExchangeName;
use crate::inquisidor::Inquisidor;
use crate::markets::{select_market_details, MarketDetail};
use crate::mita::Mita;
use crate::trades::select_insert_delete_trades;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::convert::TryFrom;
use uuid::Uuid;

#[derive(Debug)]
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
    // CreateDailyCandles,
    // ValidateDailyCandles,
    // ArchiveDailyCandles,
    // PurgeMetrics,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::ProcessTrades => "processtrades",
            EventType::ValidateCandle => "validatecandle",
        }
    }
}

impl TryFrom<String> for EventType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "processtrades" => Ok(Self::ProcessTrades),
            "validatecandle" => Ok(Self::ValidateCandle),
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

impl Inquisidor {
    pub async fn process_events(&self) {
        // Get any open events for the droplet
        let open_events =
            select_open_events_for_droplet(&self.pool, &self.settings.application.droplet)
                .await
                .expect("Failed to select open events.");
        // Get all market details - for market id and strip name fn in event processing
        let markets = select_market_details(&self.pool)
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
                EventType::ProcessTrades => continue, // All trade processing done by mita
                EventType::ValidateCandle => {
                    self.process_event_validate_candle(event, market).await
                }
            }
        }
    }

    pub async fn process_event_validate_candle(&self, event: &Event, market: &MarketDetail) {
        // Get candles to validate based on event end date
        let unvalidated_candles = select_candles_unvalidated_lt_datetime(
            &self.pool,
            &event.exchange_name,
            &event.market_id,
            event.end_ts.unwrap(),
            TimeFrame::T15,
        )
        .await
        .expect("Failed to select candles.");
        if !unvalidated_candles.is_empty() {
            // Validate
            validate_hb_candles(
                &self.pool,
                &self.clients[&event.exchange_name],
                &event.exchange_name,
                market,
                &self.settings,
                &unvalidated_candles,
            )
            .await;
        }
        // Close event
        update_event_status_processed(&self.pool, event)
            .await
            .expect("Failed to update event status to done.");
    }
}

impl Mita {
    pub async fn process_events(&self) {
        // Get any open events for the droplet
        let open_events =
            select_open_events_for_droplet(&self.pool, &self.settings.application.droplet)
                .await
                .expect("Failed to select open events.");
        // Get all market details - for market id and strip name fn in event processing
        let markets = select_market_details(&self.pool)
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
            }
        }
    }

    pub async fn process_event_process_trades(&self, event: &Event, market: &MarketDetail) {
        // Select insert delete the trades from ws to processed
        select_insert_delete_trades(
            &self.pool,
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
        update_event_status_processed(&self.pool, event)
            .await
            .expect("Failed to update event status to done.");
        // Add validation event
        insert_event_validated_candles(&self.pool, "ig", event.start_ts.unwrap(), market)
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

pub async fn insert_event_validated_candles(
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
        .bind(event_time)
        .bind(event_time + Duration::seconds(15))
        .bind(EventStatus::New.as_str())
        .bind("Validated candles.")
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
