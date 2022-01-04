use crate::exchanges::ExchangeName;
use crate::markets::MarketDetail;
use crate::mita::Mita;
use chrono::{DateTime, Utc};
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
    pub processed_ts: Option<Utc>,
    pub event_status: EventStatus,
    pub notes: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum EventType {
    ProcessTrades,
    // ValidateCandle,
    // CreateDailyCandles,
    // ValidateDailyCandles,
    // ArchiveDailyCandles,
    // PurgeMetrics,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::ProcessTrades => "processtrades",
        }
    }
}

impl TryFrom<String> for EventType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "processtrades" => Ok(Self::ProcessTrades),
            other => Err(format!("{} is not a supported validation type.", other)),
        }
    }
}

#[derive(Debug)]
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

impl Mita {
    pub async fn process_events(&self) {
        // Get any open events for the droplet
        let open_events =
            select_open_events_for_droplet(&self.pool, &self.settings.application.droplet)
                .await
                .expect("Failed to select open events.");
        // Process events
        for event in open_events.iter() {
            match event.event_type {
                EventType::ProcessTrades => self.process_event_process_trades(event).await,
            }
        }
    }

    pub async fn process_event_process_trades(&self, event: &Event) {}
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
            event_id, droplet, exchange_name, market_id, start_ts, end_ts, event_ts, created_ts,
            event_status, notes)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#;
    sqlx::query(sql)
        .bind(Uuid::new_v4())
        .bind(droplet)
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
            event_status as "event_status: EventStatus"
        FROM events
        WHERE droplet = $1
        AND event_ts > $2
        AND event_status != 'done'
        "#,
        droplet,
        Utc::now(),
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
