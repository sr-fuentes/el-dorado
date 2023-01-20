use crate::exchanges::ExchangeName;
use chrono::{DateTime, Utc};
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
    ForwardFillTrades,
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
            EventType::ForwardFillTrades => "forwardfilltrades",
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
            "forwardfilltrades" => Ok(Self::ForwardFillTrades),
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
