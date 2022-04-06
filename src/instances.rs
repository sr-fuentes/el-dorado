use crate::{exchanges::ExchangeName, inquisidor::Inquisidor, mita::Mita};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::convert::{TryFrom, TryInto};

pub struct Instance {
    pub instance_type: InstanceType,
    pub droplet: String,
    pub exchange_name: ExchangeName,
    pub instance_status: InstanceStatus,
    pub restart: bool,
    pub last_retart_ts: DateTime<Utc>,
    pub restart_count: i32,
    pub num_markets: i32,
    pub last_update_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceType {
    Mita,
    Ig,
}

impl InstanceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceType::Mita => "mita",
            InstanceType::Ig => "ig",
        }
    }
}

impl TryFrom<String> for InstanceType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "mita" => Ok(Self::Mita),
            "ig" => Ok(Self::Ig),
            other => Err(format!("{} is not a supported instance.", other)),
        }
    }
}

#[derive(Debug, Clone, Copy, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceStatus {
    Sync,
    Active,
    Restart,
    Paused,
    Terminated,
}

impl InstanceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceStatus::Sync => "sync",
            InstanceStatus::Active => "active",
            InstanceStatus::Restart => "restart",
            InstanceStatus::Paused => "paused",
            InstanceStatus::Terminated => "terminated",
        }
    }
}

impl TryFrom<String> for InstanceStatus {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "sync" => Ok(Self::Sync),
            "active" => Ok(Self::Active),
            "restart" => Ok(Self::Restart),
            "paused" => Ok(Self::Paused),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported instance status.", other)),
        }
    }
}

pub async fn insert_or_update_instance_mita(pool: &PgPool, mita: &Mita) {}

pub async fn insert_or_update_instance_ig(pool: &PgPool, ig: &Inquisidor) {}

pub async fn update_instance_status(
    pool: &PgPool,
    droplet: &str,
    exchange_name: Option<&ExchangeName>,
    status: &InstanceStatus,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        Some(e) => {
            let sql = r#"
            UPDATE instances
            SET (instance_status, last_updated_ts) = ($1, $2)
            WHERE droplet = $3 AND exchange_name = $4
            "#;
        sqlx::query(sql)
            .bind(status.as_str())
            .bind(Utc::now())
            .bind(droplet)
            .bind(e.as_str())
            .execute(pool)
            .await?;
        },
        None => {
            let sql = r#"
            UPDATE instances
            SET (instance_status, last_updated_ts) = ($1, $2)
            WHERE droplet = $3
            "#;
        sqlx::query(sql)
            .bind(status.as_str())
            .bind(Utc::now())
            .bind(droplet)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

pub async fn update_instance_last_updated(
    pool: &PgPool,
    droplet: &str,
    exchange_name: Option<&ExchangeName>,
) -> Result<(), sqlx::Error> {
    match exchange_name {
        Some(e) => {
            let sql = r#"
            UPDATE instances
            SET last_updated_ts = $1
            WHERE droplet = $2 AND exchange_name = $3
            "#;
        sqlx::query(sql)
            .bind(Utc::now())
            .bind(droplet)
            .bind(e.as_str())
            .execute(pool)
            .await?;
        },
        None => {
            let sql = r#"
            UPDATE instances
            SET last_updated_ts = $1
            WHERE droplet = $2
            "#;
        sqlx::query(sql)
            .bind(Utc::now())
            .bind(droplet)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

pub async fn select_instances(pool: &PgPool) -> Result<Vec<Instance>, sqlx::Error> {
    let rows = sqlx::query_as!(
        Instance,
        r#"
        SELECT instance_type as "instance_type: InstanceType",
            droplet,
            exchange_name as "exchange_name: ExchangeName",
            instance_status as "instance_status: InstanceStatus",
            restart, last_restart_ts, restart_count, num_markets, last_update_ts
        FROM instances
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
