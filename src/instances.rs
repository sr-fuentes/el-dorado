use crate::markets::{select_market_detail_by_exchange_mita, MarketDetail};
use crate::metrics::select_metrics_ap_by_exchange_market;
use crate::{exchanges::ExchangeName, inquisidor::Inquisidor, mita::Mita};
use chrono::{DateTime, Duration, DurationRound, Utc};
use sqlx::PgPool;
use std::convert::TryFrom;

#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct Instance {
    pub instance_type: InstanceType,
    pub droplet: String,
    pub exchange_name: Option<ExchangeName>,
    pub instance_status: InstanceStatus,
    pub restart: bool,
    pub last_restart_ts: Option<DateTime<Utc>>,
    pub restart_count: Option<i32>,
    pub num_markets: i32,
    pub last_update_ts: DateTime<Utc>,
}

impl Instance {
    pub fn time_since_last_update(&self) -> Duration {
        Utc::now() - self.last_update_ts
    }

    pub async fn inactive_markets(&self, pool: &PgPool, duration: Duration) -> Vec<MarketDetail> {
        let im = Vec::new();
        if self.instance_type == InstanceType::Ig {
            // Ig covers all market - return empty vec as this fn should be used for mita instances
            return im;
        };
        // Get markets for instance
        let markets = select_market_detail_by_exchange_mita(
            pool,
            &self.exchange_name.unwrap(),
            &self.droplet,
        )
        .await
        .expect("Failed to select market details by exchange/mita.");
        // Put market names into list
        let market_names = markets.iter().map(|m| m.market_name.clone()).collect();
        // Get metrics
        let metrics =
            select_metrics_ap_by_exchange_market(pool, &self.exchange_name.unwrap(), &market_names)
                .await
                .expect("Failed to select metrics ap");
        // Set last expected candle datetime
        let expected_ts = Utc::now().duration_trunc(duration).unwrap();
        for market in markets.iter() {
            // Check last metric versus expected timestamp
        }
        im
    }
}

#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
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

#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceStatus {
    New,
    Sync,
    Active,
    Restart,
    Paused,
    Terminated,
}

impl InstanceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceStatus::New => "new",
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
            "new" => Ok(Self::New),
            "sync" => Ok(Self::Sync),
            "active" => Ok(Self::Active),
            "restart" => Ok(Self::Restart),
            "paused" => Ok(Self::Paused),
            "terminated" => Ok(Self::Terminated),
            other => Err(format!("{} is not a supported instance status.", other)),
        }
    }
}

impl Mita {
    pub async fn insert_instance(&self) {
        insert_or_update_instance_mita(self)
            .await
            .expect("Failed to insert mita instance.");
    }

    pub async fn update_instance_status(&self, status: &InstanceStatus) {
        update_instance_status(
            &self.ed_pool,
            &self.settings.application.droplet,
            Some(&self.exchange.name),
            status,
        )
        .await
        .expect("Failed to update status.");
    }

    pub async fn update_instance_last(&self) {
        update_instance_last_updated(
            &self.ed_pool,
            &self.settings.application.droplet,
            Some(&self.exchange.name),
        )
        .await
        .expect("Failed to update last updated.");
    }
}

pub async fn insert_or_update_instance_mita(mita: &Mita) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO instances (
            instance_type, droplet, exchange_name, instance_status, restart, last_restart_ts,
            restart_count, num_markets, last_update_ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (droplet, exchange_name)
        DO
            UPDATE SET (instance_status, restart, last_restart_ts, restart_count, num_markets,
                last_update_ts) = (EXCLUDED.instance_status, EXCLUDED.restart,
                EXCLUDED.last_restart_ts, EXCLUDED.restart_count, EXCLUDED.num_markets,
                EXCLUDED.last_update_ts)
        "#;
    sqlx::query(sql)
        .bind(InstanceType::Mita.as_str())
        .bind(&mita.settings.application.droplet)
        .bind(mita.exchange.name.as_str())
        .bind(InstanceStatus::New.as_str())
        .bind(mita.restart)
        .bind(mita.last_restart)
        .bind(mita.restart_count as i32)
        .bind(mita.markets.len() as i32)
        .bind(Utc::now())
        .execute(&mita.ed_pool)
        .await?;
    Ok(())
}

pub async fn insert_or_update_instance_ig(
    pool: &PgPool,
    ig: &Inquisidor,
    n: i32,
) -> Result<(), sqlx::Error> {
    let sql = r#"
        INSERT INTO instances (
            instance_type, droplet, instance_status, restart, num_markets, last_update_ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (droplet)
        DO UPDATE
        SET (instance_status, num_markets, last_update_ts) = (EXCLUDED.instance_status,
            EXCLUDED.num_markets, EXCLUDED.last_update_ts)
        "#;
    sqlx::query(sql)
        .bind(InstanceType::Ig.as_str())
        .bind(&ig.settings.application.droplet)
        .bind(InstanceStatus::Sync.as_str())
        .bind(false)
        .bind(n)
        .bind(Utc::now())
        .execute(pool)
        .await?;
    Ok(())
}

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
            SET (instance_status, last_update_ts) = ($1, $2)
            WHERE droplet = $3 AND exchange_name = $4
            "#;
            sqlx::query(sql)
                .bind(status.as_str())
                .bind(Utc::now())
                .bind(droplet)
                .bind(e.as_str())
                .execute(pool)
                .await?;
        }
        None => {
            let sql = r#"
            UPDATE instances
            SET (instance_status, last_update_ts) = ($1, $2)
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
            SET last_update_ts = $1
            WHERE droplet = $2 AND exchange_name = $3
            "#;
            sqlx::query(sql)
                .bind(Utc::now())
                .bind(droplet)
                .bind(e.as_str())
                .execute(pool)
                .await?;
        }
        None => {
            let sql = r#"
            UPDATE instances
            SET last_update_ts = $1
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
            exchange_name as "exchange_name: Option<ExchangeName>",
            instance_status as "instance_status: InstanceStatus",
            restart, last_restart_ts, restart_count, num_markets, last_update_ts
        FROM instances
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use crate::instances::*;
    use crate::mita::Mita;
    use crate::utilities::get_input;

    #[tokio::test]
    pub async fn insert_mita_instance() {
        let mita = Mita::new().await;
        // Clear table
        let sql = r#"DELETE FROM instances WHERE 1=1"#;
        sqlx::query(sql)
            .execute(&mita.ed_pool)
            .await
            .expect("Failed to clear isntances table.");
        // Insert mita
        insert_or_update_instance_mita(&mita)
            .await
            .expect("Failed to insert/update mita.");
    }

    #[tokio::test]
    pub async fn insert_conflict_update_mita_instance() {
        let mita = Mita::new().await;
        // Clear table
        let sql = r#"DELETE FROM instances WHERE 1=1"#;
        sqlx::query(sql)
            .execute(&mita.ed_pool)
            .await
            .expect("Failed to clear isntances table.");
        // Insert mita
        insert_or_update_instance_mita(&mita)
            .await
            .expect("Failed to insert/update mita.");
        let _input: String = get_input("Press enter to continue: ");
        insert_or_update_instance_mita(&mita)
            .await
            .expect("Failed to insert/update mita.");
    }

    #[tokio::test]
    pub async fn update_mita_instance_status() {
        let mita = Mita::new().await;
        // Clear table
        let sql = r#"DELETE FROM instances WHERE 1=1"#;
        sqlx::query(sql)
            .execute(&mita.ed_pool)
            .await
            .expect("Failed to clear isntances table.");
        // Insert mita
        insert_or_update_instance_mita(&mita)
            .await
            .expect("Failed to insert/update mita.");
        update_instance_status(
            &mita.ed_pool,
            &mita.settings.application.droplet,
            Some(&mita.exchange.name),
            &InstanceStatus::Terminated,
        )
        .await
        .expect("Failed to update instance status.");
    }

    #[tokio::test]
    pub async fn update_mita_instance_last_update() {
        let mita = Mita::new().await;
        // Clear table
        let sql = r#"DELETE FROM instances WHERE 1=1"#;
        sqlx::query(sql)
            .execute(&mita.ed_pool)
            .await
            .expect("Failed to clear isntances table.");
        // Insert mita
        insert_or_update_instance_mita(&mita)
            .await
            .expect("Failed to insert/update mita.");
        update_instance_last_updated(
            &mita.ed_pool,
            &mita.settings.application.droplet,
            Some(&mita.exchange.name),
        )
        .await
        .expect("Failed to update last upadted.");
    }
}
