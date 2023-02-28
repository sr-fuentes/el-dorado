use crate::configuration::Settings;
use crate::exchanges::ExchangeName;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::convert::{TryFrom, TryInto};

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
    pub last_message_ts: Option<DateTime<Utc>>,
}

impl Instance {
    pub async fn initialize(pool: &PgPool, settings: &Settings) -> Self {
        let instance_type = settings
            .application
            .instance_type
            .clone()
            .try_into()
            .expect("Failed to parse instance type.");
        let droplet = settings.application.droplet.clone();
        let exchange_name: Option<ExchangeName> = match instance_type {
            InstanceType::Ig => None,
            InstanceType::Mita => Some(
                settings
                    .application
                    .exchange
                    .clone()
                    .try_into()
                    .expect("Failed to parse exchange from settings."),
            ),
            InstanceType::Conqui => None,
        };
        // Check the database for an entry for the instance to get the last restart and message info
        let instance = match instance_type {
            InstanceType::Ig => Self::select_ig(pool).await,
            InstanceType::Mita => Self::select_mita(pool, &exchange_name.unwrap(), &droplet).await,
            InstanceType::Conqui => Self::select_conqui(pool).await,
        };
        let (last_restart_ts, restart_count, last_message_ts) = match instance {
            Ok(i) => (i.last_restart_ts, i.restart_count, i.last_message_ts),
            Err(sqlx::Error::RowNotFound) => (None, Some(0), None),
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };
        Self {
            instance_type,
            droplet,
            exchange_name,
            instance_status: InstanceStatus::New,
            restart: true,
            last_restart_ts,
            restart_count,
            num_markets: 0,
            last_update_ts: Utc::now(),
            last_message_ts,
        }
    }

    async fn select_ig(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Instance,
            r#"
            SELECT instance_type as "instance_type: InstanceType",
                droplet,
                exchange_name as "exchange_name: Option<ExchangeName>",
                instance_status as "instance_status: InstanceStatus",
                restart, last_restart_ts, restart_count, num_markets, last_update_ts,
                last_message_ts
            FROM instances
            WHERE instance_type = $1
            "#,
            InstanceType::Ig.as_str()
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    async fn select_conqui(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Instance,
            r#"
            SELECT instance_type as "instance_type: InstanceType",
                droplet,
                exchange_name as "exchange_name: Option<ExchangeName>",
                instance_status as "instance_status: InstanceStatus",
                restart, last_restart_ts, restart_count, num_markets, last_update_ts,
                last_message_ts
            FROM instances
            WHERE instance_type = $1
            "#,
            InstanceType::Conqui.as_str()
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    async fn select_mita(
        pool: &PgPool,
        exchange: &ExchangeName,
        droplet: &str,
    ) -> Result<Self, sqlx::Error> {
        let row = sqlx::query_as!(
            Instance,
            r#"
            SELECT instance_type as "instance_type: InstanceType",
                droplet,
                exchange_name as "exchange_name: Option<ExchangeName>",
                instance_status as "instance_status: InstanceStatus",
                restart, last_restart_ts, restart_count, num_markets, last_update_ts,
                last_message_ts
            FROM instances
            WHERE instance_type = $1
            AND exchange_name = $2
            AND droplet = $3
            "#,
            InstanceType::Mita.as_str(),
            exchange.as_str(),
            droplet,
        )
        .fetch_one(pool)
        .await?;
        Ok(row)
    }

    pub fn time_since_last_update(&self) -> Duration {
        Utc::now() - self.last_update_ts
    }

    pub fn time_since_last_message(&self) -> Option<Duration> {
        self.last_message_ts.map(|ts| Utc::now() - ts)
    }

    // pub async fn inactive_markets(
    //     &self,
    //     pool: &PgPool,
    //     duration: Duration,
    // ) -> Vec<(MarketDetail, String)> {
    //     let mut im = Vec::new();
    //     if self.instance_type == InstanceType::Ig {
    //         // Ig covers all market - return empty vec as this fn should be used for mita instances
    //         return im;
    //     };
    //     // Get markets for instance
    //     let markets = MarketDetail::select_by_exchange_mita(
    //         pool,
    //         &self.exchange_name.unwrap(),
    //         &self.droplet,
    //     )
    //     .await
    //     .expect("Failed to select market details by exchange/mita.");
    //     // Put market names into list
    //     let market_names: Vec<String> = markets.iter().map(|m| m.market_name.clone()).collect();
    //     // let market_names_str: Vec<&str> = markets.iter().map(|m| m.market_name.as_ref()).collect();
    //     // println!("Market names str: {:?}", market_names);
    //     // Get metrics and sort descending as filter in later step takes first match
    //     let mut metrics =
    //         select_metrics_ap_by_exchange_market(pool, &self.exchange_name.unwrap(), &market_names)
    //             .await
    //             .expect("Failed to select metrics ap");
    //     metrics.sort_by(|m1, m2| m2.datetime.cmp(&m1.datetime));
    //     // Set last expected candle datetime. Current time truncated to time frame. Then subtract
    //     // one duration to get to the start of the last expected metric.
    //     // Example:
    //     //      Time Now: 12:07
    //     //      Duration: 15 minutes
    //     //      12:07 truncated by 15 minutes = 12:00 - 15 minutes = 11:45
    //     //      Expected last metric datetime = 11:45 (11:45-12:00 interval)
    //     let expected_ts = Utc::now().duration_trunc(duration).unwrap() - duration;
    //     for market in markets.iter() {
    //         // Check last metric versus expected timestamp
    //         let last_metric = metrics.iter().find(|m| {
    //             m.exchange_name == self.exchange_name.unwrap()
    //                 && m.market_name == market.market_name
    //                 && m.time_frame == TimeFrame::T15
    //                 && m.lbp == 8640
    //         });
    //         match last_metric {
    //             Some(lm) => {
    //                 // Check if the latest metric matches expected time. Add to vec with reason if
    //                 // if does not = Stale Metric
    //                 if lm.datetime != expected_ts {
    //                     im.push((market.clone(), "Stale Metric".to_string()));
    //                     println!(
    //                         "{:?} has stale metric. Last {:?} versus expected {:?}",
    //                         market.market_name, lm.datetime, expected_ts
    //                     );
    //                 } else {
    //                     // println!("{:?} metrics are current.", market.market_name);
    //                 }
    //             }
    //             None => {
    //                 // Add to vec with reason = No Metric Found
    //                 im.push((market.clone(), "No Metric Found".to_string()));
    //                 println!("{:?} had no metric found.", market.market_name);
    //             }
    //         }
    //     }
    //     im
    // }

    pub async fn update_last_message_ts(&self, pool: &PgPool) -> Result<(), sqlx::Error> {
        match &self.exchange_name {
            Some(e) => {
                let sql = r#"
                UPDATE instances
                SET last_message_ts = $1
                WHERE droplet = $2 AND exchange_name = $3
                "#;
                sqlx::query(sql)
                    .bind(Utc::now())
                    .bind(&self.droplet)
                    .bind(e.as_str())
                    .execute(pool)
                    .await?;
            }
            None => {
                let sql = r#"
                UPDATE instances
                SET last_message_ts = $1
                WHERE droplet = $2
                "#;
                sqlx::query(sql)
                    .bind(Utc::now())
                    .bind(&self.droplet)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(rename_all = "lowercase")]
pub enum InstanceType {
    Mita,
    Ig,
    Conqui,
}

impl InstanceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InstanceType::Mita => "mita",
            InstanceType::Ig => "ig",
            InstanceType::Conqui => "conqui",
        }
    }
}

impl TryFrom<String> for InstanceType {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "mita" => Ok(Self::Mita),
            "ig" => Ok(Self::Ig),
            "conqui" => Ok(Self::Conqui),
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

// pub async fn insert_or_update_instance_mita(mita: &Mita) -> Result<(), sqlx::Error> {
//     let sql = r#"
//         INSERT INTO instances (
//             instance_type, droplet, exchange_name, instance_status, restart, last_restart_ts,
//             restart_count, num_markets, last_update_ts, last_message_ts)
//         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)
//         ON CONFLICT (droplet, exchange_name)
//         DO
//             UPDATE SET (instance_status, restart, last_restart_ts, restart_count, num_markets,
//                 last_update_ts, last_message_ts) = (EXCLUDED.instance_status, EXCLUDED.restart,
//                 EXCLUDED.last_restart_ts, EXCLUDED.restart_count, EXCLUDED.num_markets,
//                 EXCLUDED.last_update_ts, EXCLUDED.last_message_ts)
//         "#;
//     sqlx::query(sql)
//         .bind(InstanceType::Mita.as_str())
//         .bind(&mita.settings.application.droplet)
//         .bind(mita.exchange.name.as_str())
//         .bind(InstanceStatus::New.as_str())
//         .bind(mita.restart)
//         .bind(mita.last_restart)
//         .bind(mita.restart_count as i32)
//         .bind(mita.markets.len() as i32)
//         .bind(Utc::now())
//         .execute(&mita.ed_pool)
//         .await?;
//     Ok(())
// }

// pub async fn insert_or_update_instance_ig(
//     pool: &PgPool,
//     ig: &Inquisidor,
//     n: i32,
// ) -> Result<(), sqlx::Error> {
//     let sql = r#"
//         INSERT INTO instances (
//             instance_type, droplet, instance_status, restart, num_markets, last_update_ts)
//         VALUES ($1, $2, $3, $4, $5, $6, $7)
//         ON CONFLICT (droplet)
//         DO UPDATE
//         SET (instance_status, num_markets, last_update_ts) = (EXCLUDED.instance_status,
//             EXCLUDED.num_markets, EXCLUDED.last_update_ts)
//         "#;
//     sqlx::query(sql)
//         .bind(InstanceType::Ig.as_str())
//         .bind(&ig.settings.application.droplet)
//         .bind(InstanceStatus::Sync.as_str())
//         .bind(false)
//         .bind(n)
//         .bind(Utc::now())
//         .execute(pool)
//         .await?;
//     Ok(())
// }

// pub async fn update_instance_status(
//     pool: &PgPool,
//     droplet: &str,
//     exchange_name: Option<&ExchangeName>,
//     status: &InstanceStatus,
// ) -> Result<(), sqlx::Error> {
//     match exchange_name {
//         Some(e) => {
//             let sql = r#"
//             UPDATE instances
//             SET (instance_status, last_update_ts) = ($1, $2)
//             WHERE droplet = $3 AND exchange_name = $4
//             "#;
//             sqlx::query(sql)
//                 .bind(status.as_str())
//                 .bind(Utc::now())
//                 .bind(droplet)
//                 .bind(e.as_str())
//                 .execute(pool)
//                 .await?;
//         }
//         None => {
//             let sql = r#"
//             UPDATE instances
//             SET (instance_status, last_update_ts) = ($1, $2)
//             WHERE droplet = $3
//             "#;
//             sqlx::query(sql)
//                 .bind(status.as_str())
//                 .bind(Utc::now())
//                 .bind(droplet)
//                 .execute(pool)
//                 .await?;
//         }
//     }
//     Ok(())
// }

// pub async fn update_instance_last_updated(
//     pool: &PgPool,
//     droplet: &str,
//     exchange_name: Option<&ExchangeName>,
// ) -> Result<(), sqlx::Error> {
//     match exchange_name {
//         Some(e) => {
//             let sql = r#"
//             UPDATE instances
//             SET last_update_ts = $1
//             WHERE droplet = $2 AND exchange_name = $3
//             "#;
//             sqlx::query(sql)
//                 .bind(Utc::now())
//                 .bind(droplet)
//                 .bind(e.as_str())
//                 .execute(pool)
//                 .await?;
//         }
//         None => {
//             let sql = r#"
//             UPDATE instances
//             SET last_update_ts = $1
//             WHERE droplet = $2
//             "#;
//             sqlx::query(sql)
//                 .bind(Utc::now())
//                 .bind(droplet)
//                 .execute(pool)
//                 .await?;
//         }
//     }
//     Ok(())
// }

// pub async fn select_instances(pool: &PgPool) -> Result<Vec<Instance>, sqlx::Error> {
//     let rows = sqlx::query_as!(
//         Instance,
//         r#"
//         SELECT instance_type as "instance_type: InstanceType",
//             droplet,
//             exchange_name as "exchange_name: Option<ExchangeName>",
//             instance_status as "instance_status: InstanceStatus",
//             restart, last_restart_ts, restart_count, num_markets, last_update_ts,
//             last_message_ts
//         FROM instances
//         "#
//     )
//     .fetch_all(pool)
//     .await?;
//     Ok(rows)
// }

#[cfg(test)]
mod tests {
    // use crate::{
    //     instances::{
    //         insert_or_update_instance_mita, update_instance_last_updated, update_instance_status,
    //         InstanceStatus,
    //     },
    //     mita::Mita,
    //     utilities::get_input,
    // };

    // #[tokio::test]
    // pub async fn insert_mita_instance() {
    //     let mita = Mita::new().await;
    //     // Clear table
    //     let sql = r#"DELETE FROM instances WHERE 1=1"#;
    //     sqlx::query(sql)
    //         .execute(&mita.ed_pool)
    //         .await
    //         .expect("Failed to clear isntances table.");
    //     // Insert mita
    //     insert_or_update_instance_mita(&mita)
    //         .await
    //         .expect("Failed to insert/update mita.");
    // }

    // #[tokio::test]
    // pub async fn insert_conflict_update_mita_instance() {
    //     let mita = Mita::new().await;
    //     // Clear table
    //     let sql = r#"DELETE FROM instances WHERE 1=1"#;
    //     sqlx::query(sql)
    //         .execute(&mita.ed_pool)
    //         .await
    //         .expect("Failed to clear isntances table.");
    //     // Insert mita
    //     insert_or_update_instance_mita(&mita)
    //         .await
    //         .expect("Failed to insert/update mita.");
    //     let _input: String = get_input("Press enter to continue: ");
    //     insert_or_update_instance_mita(&mita)
    //         .await
    //         .expect("Failed to insert/update mita.");
    // }

    // #[tokio::test]
    // pub async fn update_mita_instance_status() {
    //     let mita = Mita::new().await;
    //     // Clear table
    //     let sql = r#"DELETE FROM instances WHERE 1=1"#;
    //     sqlx::query(sql)
    //         .execute(&mita.ed_pool)
    //         .await
    //         .expect("Failed to clear isntances table.");
    //     // Insert mita
    //     insert_or_update_instance_mita(&mita)
    //         .await
    //         .expect("Failed to insert/update mita.");
    //     update_instance_status(
    //         &mita.ed_pool,
    //         &mita.settings.application.droplet,
    //         Some(&mita.exchange.name),
    //         &InstanceStatus::Terminated,
    //     )
    //     .await
    //     .expect("Failed to update instance status.");
    // }

    // #[tokio::test]
    // pub async fn update_mita_instance_last_update() {
    //     let mita = Mita::new().await;
    //     // Clear table
    //     let sql = r#"DELETE FROM instances WHERE 1=1"#;
    //     sqlx::query(sql)
    //         .execute(&mita.ed_pool)
    //         .await
    //         .expect("Failed to clear isntances table.");
    //     // Insert mita
    //     insert_or_update_instance_mita(&mita)
    //         .await
    //         .expect("Failed to insert/update mita.");
    //     update_instance_last_updated(
    //         &mita.ed_pool,
    //         &mita.settings.application.droplet,
    //         Some(&mita.exchange.name),
    //     )
    //     .await
    //     .expect("Failed to update last upadted.");
    // }
}
