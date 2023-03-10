use crate::{
    configuration::{Database, Settings},
    exchanges::{client::RestClient, ExchangeName},
    instances::{Instance, InstanceType},
    markets::{MarketDetail, MarketStatus},
    utilities::Twilio,
};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use std::{collections::HashMap, convert::TryInto};
use uuid::Uuid;

#[derive(Debug)]
pub struct ElDorado {
    pub pools: HashMap<Database, PgPool>,
    pub clients: HashMap<ExchangeName, RestClient>,
    pub twilio: Twilio,
    pub markets: Vec<MarketDetail>,
    pub market_names: HashMap<ExchangeName, HashMap<String, MarketDetail>>,
    pub market_ids: HashMap<Uuid, MarketDetail>,
    pub instance: Instance,
    pub storage_path: String,
    pub start_dt: DateTime<Utc>,
    pub sync_days: i64,
}

impl ElDorado {
    // Initialize new instance of ElDorado based on the configuration settings
    pub async fn new() -> Option<Self> {
        // Load configuration settings
        let settings = Settings::from_configuration().expect("Failed to read configuration.");
        // Create PgPools to each database in settings
        let pools = ElDorado::create_pgpools(&settings).await;
        // Create clients map
        let clients = RestClient::initialize_client_map();
        // Create Twilio client
        let twilio = Twilio::new();
        // Initialize Instance
        let mut instance = Instance::initialize(&pools[&Database::ElDorado], &settings).await;
        // Load markets and insert into market maps
        let market_details = MarketDetail::select_all(&pools[&Database::ElDorado])
            .await
            .expect("Failed to select markets.");
        let (market_names, market_ids) = ElDorado::map_markets(&market_details);
        // Get markets for the system instance
        let markets = match instance.instance_type {
            InstanceType::Ig | InstanceType::Conqui => {
                // Filter for all active markets
                market_details
                    .iter()
                    .filter(|m| m.status == MarketStatus::Active)
                    .cloned()
                    .collect()
            }
            InstanceType::Mita => {
                let markets = MarketDetail::select_by_exchange_mita(
                    &pools[&Database::ElDorado],
                    &instance.exchange_name.unwrap(),
                    &instance.droplet,
                )
                .await
                .expect("Failed to select market details.");
                // Validate mita markets are in the correct status and have a candle timeframe
                for market in markets.iter() {
                    if market.status != MarketStatus::Active {
                        println!(
                            "{} is not in Active status. Current status: {}",
                            market.market_name,
                            market.status.as_str()
                        );
                        return None;
                    } else {
                        println!("{} initialized for Mita.", market.market_name);
                    }
                }
                markets
            }
        };
        // Update instance market number field
        instance.num_markets = markets.len() as i32;
        // Get storage path from config
        let storage_path = settings.application.archive_path.clone();
        Some(Self {
            pools,
            clients,
            twilio,
            markets,
            market_names,
            market_ids,
            instance,
            storage_path,
            start_dt: Utc::now(),
            sync_days: 100,
        })
    }

    // Run the default function based on InstanceType and continue restarting until explict exit.
    // IG - manage the events and state
    // Mita - manage the trades / candles and metrics for the give exchange and markets
    pub async fn run(&mut self) {
        // let mut restart = self.instance.restart;
        while self.instance.restart {
            self.start_dt = Utc::now();
            let restart = match self.instance.instance_type {
                InstanceType::Ig => self.inquisidor().await,
                InstanceType::Mita => self.mita().await,
                InstanceType::Conqui => return,
            };
            (
                self.instance.restart,
                self.instance.restart_count,
                self.instance.last_restart_ts,
            ) = self.process_restart(restart).await;
            // match self.instance.restart {
            //     true => self.update_instance_status(&InstanceStatus::Restart).await,
            //     false => self.update_instance_status(&InstanceStatus::Shutdown).await,
            // }
        }
    }

    // Review the restart parameter that was returned from the instance run. If instructed to
    // restart, sleep for the scheduled time and update restart atttributes on the instance
    pub async fn process_restart(
        &self,
        restart: bool,
    ) -> (bool, Option<i32>, Option<DateTime<Utc>>) {
        // If time from last restart is more than 24 hours - sleep 5 seconds before restart, else
        // follow pattern to increase time as restarts increase
        let (sleep_duration, restart_count) = if self.instance.last_restart_ts.is_none()
            || Utc::now() - self.instance.last_restart_ts.unwrap() > Duration::days(1)
        {
            (5, 1)
        } else {
            match self.instance.restart_count.unwrap() {
                0 => (5, self.instance.restart_count.unwrap() + 1),
                1 => (30, self.instance.restart_count.unwrap() + 1),
                _ => (60, self.instance.restart_count.unwrap() + 1),
            }
        };
        if restart {
            println!("Sleeping for {:?} before restarting.", sleep_duration);
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_duration)).await;
            (restart, Some(restart_count), Some(Utc::now()))
        } else {
            println!("No restart. Shutdown.");
            (
                restart,
                self.instance.restart_count,
                self.instance.last_restart_ts,
            )
        }
    }

    // Create hashmap of database pool connections based on instance type.
    pub async fn create_pgpools(settings: &Settings) -> HashMap<Database, PgPool> {
        let instance_type = settings
            .application
            .instance_type
            .clone()
            .try_into()
            .expect("Failed to parse instance type.");
        let pools = match instance_type {
            InstanceType::Mita => Self::create_pgpools_mita(settings).await,
            InstanceType::Ig | InstanceType::Conqui => Self::create_pgpools_ig(settings).await,
        };
        pools
    }

    // Create hashmap of the system db and the trade db for the Mita. Prevents excessive db
    // connections to exchange trade dbs that are not utilized when compared to IG pools
    pub async fn create_pgpools_mita(settings: &Settings) -> HashMap<Database, PgPool> {
        let mut pools = HashMap::new();
        let exchange: ExchangeName = settings
            .application
            .exchange
            .clone()
            .try_into()
            .expect("Failed to parse exchange.");
        // Insert system pool
        pools.insert(
            Database::ElDorado,
            PgPool::connect_with(settings.ed_db.with_db())
                .await
                .expect("Failed to connect to El Dorado database."),
        );
        // Insert trade pool
        match exchange {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                pools.insert(
                    Database::Ftx,
                    PgPool::connect_with(settings.ftx_db.with_db())
                        .await
                        .expect("Failed to connect to El Dorado database."),
                );
            }
            ExchangeName::Gdax => {
                pools.insert(
                    Database::Gdax,
                    PgPool::connect_with(settings.gdax_db.with_db())
                        .await
                        .expect("Failed to connect to El Dorado database."),
                );
            }
        };
        pools
    }

    // Create hashmap of all database connections, the main system db and each exchange trade db.
    // To be used by IG instances which manage the entire platform
    pub async fn create_pgpools_ig(settings: &Settings) -> HashMap<Database, PgPool> {
        let mut pools = HashMap::new();
        // Insert system pool
        pools.insert(
            Database::ElDorado,
            PgPool::connect_with(settings.ed_db.with_db())
                .await
                .expect("Failed to connect to El Dorado database."),
        );
        pools.insert(
            Database::Ftx,
            PgPool::connect_with(settings.ftx_db.with_db())
                .await
                .expect("Failed to connect to El Dorado database."),
        );
        pools.insert(
            Database::Gdax,
            PgPool::connect_with(settings.gdax_db.with_db())
                .await
                .expect("Failed to connect to El Dorado database."),
        );
        pools
    }
}

#[cfg(test)]
mod test {
    use crate::eldorado::ElDorado;

    #[tokio::test]
    async fn create_new_eldorado() {
        let eld = ElDorado::new().await;
        println!("ElDorado: {:?}", eld);
    }
}
