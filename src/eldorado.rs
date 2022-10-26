use crate::{
    configuration::{Database, Settings},
    exchanges::{client::RestClient, ExchangeName},
    instances::{Instance, InstanceType},
    markets::{MarketDetail, MarketStatus},
    utilities::Twilio,
};
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
}

impl ElDorado {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = Settings::from_configuration().expect("Failed to read configuration.");
        // Create PgPools to each database in settings
        let pools = ElDorado::create_pgpools(&settings).await;
        // Create clients map
        let clients = RestClient::initialize_client_map();
        // Create Twilio client
        let twilio = Twilio::new();
        // Initialize Instance
        let mut instance = Instance::initialize(&settings);
        // Load markets and insert into market maps
        let market_details = MarketDetail::select_all(&pools[&Database::ElDorado])
            .await
            .expect("Failed to select markets.");
        let (market_names, market_ids) = ElDorado::map_markets(&market_details);
        // Get markets for the system instance
        let markets = match instance.instance_type {
            InstanceType::Ig => {
                // Filter for all active markets
                market_details
                    .iter()
                    .filter(|m| m.market_data_status == MarketStatus::Active)
                    .cloned()
                    .collect()
            }
            InstanceType::Mita => MarketDetail::select_by_exchange_mita(
                &pools[&Database::ElDorado],
                &instance.exchange_name.unwrap(),
                &instance.droplet,
            )
            .await
            .expect("Failed to select market details."),
        };
        // Update instance market number field
        instance.num_markets = markets.len() as i32;
        // Get storage path from config
        let storage_path = settings.application.archive_path.clone();
        Self {
            pools,
            clients,
            twilio,
            markets,
            market_names,
            market_ids,
            instance,
            storage_path,
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
            InstanceType::Ig => Self::create_pgpools_ig(settings).await,
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
