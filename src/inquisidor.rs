use crate::configuration::{get_configuration, Settings};
use sqlx::PgPool;

#[derive(Debug)]
pub struct Inquisidor {
    pub settings: Settings,
    pub pool: PgPool,
}

impl Inquisidor {
    pub async fn new() -> Self {
        // Load configuration settings
        let settings = get_configuration().expect("Failed to read configuration.");
        // Create db connection with pgpool
        let pool = PgPool::connect_with(settings.database.with_db())
            .await
            .expect("Failed to connect to postgres db.");
        Self {
            settings,
            pool,
        }
    }
}