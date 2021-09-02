use el_dorado::configuration::get_configuration;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Load telemetry
    // Load configuration
    // Build app
    // Run app

    // Build App that can :

    // 1) Run Backfill Hist Trades
    //  a) Determine Start Time, End Time
    //  b) Backfill missing trades
    //  c) Calc and save metrics to db on given intervals
    //  d) Validate trades and archive

    // 2) Listen to live websocket trade feed
    //  a) Read from websocket and write to local disk

    // 3) Calculate candles & metrics on an interval basis

    // Load configuration
    let configuration = get_configuration().expect("Failed to read configuration.");
    println!("Configuration: {:?}", configuration);

    // Create db connection
    let connection_pool = PgPool::connect(&configuration.database.connection_string())
        .await
        .expect("Failed to connect to Postgres.");
    
    // Get asset
    Ok(())
}
