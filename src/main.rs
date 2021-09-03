use el_dorado::configuration::get_configuration;
use el_dorado::markets::get_market;
use sqlx::PgPool;
use clap::App;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Load telemetry
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

    // Load clap commands and arguments
    let matches = App::new("El Dorado")
        .version("1.0")
        .subcommand(App::new("add")
                .about("add new exchange to app")
        )
        .subcommand(App::new("refresh")
                .about("refresh markets for exchange")
        )
        .subcommand(App::new("edit")
                .about("edit exchange information")
        )
        .subcommand(App::new("run")
                .about("run el-dorado for a market")
        )
        .get_matches();

    println!("Matches: {:?}", matches);

    match matches.subcommand_name() {
        Some("add") => println!("Add was used."),
        Some("refresh") => println!("Refresh was used."),
        Some("edit") => println!("Edit was used."),
        Some("run") => println!("Run was used."),
        None => println!("No subcommand was used."),
        _ => println!("Some other subcommand was used"),
    }

    // Load configuration
    let configuration = get_configuration().expect("Failed to read configuration.");
    println!("Configuration: {:?}", configuration);

    // Create db connection
    let connection_pool = PgPool::connect(&configuration.database.connection_string())
        .await
        .expect("Failed to connect to Postgres.");
    
    // Get market
    let market_id = get_market();
    println!("Market Id: {}", market_id);

    // Create tasks

    // Await tasks

    Ok(())
}
