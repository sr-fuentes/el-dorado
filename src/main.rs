use clap::App;
use el_dorado::cleanup::cleanup_03;
use el_dorado::configuration::get_configuration;
use el_dorado::mita::Mita;
use el_dorado::{archive::archive, exchanges::add, historical::run, stream::stream};
use sqlx::PgPool;

#[tokio::main]
async fn main() {
    // Load configuration
    let configuration = get_configuration().expect("Failed to read configuration.");
    println!("Configuration: {:?}", configuration);

    // Create db connection
    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");

    // Create new mita instance
    let mita = Mita::new().await;

    // Load clap commands and arguments
    let matches = App::new("El Dorado")
        .version("0.1.4")
        .subcommand(App::new("add").about("add new exchange to app"))
        .subcommand(App::new("refresh").about("refresh markets for exchange"))
        .subcommand(App::new("edit").about("edit exchange information"))
        .subcommand(App::new("run").about("run el-dorado for a market"))
        .subcommand(App::new("historical").about("backfill to current start of day"))
        .subcommand(App::new("cleanup").about("run current cleanup script"))
        .subcommand(App::new("archive").about("archive trade for valid candles"))
        .subcommand(App::new("stream").about("stream trades to db"))
        .subcommand(App::new("stream2").about("steam trades to db w/mita"))
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        Some("add") => add(&connection_pool, &configuration).await,
        Some("refresh") => println!("Refresh is not yet implemented."),
        Some("edit") => println!("Edit is not yet implemented."),
        Some("run") => run(&connection_pool, &configuration).await,
        Some("historical") => run(&connection_pool, &configuration).await,
        Some("cleanup") => cleanup_03(&connection_pool, &configuration).await, // Remove options when no cleanup job
        Some("archive") => archive(&connection_pool, &configuration).await,
        Some("stream") => stream(&connection_pool, &configuration).await,
        Some("stream2") => mita.stream().await,
        None => println!("Please run with subcommands: `add` `refresh` `edit` or `run`."),
        _ => unreachable!(), // CLAP will error out before running this arm
    }

    // Create tasks

    // Await tasks
}
