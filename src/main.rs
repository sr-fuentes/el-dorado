use clap::App;
use el_dorado::configuration::get_configuration;
use el_dorado::{exchanges::add, historical::run};
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

    // Load clap commands and arguments
    let matches = App::new("El Dorado")
        .version("1.0")
        .subcommand(App::new("add").about("add new exchange to app"))
        .subcommand(App::new("refresh").about("refresh markets for exchange"))
        .subcommand(App::new("edit").about("edit exchange information"))
        .subcommand(App::new("run").about("run el-dorado for a market"))
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        Some("add") => add(&connection_pool).await,
        Some("refresh") => println!("Refresh is not yet implemented."),
        Some("edit") => println!("Edit is not yet implemented."),
        Some("run") => run(&connection_pool, configuration).await,
        None => println!("Please run with subcommands: `add` `refresh` `edit` or `run`."),
        _ => unreachable!(), // CLAP will error out before running this arm
    }

    // Create tasks

    // Await tasks
}
