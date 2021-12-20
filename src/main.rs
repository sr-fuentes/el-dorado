use clap::App;
use el_dorado::inquisidor::Inquisidor;
use el_dorado::mita::Mita;

#[tokio::main]
async fn main() {
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
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        Some("add") => {
            // Create new admin instance and add new exchange
            let ig = Inquisidor::new().await;
            ig.add_new_exchange().await;
        }
        Some("refresh") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.refresh_exchange().await;
        }
        Some("edit") => println!("Edit is not yet implemented."),
        Some("run") => {
            // Create new mita instance and run stream and backfill until no restart
            let mita = Mita::new().await;
            mita.reset_trade_tables(&["ws", "rest", "processed", "validated"])
                .await;
            let res = tokio::select! {
                res1 = mita.run() => res1,
                res2 = mita.stream() => res2,
            };
            println!("Res: {:?}", res);
        }
        Some("historical") => {
            // Create new mita instance and backfill until start of current day
            let mita = Mita::new().await;
            mita.reset_trade_tables(&["rest", "processed", "validated"])
                .await;
            mita.historical("eod").await;
        }
        Some("cleanup") => println!("No cleanup job available."), // Remove options when no cleanup job
        Some("archive") => {
            // Create new admin instance and add new exchange
            let ig = Inquisidor::new().await;
            ig.archive_validated_trades().await;
        }
        Some("stream") => {
            // Create new mita instance and run stream until no restart
            let mita = Mita::new().await;
            mita.reset_trade_tables(&["ws"]).await;
            mita.stream().await;
        }
        None => println!("Please run with subcommands: `add` `refresh` `edit` or `run`."),
        _ => unreachable!(), // CLAP will error out before running this arm
    }
}
