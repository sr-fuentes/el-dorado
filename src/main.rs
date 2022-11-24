use clap::App;
use el_dorado::{eldorado::ElDorado, inquisidor::Inquisidor};

#[tokio::main]
async fn main() {
    // Load clap commands and arguments
    let matches = App::new("El Dorado")
        .version("0.1.4")
        // .subcommand(App::new("add").about("add new exchange to app"))
        .subcommand(App::new("refresh").about("refresh markets for exchange"))
        .subcommand(App::new("rank").about("rank exchange markets"))
        .subcommand(App::new("set").about("update ranks from proposed to current"))
        .subcommand(App::new("eldorado").about("run el-dorado instance"))
        .subcommand(App::new("fill").about("fill from first candle to start"))
        // .subcommand(App::new("candle").about("make candles from backfilled trades"))
        .subcommand(App::new("candleload").about("load candles from file to db"))
        .subcommand(App::new("manage").about("run current cleanup script"))
        // .subcommand(App::new("manual").about("manually validate bad candles"))
        // .subcommand(App::new("archive").about("archive trade for valid candles"))
        .subcommand(App::new("stream").about("stream trades to db"))
        .subcommand(App::new("monitor").about("monitor active processes"))
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        // Some("add") => {
        //     // Create new admin instance and add new exchange
        //     let ig = Inquisidor::new().await;
        //     ig.add_new_exchange().await;
        // }
        Some("refresh") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.refresh_exchange().await;
        }
        Some("rank") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.update_market_ranks().await;
        }
        Some("set") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.update_market_mitas_from_ranks().await;
        }
        Some("eldorado") => {
            // Create new eldorado instance and run the default fn for the instance type
            // For IG => manage the system
            // For MITA => run trade/candle/metrics engine for give markets
            let mut eld = ElDorado::new().await;
            eld.run().await;
        }
        Some("fill") => {
            // Download and archive trades from beginning of normal running sync (min 90 days) to
            // the first trades of exchange.
            let ig = Inquisidor::new().await;
            ig.fill().await;
        }
        // Some("candle") => {
        //     // Make candles for trades that have been backfilled
        //     let ig = Inquisidor::new().await;
        //     ig.make_candles().await;
        // }
        Some("candleload") => {
            // Make candles for trades that have been backfilled
            let ig = Inquisidor::new().await;
            ig.load_candles().await;
        }
        Some("manage") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.run().await;
        }
        // Some("manual") => {
        //     // Create new admin instance and run all manual validations
        //     let ig = Inquisidor::new().await;
        //     ig.process_candle_validations(el_dorado::validation::ValidationStatus::Open)
        //         .await;
        // }
        // Some("archive") => {
        //     // Create new admin instance and add new exchange
        //     let ig = Inquisidor::new().await;
        //     ig.archive_validated_trades().await;
        // }
        Some("stream") => {
            // Create new mita instance and run stream until no restart
            let eld = ElDorado::new().await;
            eld.stream().await;
        }
        Some("monitor") => {
            // Create ig instance and review all existing active processes
            let ig = Inquisidor::new().await;
            ig.monitor().await;
        }
        None => println!("Please run with subcommands: `add` `refresh` `edit` or `run`."),
        _ => unreachable!(), // CLAP will error out before running this arm
    }
}
