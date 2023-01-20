use clap::App;
use el_dorado::eldorado::ElDorado;

#[tokio::main]
async fn main() {
    // Load clap commands and arguments
    let matches = App::new("El Dorado")
        .version("0.4.0")
        .subcommand(App::new("refresh").about("refresh markets for exchange"))
        .subcommand(App::new("run").about("run el-dorado instance"))
        .subcommand(App::new("fill").about("fill from first candle to start"))
        // .subcommand(App::new("archive").about("archive trade for valid candles"))
        .subcommand(App::new("stream").about("stream trades to db"))
        // .subcommand(App::new("monitor").about("monitor active processes"))
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        Some("refresh") => {
            // Create new admin instance and refresh exchange
            let eld = ElDorado::new().await;
            eld.refresh_exchange().await;
        }
        Some("run") => {
            // Create new eldorado instance and run the default fn for the instance type
            // For IG => manage the system
            // For MITA => run trade/candle/metrics engine for give markets
            let mut eld = ElDorado::new().await;
            eld.run().await;
        }
        Some("fill") => {
            // Download and archive trades from beginning of normal running sync (min 90 days) to
            // the first trades of exchange.
            let eld = ElDorado::new().await;
            match eld.prompt_market_input(&None).await {
                Some(m) => eld.fill(&Some(m), false).await,
                None => println!("No valid market to fill."),
            }
        }
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
        // Some("monitor") => {
        //     // Create ig instance and review all existing active processes
        //     let ig = Inquisidor::new().await;
        //     ig.monitor().await;
        // }
        None => println!("Please run with subcommands: `run` `refresh` `stream` or `archive`."),
        _ => unreachable!(), // CLAP will error out before running this arm
    }
}
