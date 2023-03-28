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
        .subcommand(App::new("archive").about("archive trade for valid candles"))
        .subcommand(App::new("stream").about("stream trades to db"))
        // .subcommand(App::new("monitor").about("monitor active processes"))
        .get_matches();

    // Match subcommand and route
    match matches.subcommand_name() {
        Some("refresh") => {
            // Create new admin instance and refresh exchange
            match ElDorado::new().await {
                Some(eld) => eld.refresh_exchange().await,
                None => println!("Could not create El Dorado instance."),
            }
        }
        Some("run") => {
            // Create new eldorado instance and run the default fn for the instance type
            // For IG => manage the system
            // For MITA => run trade/candle/metrics engine for give markets
            match ElDorado::new().await {
                Some(mut eld) => eld.run().await,
                None => println!("Could not create El Dorado instance."),
            }
        }
        Some("fill") => {
            // Download and archive trades from beginning of normal running sync (min 90 days) to
            // the first trades of exchange.
            match ElDorado::new().await {
                Some(eld) => match eld.prompt_market_input(&None).await {
                    Some(m) => eld.fill(&Some(m), false).await,
                    None => println!("No valid market to fill."),
                },
                None => println!("Could not create El Dorado insance."),
            }
        }
        Some("archive") => {
            // Archive any monthly trades into candle archives, update market archive table
            match ElDorado::new().await {
                Some(eld) => match eld.prompt_market_input(&None).await {
                    Some(m) => eld.archive(&Some(m)).await,
                    None => println!("No valid market to archive."),
                },
                None => println!("Could not create El Dorado instance."),
            }
        }
        Some("stream") => {
            // Create new mita instance and run stream until no restart
            match ElDorado::new().await {
                Some(eld) => {
                    let _restart = eld.stream().await;
                }
                None => println!("Could not create El Dorado instance."),
            }
        }
        // Some("monitor") => {
        //     // Create ig instance and review all existing active processes
        //     let ig = Inquisidor::new().await;
        //     ig.monitor().await;
        // }
        _ => println!("Please run with subcommands: `run` `refresh` `stream` or `archive`."),
    }
}
