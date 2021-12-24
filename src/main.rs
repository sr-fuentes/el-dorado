use chrono::Utc;
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
        .subcommand(App::new("manage").about("run current cleanup script"))
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
            let mut mita = Mita::new().await;
            // Restart loop if the mita restart value is true, else exit program
            while mita.restart {
                // Set restart value to false, error handling must explicity set back to true
                mita.restart = false;
                mita.reset_trade_tables(&["ws", "rest", "processed", "validated"])
                    .await;
                let restart = tokio::select! {
                    res1 = mita.run() => res1,
                    res2 = mita.stream() => res2,
                };
                println!("Res: {:?}", restart);
                if restart {
                    let dur = mita.process_restart().await;
                    mita.last_restart = Utc::now();
                    if dur > chrono::Duration::days(1) {
                        // If there has been > 24 hours since last restart
                        // reset the counter
                        mita.restart_count = 1
                    } else {
                        mita.restart_count += 1;
                    };
                    mita.restart = true;
                }
            }
        }
        Some("historical") => {
            // Create new mita instance and backfill until start of current day
            let mita = Mita::new().await;
            mita.reset_trade_tables(&["rest", "processed", "validated"])
                .await;
            mita.historical("eod").await;
        }
        Some("manage") => {
            // Create new admin instance and refresh exchange
            let ig = Inquisidor::new().await;
            ig.run().await;
        }
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
