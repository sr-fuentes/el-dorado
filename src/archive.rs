use crate::candles::*;
use crate::configuration::*;
use crate::markets::select_markets_active;
use crate::trades::*;
use chrono::Duration;
use csv::Writer;
use sqlx::PgPool;

pub async fn archive(pool: &PgPool, config: &Settings) {
    // Get Active markets
    let markets = select_markets_active(pool)
        .await
        .expect("Could not fetch active markets.");

    // Check for trades to archive for each active market
    for market in markets.iter() {
        // Get market table name for table and file
        let market_table_name = market.strip_name();
        // Check directory for exchange csv is created
        let p = format!(
            "{}/csv/{}",
            config.application.archive_path, &market.exchange_name
        );
        std::fs::create_dir_all(&p).expect("Could not create directories.");
        // Get validated but not archived 01d candles
        let candles_to_archive = select_candles_valid_not_archived(pool, &market.market_id)
            .await
            .expect("Could not fetch valid not archived candles.");

        // Archive trades
        for candle in candles_to_archive.iter() {
            println!(
                "Archiving {} - {} {:?} daily trades.",
                &market.exchange_name, &market.market_name, &candle.datetime
            );
            // Select trades associated w/ candle
            let trades_to_archive = select_ftx_trades_by_time(
                pool,
                &market.exchange_name,
                market_table_name.as_str(),
                "validated",
                candle.datetime,
                candle.datetime + Duration::days(1),
            )
            .await
            .expect("Could not fetch validated trades.");
            // Validate the number of trades selected = trade count from candle
            if trades_to_archive.len() as i64 != candle.trade_count {
                println!(
                    "Trade count does not match candle. Candle {:?}, Trade Count {}",
                    candle,
                    trades_to_archive.len()
                );
                continue;
            }
            // Define filename = TICKER_YYYYMMDD.csv
            let f = format!("{}_{}.csv", market_table_name, candle.datetime.format("%F"));
            // Set filepath and file name
            let fp = std::path::Path::new(&p).join(f);
            // Write trades to file
            let mut wtr = Writer::from_path(fp).expect("Could not open file.");
            for trade in trades_to_archive.iter() {
                wtr.serialize(trade).expect("could not serialize trade.");
            }
            wtr.flush().expect("could not flush wtr.");
            // Delete trades from validated table
            delete_ftx_trades_by_time(
                pool,
                &market.exchange_name,
                market_table_name.as_str(),
                "validated",
                candle.datetime,
                candle.datetime + Duration::days(1),
            )
            .await
            .expect("Could not delete archived trades.");
            // Update candle status to archived
            update_candle_archived(pool, &market.market_id, candle)
                .await
                .expect("Could not update candle archive status.");
        }
    }
}

#[cfg(test)]
mod test {
    use crate::candles::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::{fetch_exchanges, ftx::RestClient, ftx::Trade, ExchangeName};
    use crate::markets::{fetch_markets, select_market_detail};
    use crate::trades::select_ftx_trades_by_time;
    use chrono::{Duration, DurationRound};
    use csv::Writer;
    use flate2::{write::GzEncoder, write::ZlibEncoder, Compression};
    use sqlx::PgPool;
    use std::fs::File;
    use std::io::{copy, BufReader};

    #[tokio::test]
    async fn fetch_trades_and_write_to_csv() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = fetch_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");

        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == configuration.application.exchange)
            .unwrap();

        // Get REST client for exchange
        let client = match exchange.name {
            ExchangeName::FtxUs => RestClient::new_us(),
            ExchangeName::Ftx => RestClient::new_intl(),
        };

        // Get input from config for market to archive
        let market_ids = fetch_markets(&pool, &exchange)
            .await
            .expect("Could not fetch exchanges.");
        let market = market_ids
            .iter()
            .find(|m| m.market_name == configuration.application.market)
            .unwrap();
        let market_detail = select_market_detail(&pool, market)
            .await
            .expect("Could not fetch market detail.");

        // Gets 15t candles for market newer than last 01d candle
        let candles = match select_last_01d_candle(&pool, &market.market_id).await {
            Ok(c) => select_candles_gte_datetime(
                &pool,
                &exchange.name.as_str(),
                &market.market_id,
                c.datetime + Duration::days(1),
            )
            .await
            .expect("Could not fetch candles."),
            Err(sqlx::Error::RowNotFound) => {
                select_candles(&pool, &exchange.name.as_str(), &market.market_id, 900)
                    .await
                    .expect("Could not fetch candles.")
            }
            Err(e) => panic!("Sqlx Error: {:?}", e),
        };

        // If there are no candles, then return, nothing to archive
        if candles.len() == 0 {
            return;
        };

        // Filter candles for last full day
        let next_candle = candles.last().unwrap().datetime + Duration::seconds(900);
        let last_full_day = next_candle.duration_trunc(Duration::days(1)).unwrap();
        let filtered_candles: Vec<Candle> = candles
            .iter()
            .filter(|c| c.datetime < last_full_day)
            .cloned()
            .collect();

        // Resample to 01d candles
        let resampled_candles =
            resample_candles(market.market_id, &filtered_candles, Duration::days(1));

        // If there are no resampled candles, then return
        if resampled_candles.len() == 0 {
            return;
        };

        // Insert 01D candles
        insert_candles_01d(&pool, &market.market_id, &resampled_candles)
            .await
            .expect("Could not insert candles.");

        // Get exchange candles for validation
        let first_candle = resampled_candles.first().unwrap().datetime;
        let last_candle = resampled_candles.last().unwrap().datetime;
        let mut exchange_candles =
            get_ftx_candles(&client, &market_detail, first_candle, last_candle, 86400).await;

        // Validate 01d candles - if all 15T candles are validated (trades archived)
        for candle in resampled_candles.iter() {
            // get 15t candles that make up candle
            let hb_candles: Vec<Candle> = filtered_candles
                .iter()
                .filter(|c| {
                    c.datetime.duration_trunc(Duration::days(1)).unwrap() == candle.datetime
                })
                .cloned()
                .collect();
            println!("Validating {:?} with {:?}", candle, hb_candles);
            // Check if all hb candles are valid
            let hb_is_validated = hb_candles.iter().all(|c| c.is_validated == true);
            // Check if volume matches value
            let vol_is_validated = validate_candle(&candle, &mut exchange_candles);
            // Update candle validation status
            if hb_is_validated && vol_is_validated {
                update_candle_validation(
                    &pool,
                    &exchange.name.as_str(),
                    &market.market_id,
                    &candle,
                    86400,
                )
                .await
                .expect("Could not update candle validation status.");
            }
        }

        // Get validated but not archived 01d candles
        let candles_to_archive = select_candles_valid_not_archived(&pool, &market.market_id)
            .await
            .expect("Could not fetch valid not archived candles.");

        // Archive trades
        for candle in candles_to_archive.iter() {
            // Select trades associated w/ candle
            let _trades_to_archive = select_ftx_trades_by_time(
                &pool,
                &exchange.name.as_str(),
                market.strip_name().as_str(),
                "validated",
                candle.datetime,
                candle.datetime + Duration::days(1),
            )
            .await
            .expect("Could not fetch validated trades.");
            // Write trades to file
        }

        // Update 01d candles to is_archived
    }

    #[tokio::test]
    async fn write_to_csv() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get all trades from processed table
        let sql = r#"
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM trades_ftxus_processed
            "#;
        let trades = sqlx::query_as::<_, Trade>(&sql)
            .fetch_all(&pool)
            .await
            .expect("Could not fetch trades.");

        // Write trades to csv file
        let mut wtr = Writer::from_path("trades.csv").expect("Could not open file.");
        for trade in trades.iter() {
            wtr.serialize(trade).expect("could not serialize trade.");
        }
        wtr.flush().expect("could not flush wtr.");

        // Take csv file and compress
        let mut input = BufReader::new(File::open("trades.csv").unwrap());
        let output = File::create("trades.csv.zlib  ").unwrap();
        let mut encoder = ZlibEncoder::new(output, Compression::default());
        copy(&mut input, &mut encoder).unwrap();
        let _output = encoder.finish().unwrap();
    }

    #[tokio::test]
    async fn write_to_compressed_csv() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get all trades from processed table
        let sql = r#"
            SELECT trade_id as id, price, size, side, liquidation, time
            FROM trades_ftxus_processed
            "#;
        let trades = sqlx::query_as::<_, Trade>(&sql)
            .fetch_all(&pool)
            .await
            .expect("Could not fetch trades.");

        // Write trades to compressed csv file
        let mut wtr = Writer::from_writer(vec![]);
        for trade in trades.iter() {
            wtr.serialize(trade).expect("could not serialize trade.");
        }

        let f = File::create("test2.csv.gz").expect("could not create file.");
        // let mut gz = GzBuilder::new()
        //     .filename("test.csv")
        //     .comment("test file, please delete")
        //     .write(f, Compression::default());
        //let data = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
        let mut _gz = GzEncoder::new(f, Compression::default());
        let _test = wtr.into_inner().unwrap();
        // gz.write_all(&test).expect("could not write to file.");
        // gz.finish().expect("could not close file.");
    }
}
