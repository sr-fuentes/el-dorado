use crate::candles::*;
use crate::exchanges::ExchangeName;
use crate::inquisidor::Inquisidor;
use crate::markets::{select_markets_by_market_data_status, MarketDetail, MarketStatus};
use crate::trades::*;
use crate::validation::insert_candle_count_validation;
use chrono::Duration;
use csv::Writer;

impl Inquisidor {
    pub async fn archive_validated_trades(&self) {
        // Get Active markets
        let markets = select_markets_by_market_data_status(&self.ig_pool, &MarketStatus::Active)
            .await
            .expect("Failed to fetch active markets.");
        // Check for trades to archive in each active market
        for market in markets.iter() {
            self.archive_validated_trades_for_market(market).await;
        }
    }

    pub async fn archive_validated_trades_for_market(&self, market: &MarketDetail) {
        // Get mark{et table for table and file
        // let market_table_name = market.strip_name();
        // Check directory for exchange csv is created
        let p = format!(
            "{}/csv/{}",
            &self.settings.application.archive_path,
            &market.exchange_name.as_str()
        );
        std::fs::create_dir_all(&p).expect("Failed to create directories.");
        // Get validated but not archived 01d candles
        let candles_to_archive =
            select_candles_valid_not_archived(&self.ig_pool, &market.market_id)
                .await
                .expect("Failed to fetch candles to archive.");
        // Archive trades
        for candle in candles_to_archive.iter() {
            println!(
                "Archiving {:?} - {} {:?} daily trades.",
                &market.exchange_name, &market.market_name, &candle.datetime
            );
            // Select trades associated with candle since we are working with trades
            // separate by exchange
            let archive_success = match market.exchange_name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    self.archive_ftx_trades(market, candle, &p).await
                }
                ExchangeName::Gdax => self.archive_gdax_trades(market, candle, &p).await,
            };
            if archive_success {
                // Delete trades from validate table
                match market.exchange_name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        delete_trades_by_time(
                            &self.ftx_pool,
                            &market.exchange_name,
                            market,
                            "validated",
                            candle.datetime,
                            candle.datetime + Duration::days(1),
                        )
                        .await
                        .expect("Failed to delete archived trades.");
                    }
                    ExchangeName::Gdax => {
                        delete_trades_by_time(
                            &self.gdax_pool,
                            &market.exchange_name,
                            market,
                            "validated",
                            candle.datetime,
                            candle.datetime + Duration::days(1),
                        )
                        .await
                        .expect("Failed to delete archived trades.");
                    }
                }
                update_candle_archived(&self.ig_pool, &market.market_id, candle)
                    .await
                    .expect("Failed to update candle archive status.");
            }
        }
    }

    pub async fn archive_ftx_trades(
        &self,
        market: &MarketDetail,
        candle: &DailyCandle,
        path: &str,
    ) -> bool {
        let trades_to_archive = select_ftx_trades_by_time(
            &self.ftx_pool,
            &market.exchange_name,
            market,
            "validated",
            candle.datetime,
            candle.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to fetch validated trades.");
        // Check the number of trades selected == trade count from candle
        if trades_to_archive.len() as i64 != candle.trade_count {
            println!(
                "Trade count does not match on candle. Candle: {}, Trade Count {}",
                candle.trade_count,
                trades_to_archive.len()
            );
            // Insert count validation
            insert_candle_count_validation(
                &self.ig_pool,
                &market.exchange_name,
                &market.market_id,
                &candle.datetime,
            )
            .await
            .expect("Failed to create count validation.");
            return false;
        }
        // Define filename = TICKER_YYYYMMDD.csv
        let f = format!("{}_{}.csv", market.as_strip(), candle.datetime.format("%F"));
        // Set filepath and file name
        let fp = std::path::Path::new(path).join(f);
        // Write trades to file
        let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
        for trade in trades_to_archive.iter() {
            wtr.serialize(trade).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
        true
    }

    pub async fn archive_gdax_trades(
        &self,
        market: &MarketDetail,
        candle: &DailyCandle,
        path: &str,
    ) -> bool {
        let trades_to_archive = select_gdax_trades_by_time(
            &self.gdax_pool,
            market,
            "validated",
            candle.datetime,
            candle.datetime + Duration::days(1),
        )
        .await
        .expect("Failed to fetch validated trades.");
        // Check the number of trades selected == trade count from candle
        if trades_to_archive.len() as i64 != candle.trade_count {
            println!(
                "Trade count does not match on candle. Candle: {}, Trade Count {}",
                candle.trade_count,
                trades_to_archive.len()
            );
            // Insert count validation
            insert_candle_count_validation(
                &self.ig_pool,
                &market.exchange_name,
                &market.market_id,
                &candle.datetime,
            )
            .await
            .expect("Failed to create count validation.");
            return false;
        }
        // Define filename = TICKER_YYYYMMDD.csv
        let f = format!("{}_{}.csv", market.as_strip(), candle.datetime.format("%F"));
        // Set filepath and file name
        let fp = std::path::Path::new(path).join(f);
        // Write trades to file
        let mut wtr = Writer::from_path(fp).expect("Failed to open file.");
        for trade in trades_to_archive.iter() {
            wtr.serialize(trade).expect("Failed to serialize trade.");
        }
        wtr.flush().expect("Failed to flush wtr.");
        true
    }
}

#[cfg(test)]
mod test {
    use crate::candles::*;
    use crate::configuration::get_configuration;
    use crate::exchanges::{client::RestClient, ftx::Trade, select_exchanges};
    use crate::markets::{select_market_detail, select_market_ids_by_exchange};
    use crate::trades::select_ftx_trades_by_time;
    use chrono::{Duration, DurationRound};
    use csv::Writer;
    use flate2::{write::GzEncoder, write::ZlibEncoder, Compression};
    use sqlx::PgPool;
    use std::fs::File;
    use std::io::{copy, BufReader};

    #[tokio::test]
    async fn fetch_trades_and_write_to_csv() {
        // TODO - Make for all exchanges - NOT JUST FTX
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);

        // Create db connection
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
            .await
            .expect("Failed to connect to Postgres.");

        // Get exchanges from database
        let exchanges = select_exchanges(&pool)
            .await
            .expect("Could not fetch exchanges.");

        // Match exchange to exchanges in database
        let exchange = exchanges
            .iter()
            .find(|e| e.name.as_str() == configuration.application.exchange)
            .unwrap();

        // Get REST client for exchange
        let client = RestClient::new(&exchange.name);

        // Get input from config for market to archive
        let market_ids = select_market_ids_by_exchange(&pool, &exchange.name)
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
                &exchange.name,
                &market.market_id,
                c.datetime + Duration::days(1),
            )
            .await
            .expect("Could not fetch candles."),
            Err(sqlx::Error::RowNotFound) => {
                select_candles(&pool, &exchange.name, &market.market_id, 900)
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
        insert_candles_01d(&pool, &market.market_id, &resampled_candles, false)
            .await
            .expect("Could not insert candles.");

        // Get exchange candles for validation
        let first_candle = resampled_candles.first().unwrap().datetime;
        let last_candle = resampled_candles.last().unwrap().datetime;
        let mut exchange_candles = get_ftx_candles_daterange::<crate::exchanges::ftx::Candle>(
            &client,
            &market_detail,
            first_candle,
            last_candle,
            86400,
        )
        .await;

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
            let vol_is_validated =
                validate_candle(&exchange.name, &candle, &mut exchange_candles, &None);
            // Update candle validation status
            if hb_is_validated && vol_is_validated {
                update_candle_validation(
                    &pool,
                    &exchange.name,
                    &market.market_id,
                    &candle,
                    TimeFrame::D01,
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
                &exchange.name,
                &market_detail,
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
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
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
        let pool = PgPool::connect_with(configuration.ftx_db.with_db())
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
