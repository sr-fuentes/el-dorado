use crate::candles::*;
use crate::exchanges::{client::RestClient, error::RestError, Exchange, ExchangeName};
use crate::markets::*;
use crate::mita::Mita;
use crate::trades::*;
use crate::utilities::Trade;
use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use serde::de::DeserializeOwned;
use sqlx::PgPool;

impl Mita {
    pub async fn historical<T: crate::utilities::Candle + DeserializeOwned>(
        &self,
        end_source: &str,
    ) {
        // Get REST client for exchange
        let client = RestClient::new(&self.exchange.name);
        for market in self.markets.iter() {
            // Validate hb, create and validate 01 candles
            self.validate_candles::<T>(&client, market).await;
            // Set start and end times for backfill
            let (start_ts, start_id) = match select_last_candle(
                &self.pool,
                &self.exchange.name,
                &market.market_id,
            )
            .await
            {
                Ok(c) => (
                    Some(c.datetime + Duration::seconds(900)),
                    Some(c.last_trade_id.parse::<i64>().unwrap()),
                ),
                Err(sqlx::Error::RowNotFound) => match &self.exchange.name {
                    ExchangeName::Ftx | ExchangeName::FtxUs => {
                        (Some(get_ftx_start(&client, market).await), None)
                    }
                    ExchangeName::Gdax => get_gdax_start(&client, market).await,
                },
                Err(e) => panic!("Sqlx Error getting start time: {:?}", e),
            };
            let end = match end_source {
                "eod" => Utc::now().duration_trunc(Duration::days(1)).unwrap(),
                // Loop until there is a trade in the ws table. If there is no trade, sleep
                // and check back until there is a trade.
                "stream" => {
                    println!(
                        "Fetching first {} streamed trade from ws.",
                        market.market_name
                    );
                    loop {
                        match select_trade_first_stream(&self.pool, &self.exchange.name, market)
                            .await
                        {
                            Ok(t) => break t.time + Duration::microseconds(1), // set trade time + 1 micro
                            Err(sqlx::Error::RowNotFound) => {
                                // There are no trades, sleep for 5 seconds
                                println!("Awaiting for ws trade for {}", market.market_name);
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            }
                            Err(e) => panic!("Sqlx Error: {:?}", e),
                        };
                    }
                }
                _ => panic!("Unsupported end time."),
            };
            // Update market data status to 'Syncing'
            update_market_data_status(
                &self.pool,
                &market.market_id,
                &MarketStatus::Backfill,
                self.settings.application.ip_addr.as_str(),
            )
            .await
            .expect("Could not update market status.");
            // Backfill from start to end
            match self.exchange.name {
                ExchangeName::Ftx | ExchangeName::FtxUs => {
                    backfill_ftx(
                        &self.pool,
                        &client,
                        &self.exchange,
                        market,
                        start_ts.unwrap(),
                        end,
                    )
                    .await
                }
                ExchangeName::Gdax => backfill_gdax(),
            };
            // If `eod` end source then run validation on new backfill
            if end_source == "eod" {
                self.validate_candles(&client, market).await;
            }
        }
    }
}

pub async fn backfill_ftx(
    pool: &PgPool,
    client: &RestClient,
    exchange: &Exchange,
    market: &MarketDetail,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) {
    // From start to end fill forward trades in 15m buckets and create candle
    // Pagination returns trades in desc order from end timestamp in 100 trade batches
    // Use last trade out of 100 (earliest timestamp) to set as end time for next request for trades
    // Until trades returned is < 100 meaning there are no further trades for interval
    // Then advance interval start by interval length and set new end timestamp and begin
    // To reterive trades.
    let mut interval_start = start;
    while interval_start < end {
        // Set end of bucket to end of interval
        let interval_end = interval_start + Duration::seconds(900);
        let mut interval_end_or_last_trade =
            std::cmp::min(interval_start + Duration::seconds(900), end);
        println!(
            "Filling {} trades for interval from {} to {}.",
            market.market_name, interval_start, interval_end_or_last_trade
        );
        while interval_start < interval_end_or_last_trade {
            // Prevent 429 errors by only requesting 4 per second
            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
            let mut new_trades = match client
                .get_ftx_trades(
                    market.market_name.as_str(),
                    Some(5000),
                    Some(interval_start),
                    Some(interval_end_or_last_trade),
                )
                .await
            {
                Err(RestError::Reqwest(e)) => {
                    if e.is_timeout() {
                        println!("Request timed out. Waiting 30 seconds before retrying.");
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.is_connect() {
                        println!(
                            "Connect error with reqwest. Waiting 30 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.status() == Some(reqwest::StatusCode::BAD_GATEWAY) {
                        println!("502 Bad Gateway. Waiting 30 seconds before retry. {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    } else if e.status() == Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) {
                        println!(
                            "503 Service Unavailable. Waiting 60 seconds before retry. {:?}",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                        continue;
                    } else {
                        panic!("Error (not timeout or connect): {:?}", e)
                    }
                }
                Err(e) => panic!("Other RestError: {:?}", e),
                Ok(result) => result,
            };
            let num_trades = new_trades.len();
            if num_trades > 0 {
                new_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                // Unwrap can be used because it will only be called
                // if there is at least one element in new_trades vec
                // Set end of interval to last trade returned for pagination
                interval_end_or_last_trade = new_trades.first().unwrap().time;
                let first_trade = new_trades.last().unwrap().time;
                println!(
                    "{} {} trades returned. First: {}, Last: {}",
                    num_trades, market.market_name, interval_end_or_last_trade, first_trade
                );
                println!("New last trade ts: {}", interval_end_or_last_trade);
                // FTX trades API takes time in microseconds. This could lead to a
                // endless loop if there are more than 100 trades in that microsecond.
                // In that case move the end to last trade minus one microsecond.
                if interval_end_or_last_trade == first_trade {
                    interval_end_or_last_trade =
                        interval_end_or_last_trade - Duration::microseconds(1);
                    println!(
                        "More than 100 trades in microsecond. Resetting to: {}",
                        interval_end_or_last_trade
                    );
                }
                // Save trades to db
                insert_ftx_trades(pool, &exchange.name, market, "rest", new_trades)
                    .await
                    .expect("Failed to insert ftx trades.");
            };
            // If new trades returns less than 100 trades then there are no more trades
            // for that interval, create the candle and process the trades for that period
            // Do not create candle for last interval (when interval_end is greater than end time)
            // because the interval_end may not be complete (ie interval_end = 9:15 and current time
            // is 9:13, you dont want to create the candle because it is not closed yet).
            if num_trades < 5000 {
                if interval_end <= end {
                    // Move trades from _rest to _processed and create candle
                    // Select trades for market between start and end interval
                    let mut interval_trades = select_ftx_trades_by_time(
                        pool,
                        &exchange.name,
                        market,
                        "rest",
                        interval_start,
                        interval_end,
                    )
                    .await
                    .expect("Could not fetch trades from db.");
                    // Check if there are interval trades, if there are: create candle and archive
                    // trades. If there are not, get previous trade details from previous candle
                    // and build candle from last
                    let new_candle = match interval_trades.len() {
                        0 => {
                            // Get previous candle
                            let previous_candle = select_previous_candle(
                                pool,
                                &exchange.name,
                                &market.market_id,
                                interval_start,
                            )
                            .await
                            .expect("No previous candle.");
                            // Create Candle
                            Candle::new_from_last(
                                market.market_id,
                                interval_start,
                                previous_candle.close,
                                previous_candle.last_trade_ts,
                                &previous_candle.last_trade_id.to_string(),
                            )
                        }
                        _ => {
                            // Sort and dedup
                            interval_trades.sort_by(|t1, t2| t1.id.cmp(&t2.id));
                            interval_trades.dedup_by(|t1, t2| t1.id == t2.id);
                            // Create Candle
                            Candle::new_from_trades(
                                market.market_id,
                                interval_start,
                                &interval_trades,
                            )
                        }
                    };
                    // Insert into candles
                    insert_candle(pool, &exchange.name, &market.market_id, new_candle, false)
                        .await
                        .expect("Could not insert new candle.");
                    println!("Candle {:?} inserted", interval_start);
                    // Delete trades for market between start and end interval
                    insert_delete_ftx_trades(
                        pool,
                        &exchange.name,
                        market,
                        interval_start,
                        interval_end,
                        "rest",
                        "processed",
                        interval_trades,
                    )
                    .await
                    .expect("Failed to insert and delete ftx trades.");
                };
                // Move to next interval start
                interval_start = interval_start + Duration::seconds(900); // TODO! set to market heartbeat
            };
        }
    }
}

pub async fn get_ftx_start(client: &RestClient, market: &MarketDetail) -> DateTime<Utc> {
    // Set end time to floor of today's date, start time to 90 days prior. Check each day if there
    // are trades and return the start date - 1 that first returns trades
    let end_time = Utc::now().duration_trunc(Duration::days(1)).unwrap();
    let mut start_time = end_time - Duration::days(90);
    while start_time < end_time {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let new_trades = client
            .get_ftx_trades(market.market_name.as_str(), Some(1), None, Some(start_time))
            .await
            .expect("Failed to get trades.");
        println!("New Trades: {:?}", new_trades);
        if !new_trades.is_empty() {
            return start_time;
        } else {
            start_time = start_time + Duration::days(1)
        };
    }
    start_time
}

pub async fn get_gdax_start(
    client: &RestClient,
    market: &MarketDetail,
) -> (Option<DateTime<Utc>>, Option<i64>) {
    // Get latest trade to establish current trade id and trade ts
    let exchange_trade = client
        .get_gdax_trades(&market.market_name, Some(1), None, None)
        .await
        .expect("Failed to get gdax trade.")
        .pop()
        .unwrap();
    let start_ts = Utc::now().duration_trunc(Duration::days(1)).unwrap() - Duration::days(91);
    let mut first_trade_id = 0_i64;
    let mut first_trade_ts = Utc.ymd(2017, 1, 1).and_hms(0, 0, 0);
    let mut last_trade_id = exchange_trade.trade_id();
    let mut mid = 1001_i64; // Once mid is < 1k, return that trade
    while first_trade_ts < start_ts - Duration::days(1) || first_trade_ts > start_ts {
        mid = (first_trade_id + last_trade_id) / 2;
        if mid < 1000 {
            break;
        };
        println!(
            "First / Mid / Last: {} {} {}",
            first_trade_id, mid, last_trade_id
        );
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let exchange_trade = client
            .get_gdax_trades(&market.market_name, Some(1), None, Some(mid))
            .await
            .expect("Failed to get gdax trade.")
            .pop()
            .unwrap();
        first_trade_ts = exchange_trade.time();
        if first_trade_ts < start_ts - Duration::days(1) {
            first_trade_id = mid;
        } else if first_trade_ts > start_ts {
            last_trade_id = mid;
        };
    }
    println!("Final start: {} {}", mid, first_trade_ts);
    (Some(first_trade_ts), Some(mid))
}

#[cfg(test)]
mod tests {
    use super::get_gdax_start;
    use crate::configuration::get_configuration;
    use crate::exchanges::{client::RestClient, ExchangeName};
    use crate::markets::select_market_details;
    use sqlx::PgPool;

    #[tokio::test]
    pub async fn get_gdax_start_old_asset_returns_90d() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Create rest client
        let client = RestClient::new(&ExchangeName::Gdax);
        // Select old asset (BTC or ETH) and run get gdax start
        let market_details = select_market_details(&pool)
            .await
            .expect("Failed to select market details.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "BTC-USD")
            .unwrap();
        // Get gdax start
        println!("Getting GDAX start for BTC-USD");
        let (id, ts) = get_gdax_start(&client, market).await;
        println!("ID / TS: {:?} {:?}", id, ts);
    }

    #[tokio::test]
    pub async fn get_gdax_start_new_asset_returns_first_day() {
        // Load configuration
        let configuration = get_configuration().expect("Failed to read configuration.");
        println!("Configuration: {:?}", configuration);
        // Create db connection
        let pool = PgPool::connect_with(configuration.database.with_db())
            .await
            .expect("Failed to connect to Postgres.");
        // Create rest client
        let client = RestClient::new(&ExchangeName::Gdax);
        // Select old asset (BTC or ETH) and run get gdax start
        let market_details = select_market_details(&pool)
            .await
            .expect("Failed to select market details.");
        let market = market_details
            .iter()
            .find(|m| m.market_name == "ORCA-USD")
            .unwrap();
        // Get gdax start
        println!("Getting GDAX start for ORCA-USD");
        let (id, ts) = get_gdax_start(&client, market).await;
        println!("ID / TS: {:?} {:?}", id, ts);
    }
}
