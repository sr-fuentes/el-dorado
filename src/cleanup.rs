use crate::candles::{
    create_01d_candles, insert_candle, select_candles, select_previous_candle,
    select_unvalidated_candles, validate_01d_candles, validate_hb_candles, Candle, TimeFrame,
};
use crate::exchanges::{gdax::Trade, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    select_market_detail_by_exchange_mita, select_market_details_by_status_exchange, MarketStatus,
};
use crate::trades::{
    create_ftx_trade_table, create_gdax_trade_table, drop_trade_table, insert_ftx_trades,
    insert_gdax_trades, select_ftx_trades_by_table,
    select_gdax_trades_by_time,
};
use chrono::DurationRound;

impl Inquisidor {
    pub async fn cleanup_gdax(self) {
        // Cleanup gdax candles. There was a defect where candles selected for a timeframe where not
        // sorted by id so the first and last trades and open and close trades are not 100%
        // accurate.

        // 1) Delete all gdax candle validations
        println!("Deleting gdax candle_validations.");
        let sql = r#"
            DELETE FROM candle_validations
            WHERE exchange_name = 'gdax'
            "#;
        sqlx::query(sql)
            .execute(&self.ig_pool)
            .await
            .expect("Failed to delete gdax candle_validations.");
        println!("Gdax candle_validations deleted.");

        // 2) For each GDAX market:
        let gdax_markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &ExchangeName::Gdax,
            &MarketStatus::Backfill,
        )
        .await
        .expect("Failed to select gdax markets.");

        for market in gdax_markets.iter() {
            if market.market_name == "MANA-USD" {
                continue;
            };
            println!("Cleaning up {} candles.", market.market_name);

            // a) Delete 01d candles
            let sql = r#"
                DELETE FROM candles_01d
                WHERE market_id = $1
                "#;
            println!("Deleting 01d candles.");
            sqlx::query(sql)
                .bind(market.market_id)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to delete 01d candles.");
            println!("01d candles deleted.");

            // b) Migrate all validated trades to processed - process in 01d chunks
            // as loading all trades leads to memory and cpu max constraints
            // Load candles to get date range for days
            let candles =
                select_candles(&self.ig_pool, &ExchangeName::Gdax, &market.market_id, 900)
                    .await
                    .expect("Failed to select candles.");
            let mut first_day = candles
                .first()
                .unwrap()
                .datetime
                .duration_trunc(TimeFrame::D01.as_dur())
                .unwrap();
            let last_day = candles
                .last()
                .unwrap()
                .datetime
                .duration_trunc(TimeFrame::D01.as_dur())
                .unwrap();
            let mut dr = Vec::new();
            while first_day < last_day {
                dr.push(first_day);
                first_day = first_day + TimeFrame::D01.as_dur();
            }
            println!(
                "Processing validated trades in chunks for dr: {:?} to {:?}",
                dr.first(),
                dr.last().unwrap()
            );
            println!("Loading all validated trades by day.");
            for d in dr.iter() {
                let validated_trades = select_gdax_trades_by_time(
                    &self.ftx_pool, // Trade were in main db before split
                    &ExchangeName::Gdax,
                    market,
                    "validated",
                    *d,
                    *d + TimeFrame::D01.as_dur(),
                )
                .await
                .expect("Failed to select trades from validated_table.");
                println!(
                    "{} Validated trades loaded for {:?}. Inserting into procesesing.",
                    validated_trades.len(),
                    d,
                );
                insert_gdax_trades(
                    &self.ftx_pool,
                    &ExchangeName::Gdax,
                    market,
                    "processed",
                    validated_trades,
                )
                .await
                .expect("Failed to insert trades into processed.");
            }

            // c) Delete all hb candles
            let sql = r#"
                DELETE FROM candles_15t_gdax
                WHERE market_id = $1
                "#;
            println!("Deleting the hb candles.");
            sqlx::query(sql)
                .bind(market.market_id)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to delete gdax candle_validations.");
            println!("Deleted hb candles for {}", market.market_name);

            // c2) Delete all validated trades
            drop_trade_table(&self.ftx_pool, &ExchangeName::Gdax, market, "validated")
                .await
                .expect("Failed to drop trade table.");
            create_gdax_trade_table(&self.ftx_pool, &ExchangeName::Gdax, market, "validated")
                .await
                .expect("Faild to create trade table.");

            // d) Select all trades from processed and create candles for date range
            println!("Sleeping for 5 seconds to let trade inserts and deletes complete.");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            for d in dr.iter() {
                let mut trades = select_gdax_trades_by_time(
                    &self.ftx_pool,
                    &ExchangeName::Gdax,
                    market,
                    "processed",
                    *d,
                    *d + TimeFrame::D01.as_dur(),
                )
                .await
                .expect("Failed to select trades from processed table.");
                println!(
                    "Loaded {} processed trades for {:?} to make candles.",
                    trades.len(),
                    d
                );

                // e) Create new hb candles from processed trades
                trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
                let mut can_start = *d;
                let can_end = *d + TimeFrame::D01.as_dur();
                let mut date_range = Vec::new();
                while can_start < can_end {
                    date_range.push(can_start);
                    can_start = can_start + TimeFrame::T15.as_dur();
                }
                println!(
                    "Creating candles for dr from {:?} to {:?}",
                    date_range.first().unwrap(),
                    date_range.last().unwrap()
                );
                let mut previous_candle = match select_previous_candle(
                    &self.ig_pool,
                    &ExchangeName::Gdax,
                    &market.market_id,
                    *d,
                    TimeFrame::T15,
                )
                .await
                {
                    Ok(c) => Some(c),
                    Err(sqlx::Error::RowNotFound) => None,
                    Err(e) => panic!("Sqlx Error: {:?}", e),
                };
                let opt_candles = date_range.iter().fold(Vec::new(), |mut v, d| {
                    let mut filtered_trades: Vec<Trade> = trades
                        .iter()
                        .filter(|t| t.time.duration_trunc(TimeFrame::T15.as_dur()).unwrap() == *d)
                        .cloned()
                        .collect();
                    let new_candle = match filtered_trades.len() {
                        0 => previous_candle.as_ref().map(|pc| {
                            Candle::new_from_last(
                                market.market_id,
                                *d,
                                pc.close,
                                pc.last_trade_ts,
                                &pc.last_trade_id.to_string(),
                            )
                        }),
                        _ => {
                            filtered_trades.sort_by_key(|t1| t1.trade_id);
                            Some(Candle::new_from_trades(
                                market.market_id,
                                *d,
                                &filtered_trades,
                            ))
                        }
                    };
                    previous_candle = new_candle.clone();
                    v.push(new_candle);
                    v
                });
                let candles = opt_candles.into_iter().flatten().collect::<Vec<Candle>>();
                println!("Created {} candles to insert.", candles.len());
                for candle in candles.into_iter() {
                    insert_candle(
                        &self.ig_pool,
                        &ExchangeName::Gdax,
                        &market.market_id,
                        candle,
                        false,
                    )
                    .await
                    .expect("Could not insert new candle.");
                }
                println!("Inserted candles to db.");
            }
        }
        println!("GDAX cleanup complete.");
    }

    pub async fn cleanup_gdax_validate(self) {
        // Clean up has been done an all candles re-created from trades in the
        // _processed table.
        // Validate the daily candles, create 01d candles and validated them.
        let gdax_markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &ExchangeName::Gdax,
            &MarketStatus::Backfill,
        )
        .await
        .expect("Failed to select gdax markets.");
        for market in gdax_markets.iter() {
            println!("Validating {}.", market.market_name);
            let unvalidated_candles = select_unvalidated_candles(
                &self.ig_pool,
                &ExchangeName::Gdax,
                &market.market_id,
                TimeFrame::T15,
            )
            .await
            .expect("Could not fetch unvalidated candles.");
            println!("Validting hb candles.");
            // Validate heartbeat candles
            validate_hb_candles::<crate::exchanges::gdax::Candle>(
                &self.ig_pool,
                &self.gdax_pool,
                &self.clients[&ExchangeName::Gdax],
                &ExchangeName::Gdax,
                market,
                &self.settings,
                &unvalidated_candles,
            )
            .await;
            // Create 01d candles
            println!("Creating 01d candles");
            create_01d_candles(&self.ig_pool, &ExchangeName::Gdax, &market.market_id).await;
            // Validate 01d candles
            println!("Validating 01d candles.");
            validate_01d_candles::<crate::exchanges::gdax::Candle>(
                &self.ig_pool,
                &self.clients[&ExchangeName::Gdax],
                &ExchangeName::Gdax,
                market,
            )
            .await;
        }
    }

    pub async fn migrate_gdax_trades(self) {
        // Select all gdax markets that have trades. Drop any _rest or _ws tables in ftx db.
        // Then migrate all trades from _processed and validated from ftx to gdax db creating tables
        // as needed.
        let gdax_markets = select_market_details_by_status_exchange(
            &self.ig_pool,
            &ExchangeName::Gdax,
            &MarketStatus::Backfill,
        )
        .await
        .expect("Failed to select gdax markets.");
        for market in gdax_markets.iter() {
            println!("Migrating {} trades to gdax db.", market.market_name);
            println!("Dropping _rest and _ws table in old db.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "rest")
                .await
                .expect("Failed to drop rest table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "ws")
                .await
                .expect("Failed to drop ws table.");
            println!("Migrating _processed table.");
            create_ftx_trade_table(&self.ftx_pool, &ExchangeName::Ftx, market, "processed")
                .await
                .expect("Failed to create processed table.");
            println!("Loading _processed trades from old db.");
            let table = format!("trades_ftx_{}_processed", market.as_strip());
            let processed_trades = select_ftx_trades_by_table(&self.ig_pool, &table)
                .await
                .expect("Failed to select trades.");
            println!("Trades loaded. Inserting into new db.");
            insert_ftx_trades(
                &self.ftx_pool,
                &ExchangeName::Ftx,
                market,
                "processed",
                processed_trades,
            )
            .await
            .expect("Failed to insert trades to new db.");
            println!("Trades inserted. Dropping old table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "processed")
                .await
                .expect("Failed to drop rest table.");
            println!("Migrating _validated table.");
            create_ftx_trade_table(&self.ftx_pool, &ExchangeName::Ftx, market, "validated")
                .await
                .expect("Failed to create processed table.");
            println!("Loading _validated trades from old db.");
            let table = format!("trades_ftx_{}_validated", market.as_strip());
            let validated_trades = select_ftx_trades_by_table(&self.ig_pool, &table)
                .await
                .expect("Failed to select trades.");
            println!("Trades loaded. Inserting into new db.");
            insert_ftx_trades(
                &self.ftx_pool,
                &ExchangeName::Ftx,
                market,
                "validated",
                validated_trades,
            )
            .await
            .expect("Failed to insert trades to new db.");
            println!("Trades inserted. Dropping old table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "validated")
                .await
                .expect("Failed to drop rest table.");
        }
    }

    pub async fn migrate_ftx_trades(self) {
        // Select all gdax markets that have trades. Drop any _rest or _ws tables in ftx db.
        // Then migrate all trades from _processed and validated from ftx to gdax db creating tables
        // as needed.
        let ftx_markets =
            select_market_detail_by_exchange_mita(&self.ig_pool, &ExchangeName::Ftx, "mita-10")
                .await
                .expect("Failed to select gdax markets.");
        for market in ftx_markets.iter() {
            println!("Migrating {} trades to ftx db.", market.market_name);
            println!("Dropping _rest and _ws table in old db.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "rest")
                .await
                .expect("Failed to drop rest table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "ws")
                .await
                .expect("Failed to drop ws table.");
            println!("Migrating _processed table.");
            create_ftx_trade_table(&self.ftx_pool, &ExchangeName::Ftx, market, "processed")
                .await
                .expect("Failed to create processed table.");
            println!("Loading _processed trades from old db.");
            let table = format!("trades_ftx_{}_processed", market.as_strip());
            let processed_trades = select_ftx_trades_by_table(&self.ig_pool, &table)
                .await
                .expect("Failed to select trades.");
            println!("Trades loaded. Inserting into new db.");
            insert_ftx_trades(
                &self.ftx_pool,
                &ExchangeName::Ftx,
                market,
                "processed",
                processed_trades,
            )
            .await
            .expect("Failed to insert trades to new db.");
            println!("Trades inserted. Dropping old table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "processed")
                .await
                .expect("Failed to drop rest table.");
            println!("Migrating _validated table.");
            create_ftx_trade_table(&self.ftx_pool, &ExchangeName::Ftx, market, "validated")
                .await
                .expect("Failed to create processed table.");
            println!("Loading _validated trades from old db.");
            let table = format!("trades_ftx_{}_validated", market.as_strip());
            let validated_trades = select_ftx_trades_by_table(&self.ig_pool, &table)
                .await
                .expect("Failed to select trades.");
            println!("Trades loaded. Inserting into new db.");
            insert_ftx_trades(
                &self.ftx_pool,
                &ExchangeName::Ftx,
                market,
                "validated",
                validated_trades,
            )
            .await
            .expect("Failed to insert trades to new db.");
            println!("Trades inserted. Dropping old table.");
            drop_trade_table(&self.ig_pool, &ExchangeName::Ftx, market, "validated")
                .await
                .expect("Failed to drop rest table.");
        }
    }
}
