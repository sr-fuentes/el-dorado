use crate::candles::{
    create_01d_candles, insert_candle, select_candles, select_previous_candle,
    select_unvalidated_candles, validate_01d_candles, validate_hb_candles, Candle, DailyCandle,
    TimeFrame,
};
use crate::events::{Event, EventStatus, EventType};
use crate::exchanges::{gdax::Trade, insert_new_exchange, select_exchanges, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{
    insert_market_rank, select_market_detail_by_exchange_mita, select_market_details,
    select_market_details_by_status_exchange, select_market_ranks, MarketStatus,
};
use crate::trades::{
    create_ftx_trade_table, create_gdax_trade_table, drop_trade_table, insert_ftx_trades,
    insert_gdax_trades, select_ftx_trades_by_table, select_gdax_trades_by_time,
};
use crate::validation::{CandleValidation, ValidationStatus, ValidationType};
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

    pub async fn migrate_static_tables(self) {
        // Migrate the tables that are not updated frequently first
        // candles_01d, exchanges, market_ranks_ftx, markets
        // Start with candles_01d
        println!("Migrating candles_01d.");
        let sql = r#"
            SELECT * FROM candles_01d
            "#;
        let candles_01d = sqlx::query_as::<_, DailyCandle>(sql)
            .fetch_all(&self.ig_pool)
            .await
            .expect("Failed select daily candles.");
        let sql = r#"
            INSERT INTO candles_01d (
                datetime, open, high, low, close, volume, volume_net, volume_liquidation, value,
                trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated,
                market_id, first_trade_ts, first_trade_id, is_archived, is_complete)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
                $16, $17, $18, $19)
            "#;
        for candle in candles_01d.iter() {
            sqlx::query(sql)
                .bind(candle.datetime)
                .bind(candle.open)
                .bind(candle.high)
                .bind(candle.low)
                .bind(candle.close)
                .bind(candle.volume)
                .bind(candle.volume_net)
                .bind(candle.volume_liquidation)
                .bind(candle.value)
                .bind(candle.trade_count)
                .bind(candle.liquidation_count)
                .bind(candle.last_trade_ts)
                .bind(&candle.last_trade_id)
                .bind(candle.is_validated)
                .bind(candle.market_id)
                .bind(candle.first_trade_ts)
                .bind(&candle.first_trade_id)
                .bind(candle.is_archived)
                .bind(candle.is_complete)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to insert daily candle.");
        }
        // Exchanges
        println!("Migrating exchanges.");
        let exchanges = select_exchanges(&self.ig_pool)
            .await
            .expect("Failed to select exchanges.");
        for exchange in exchanges.iter() {
            insert_new_exchange(&self.ig_pool, exchange)
                .await
                .expect("Failed to insert exchange.");
        }
        // market_ranks_ftx
        println!("Migrating market ranks.");
        let market_ranks = select_market_ranks(&self.ig_pool, &ExchangeName::Ftx)
            .await
            .expect("Failed to select market ranks ftx.");
        for mr in market_ranks.iter() {
            insert_market_rank(&self.ig_pool, &ExchangeName::Ftx, mr)
                .await
                .expect("Failed to insert market rank ftx.");
        }
        // markets
        println!("Migrating markets.");
        let markets = select_market_details(&self.ig_pool)
            .await
            .expect("Failed to select market details.");
        let sql = r#"
            INSERT INTO markets (
                market_id, exchange_name, market_name, market_type, base_currency,
                quote_currency, underlying, market_status, market_data_status,
                first_validated_trade_id, first_validated_trade_ts,
                last_validated_trade_id, last_validated_trade_ts,
                candle_base_interval, candle_base_in_seconds,
                first_validated_candle, last_validated_candle,
                last_update_ts, last_update_ip_address,
                first_candle, last_candle, mita)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20, $21, $22)    
            "#;
        for market in markets.iter() {
            sqlx::query(sql)
                .bind(market.market_id)
                .bind(market.exchange_name.as_str())
                .bind(&market.market_name)
                .bind(&market.market_type)
                .bind(&market.base_currency)
                .bind(&market.quote_currency)
                .bind(&market.underlying)
                .bind(market.market_status.as_str())
                .bind(market.market_data_status.as_str())
                .bind(&market.first_validated_trade_id)
                .bind(market.first_validated_trade_ts)
                .bind(&market.last_validated_trade_id)
                .bind(market.last_validated_trade_ts)
                .bind(&market.candle_base_interval)
                .bind(market.candle_base_in_seconds)
                .bind(market.first_validated_candle)
                .bind(market.last_validated_candle)
                .bind(market.last_update_ts)
                .bind(market.last_update_ip_address)
                .bind(market.first_candle)
                .bind(market.last_candle)
                .bind(&market.mita)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to insert market.");
        }
    }

    pub async fn migrate_frequent_tables(self) {
        // Shut down all instances and migrate the frequently update tables
        // candle_validations, candles_15t_ftx, candles_15t_gdax, events
        println!("Migrating candle_validations.");
        let candle_validations = sqlx::query_as!(
            CandleValidation,
            r#"
            SELECT exchange_name as "exchange_name: ExchangeName",
                market_id, datetime, duration,
                validation_type as "validation_type: ValidationType",
                created_ts, processed_ts,
                validation_status as "validation_status: ValidationStatus",
                notes
            FROM candle_validations
            "#
        )
        .fetch_all(&self.ig_pool)
        .await
        .expect("Failed to select candle validations.");
        let sql = r#"
            INSERT INTO candle_validations (
                exchange_name, market_id, datetime, duration, validation_type, created_ts,
                processed_ts, validation_status, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (exchange_name, market_id, datetime, duration) DO NOTHING
            "#;
        for cv in candle_validations.iter() {
            sqlx::query(sql)
                .bind(cv.exchange_name.as_str())
                .bind(cv.market_id)
                .bind(cv.datetime)
                .bind(cv.duration)
                .bind(cv.validation_type.as_str())
                .bind(cv.created_ts)
                .bind(cv.processed_ts)
                .bind(cv.validation_status.as_str())
                .bind(&cv.notes)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to insert candle_validation.");
        }
        println!("Migrating ftx candles.");
        let sql = r#"
            SELECT * from candles_15t_ftx
            "#;
        let ftx_candles = sqlx::query_as::<_, Candle>(sql)
            .fetch_all(&self.ig_pool)
            .await
            .expect("Failed to select FTX candles.");
        let sql = r#"
                INSERT INTO candles_15T_ftx (
                    datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                    trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                    market_id, first_trade_ts, first_trade_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            "#;
        for candle in ftx_candles.iter() {
            sqlx::query(sql)
                .bind(candle.datetime)
                .bind(candle.open)
                .bind(candle.high)
                .bind(candle.low)
                .bind(candle.close)
                .bind(candle.volume)
                .bind(candle.volume_net)
                .bind(candle.volume_liquidation)
                .bind(candle.value)
                .bind(candle.trade_count)
                .bind(candle.liquidation_count)
                .bind(candle.last_trade_ts)
                .bind(&candle.last_trade_id)
                .bind(candle.is_validated)
                .bind(candle.market_id)
                .bind(candle.first_trade_ts)
                .bind(&candle.first_trade_id)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to insert ftx candle.");
        }
        println!("Migrating gdax candles.");
        let sql = r#"
            SELECT * from candles_15t_gdax
            "#;
        let ftx_candles = sqlx::query_as::<_, Candle>(sql)
            .fetch_all(&self.ig_pool)
            .await
            .expect("Failed to select GDAX candles.");
        let sql = r#"
                INSERT INTO candles_15T_gdax (
                    datetime, open, high, low, close, volume, volume_net, volume_liquidation, value, 
                    trade_count, liquidation_count, last_trade_ts, last_trade_id, is_validated, 
                    market_id, first_trade_ts, first_trade_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            "#;
        for candle in ftx_candles.iter() {
            sqlx::query(sql)
                .bind(candle.datetime)
                .bind(candle.open)
                .bind(candle.high)
                .bind(candle.low)
                .bind(candle.close)
                .bind(candle.volume)
                .bind(candle.volume_net)
                .bind(candle.volume_liquidation)
                .bind(candle.value)
                .bind(candle.trade_count)
                .bind(candle.liquidation_count)
                .bind(candle.last_trade_ts)
                .bind(&candle.last_trade_id)
                .bind(candle.is_validated)
                .bind(candle.market_id)
                .bind(candle.first_trade_ts)
                .bind(&candle.first_trade_id)
                .execute(&self.ig_pool)
                .await
                .expect("Failed to insert ftx candle.");
        }
        println!("Migrating events.");
        let events = sqlx::query_as!(
            Event,
            r#"
            SELECT event_id, droplet,
            event_type as "event_type: EventType",
            exchange_name as "exchange_name: ExchangeName",
            market_id, start_ts, end_ts, event_ts, created_ts, processed_ts,
            event_status as "event_status: EventStatus", 
            notes
        FROM events
        "#
        )
        .fetch_all(&self.ig_pool)
        .await
        .expect("Failed to fetch events.");
        let sql = r#"
            INSERT INTO events (
                event_id, droplet, event_type, exchange_name, market_id, start_ts, end_ts, event_ts, created_ts,
                processed_ts, event_status, notes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#;
        for event in events.iter() {
            sqlx::query(sql)
                .bind(event.event_id)
                .bind(&event.droplet)
                .bind(event.event_type.as_str())
                .bind(event.exchange_name.as_str())
                .bind(event.market_id)
                .bind(event.start_ts)
                .bind(event.end_ts)
                .bind(event.event_ts)
                .bind(event.created_ts)
                .bind(event.processed_ts)
                .bind(event.event_status.as_str())
                .bind(&event.notes)
                .execute(&self.ig_pool)
                .await
                .expect("failed to insert event");
        }
    }
}
