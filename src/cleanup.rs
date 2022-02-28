use crate::candles::{insert_candle, Candle, TimeFrame};
use crate::exchanges::{gdax::Trade, ExchangeName};
use crate::inquisidor::Inquisidor;
use crate::markets::{select_market_details_by_status_exchange, MarketStatus};
use crate::trades::{insert_gdax_trades, select_gdax_trades_by_table};
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
            .execute(&self.pool)
            .await
            .expect("Failed to delete gdax candle_validations.");
        println!("Gdax candle_validations deleted.");

        // 2) For each GDAX market:
        let gdax_markets = select_market_details_by_status_exchange(
            &self.pool,
            &ExchangeName::Gdax,
            &MarketStatus::Backfill,
        )
        .await
        .expect("Failed to select gdax markets.");

        for market in gdax_markets.iter() {
            if market.market_name != "MANA-USD" {
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
                .execute(&self.pool)
                .await
                .expect("Failed to delete 01d candles.");
            println!("01d candles deleted.");

            // b) Migrate all validated trades to processed
            println!("Loading all validated trades.");
            let table = format!("trades_gdax_{}_validated", market.as_strip());
            let validated_trades = select_gdax_trades_by_table(&self.pool, &table)
                .await
                .expect("Failed to select trades from validated_table.");
            println!(
                "{} Validated trades loaded. Inserting into procesesing.",
                validated_trades.len()
            );
            insert_gdax_trades(
                &self.pool,
                &ExchangeName::Gdax,
                market,
                "processed",
                validated_trades,
            )
            .await
            .expect("Failed to insert trades into processed.");
            println!("Trades inserted to procesed.");

            // c) Delete all hb candles
            let sql = r#"
                DELETE FROM candles_15t_gdax
                WHERE market_id = $1
                "#;
            println!("Deleting the hb candles.");
            sqlx::query(sql)
                .bind(market.market_id)
                .execute(&self.pool)
                .await
                .expect("Failed to delete gdax candle_validations.");
            println!("Deleted hb candles for {}", market.market_name);

            // d) Select all trades from processed and create candles for date range
            println!("Sleeping for 5 seconds to let trade inserts and deletes complete.");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            println!("Loading all procssed trades.");
            let table = format!("trades_gdax_{}_processed", market.as_strip());
            let mut trades = select_gdax_trades_by_table(&self.pool, &table)
                .await
                .expect("Failed to select trades from processed table.");
            println!("Loaded all processed trades.");

            // e) Create new hb candles from processed trades
            trades.sort_by(|t1, t2| t1.trade_id.cmp(&t2.trade_id));
            let first_trade = trades.first().unwrap();
            let last_trade = trades.last().unwrap();
            println!(
                "Creating new hb candles for trades starting {:?} to {:?}",
                first_trade.time, last_trade.time
            );
            let mut can_start = first_trade
                .time
                .duration_trunc(TimeFrame::T15.as_dur())
                .unwrap()
                + TimeFrame::T15.as_dur();
            let can_end = last_trade
                .time
                .duration_trunc(TimeFrame::T15.as_dur())
                .unwrap();
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
            let mut previous_candle: Option<Candle> = None;
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
                    &self.pool,
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
        println!("GDAX cleanup complete.");
    }
}
