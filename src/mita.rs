use crate::{
    candles::ProductionCandle,
    configuration::Database,
    eldorado::{ElDorado, ElDoradoError},
    exchanges::ExchangeName,
    markets::MarketDetail,
    metrics::ResearchMetric,
    trades::PrIdTi,
    utilities::{DateRange, TimeFrame},
};
use chrono::{DateTime, DurationRound, Utc};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Heartbeat {
    pub ts: DateTime<Utc>,
    pub last: PrIdTi,
    pub candles: HashMap<TimeFrame, Vec<ProductionCandle>>,
    pub metrics: Option<Vec<ResearchMetric>>,
}

impl Heartbeat {
    pub fn new() -> Self {
        Self {
            ts: DateTime::<Utc>::MIN_UTC,
            last: PrIdTi::min(),
            candles: HashMap::with_capacity(TimeFrame::tfs().len()),
            metrics: None,
        }
    }
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl ElDorado {
    // Run Mita instance.
    // Stream trades from websocket to trade tables.
    // Fill trades from start to current websocket stream
    // On candle interval, create candle and metrics
    pub async fn mita(&mut self) -> Result<(), ElDoradoError> {
        // Set restart value to false, error handling must explicitly set back to true
        self.instance.restart = false;
        self.initialize_mita().await?;
        // self.instance.update_status(&InstanceStatus::New).await;
        tokio::select! {
            res2 = self.stream() => res2,
            res1 = self.sync_and_run_mita() => res1,
        }
    }

    async fn initialize_mita(&self) -> Result<(), ElDoradoError> {
        // Create any candle schemas that are needed
        match &self.markets.first().unwrap().exchange_name {
            ExchangeName::Ftx | ExchangeName::FtxUs => {
                self.create_candles_schema(&self.pools[&Database::Ftx])
                    .await?
            }
            ExchangeName::Gdax => {
                self.create_candles_schema(&self.pools[&Database::Gdax])
                    .await?
            }
        };
        // Delete metrics for markets
        for market in self.markets.iter() {
            ResearchMetric::delete_by_market(&self.pools[&Database::ElDorado], market).await?;
        }
        Ok(())
    }

    // Sync by determiniting the last good state and filling trades from that state to the current
    // first streamed trade. Then and Run Mita loop
    async fn sync_and_run_mita(&self) -> Result<(), ElDoradoError> {
        // Wait for 5 seconds for stream to start
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        // self.instance.update_status(&InstanceStatus::Sync).await;
        // Sync candles from start to current time frame
        let mut heartbeats: HashMap<String, Heartbeat> = HashMap::new();
        self.sync(&mut heartbeats).await?;
        println!("Starting MITA loop.");
        // self.instance.update_status(&InstanceStatus::Active).await;
        self.run_mita(&mut heartbeats).await
    }

    // Run the Mita instance - at each interval for each market - aggregate trades into new candle
    // resample if needed and publish metrics with new candle datapoint
    async fn run_mita(
        &self,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) -> Result<(), ElDoradoError> {
        loop {
            // Set loop timestamp
            let dt = Utc::now();
            // For each market, check if loop datetime is greater than market heartbeat
            for market in self.markets.iter() {
                if let Some(end) =
                    self.check_interval(market, &heartbeats[&market.market_name], &dt)
                {
                    self.process_interval(market, heartbeats, &end).await?;
                }
            }
            // Reload heartbeats if needed (ie when a candle validation is updated)
            // Sleep for 200 ms to give control back to tokio scheduler
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }
    }

    // Check if there is a new interval to process. This can occur in two scenarios:
    // 1) The new timestamp is greater than the heartbeat timestamp + one interval.
    // 2) There is no metrics in the heartbeat
    fn check_interval(
        &self,
        market: &MarketDetail,
        heartbeat: &Heartbeat,
        dt: &DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        let trunc_dt = dt.duration_trunc(market.tf.as_dur()).unwrap();
        if trunc_dt > heartbeat.ts + market.tf.as_dur() {
            println!(
                "{} - Checking interval.\tTrunc dt: {}\tHeartbeat TS: {}",
                Utc::now(),
                trunc_dt,
                heartbeat.ts
            );
            println!(
                "New heartbeat interval for {}: {}",
                market.market_name, trunc_dt
            );
            println!("Returning {} for 'process_interval'", trunc_dt);
            Some(trunc_dt)
        } else if heartbeat.metrics.is_none() {
            println!("No metrics, calculating from current heartbeat.");
            println!("Returning {} for 'process_interval'", trunc_dt);
            Some(trunc_dt)
        } else {
            None
        }
    }

    async fn process_interval(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
        interval_end: &DateTime<Utc>,
    ) -> Result<(), ElDoradoError> {
        // Check first if the metrics is empty or not - inital run will have empty metric, then
        // Create date range of intervals to process - most of the time this will be for one
        // interval but may involve multiple intervals if the sync is long
        if heartbeats
            .get(&market.market_name)
            .unwrap()
            .metrics
            .is_some()
        {
            self.process_interval_new_interval(market, heartbeats, interval_end)
                .await?
        } else {
            self.process_interval_no_metrics(market, heartbeats).await?
        }
        Ok(())
    }

    async fn process_interval_new_interval(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
        interval_end: &DateTime<Utc>,
    ) -> Result<(), ElDoradoError> {
        println!(
            "Process new interval for {} with dt {}.",
            market.market_name, interval_end
        );
        let interval_start = heartbeats.get(&market.market_name).unwrap().ts + market.tf.as_dur();
        if let Some(dr) = DateRange::new(&interval_start, interval_end, &market.tf) {
            println!("Process interval dr: {:?}", dr);
            // There are intervals to process, process then run metrics
            if let Some(candles) = self
                .make_production_candles_for_interval(
                    market,
                    &dr,
                    &heartbeats.get(&market.market_name).unwrap().last,
                )
                .await?
            {
                println!("Inserting {} production candles.", candles.len());
                self.insert_production_candles(market, &candles).await?;
                println!("Updating heartbeat.");
                self.update_heartbeat(market, heartbeats, candles, interval_end)
                    .await?;
            }
        }
        Ok(())
    }

    async fn process_interval_no_metrics(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
    ) -> Result<(), ElDoradoError> {
        // There are no intervals to process but metrics need to be updated
        println!("Updating and inserting metrics.");
        let metrics = self.calc_metrics_all_tfs(market, heartbeats);
        self.insert_metrics(&metrics).await?;
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| hb.metrics = Some(metrics));
        Ok(())
    }

    async fn update_heartbeat(
        &self,
        market: &MarketDetail,
        heartbeats: &mut HashMap<String, Heartbeat>,
        mut candles: Vec<ProductionCandle>,
        interval_end: &DateTime<Utc>,
    ) -> Result<(), ElDoradoError> {
        // Create hashmap for timeframes and candles
        let last = candles.last().expect("Expected candle in Vec.");
        let last_ts = last.datetime;
        let last_pridti = last.close_as_pridti();
        println!("Appending new candles to base candles in heartbeat.");
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| {
                hb.candles
                    .entry(market.tf)
                    .and_modify(|v| v.append(&mut candles));
            });
        // Start metrics vec
        println!("Creating metrics for base tf.");
        let mut metrics = vec![ResearchMetric::new(
            market,
            market.tf,
            heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(&market.tf)
                .unwrap(),
        )];
        // For each time frame - either append new candle for interval or clone existing
        for tf in TimeFrame::tfs().iter().skip(1) {
            let hb_last = heartbeats
                .get(&market.market_name)
                .unwrap()
                .candles
                .get(tf)
                .unwrap()
                .last()
                .unwrap();
            if hb_last.datetime + tf.as_dur() < interval_end.duration_trunc(tf.as_dur()).unwrap() {
                // Resample new candles to tf from base_tf and add to tf candles
                // Assumes tf is divisible by base tf
                println!("Filtering new candles for {} tf.", tf);
                let new_candles: Vec<_> = heartbeats
                    .get(&market.market_name)
                    .unwrap()
                    .candles
                    .get(&market.tf)
                    .unwrap()
                    .iter()
                    .filter(|c| {
                        c.datetime >= hb_last.datetime + tf.as_dur()
                            && c.datetime < interval_end.duration_trunc(tf.as_dur()).unwrap()
                    })
                    .cloned()
                    .collect();
                println!(
                    "Resampling {} new base candles for {} tf",
                    new_candles.len(),
                    tf
                );
                let mut resampled_candles = self.resample_production_candles(&new_candles, tf);
                println!("Filtered new candles for {}: {:?}", tf, new_candles);
                println!("{} new {} resampled candles.", resampled_candles.len(), tf);
                println!("Appending resampled candles to candles.");
                heartbeats
                    .entry(market.market_name.clone())
                    .and_modify(|hb| {
                        hb.candles
                            .entry(*tf)
                            .and_modify(|v| v.append(&mut resampled_candles));
                    });
                // Calc metrics on new candle vec
                println!("Creating metrics for {} tf.", tf);
                metrics.push(ResearchMetric::new(
                    market,
                    *tf,
                    heartbeats
                        .get(&market.market_name)
                        .unwrap()
                        .candles
                        .get(tf)
                        .unwrap(),
                ));
            } else {
                // // Interval end does not create new interval for timeframe
                // println!("Interval end of {} does not create new interval for {}. Last candle datetime for {}: {}",
                //     )
                println!("No resample for {} tf.", tf);
            }
        }
        // Insert metrics to db
        println!("Inserting {} metrics into db", metrics.len());
        self.insert_metrics(&metrics).await?;
        // Update the market last candle
        println!("Update market last candle dt.");
        market
            .update_last_candle(&self.pools[&Database::ElDorado], &last_ts)
            .await?;
        // Updateing the new heartbeat
        println!("Updating heartbeat with new metrics.");
        heartbeats
            .entry(market.market_name.clone())
            .and_modify(|hb| {
                hb.metrics = Some(metrics);
                hb.last = last_pridti;
                hb.ts = last_ts;
            });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO: Add unit tests
}
