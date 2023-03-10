// TODO: Make candles for months and zip / compress trades
use crate::{
    configuration::Database,
    eldorado::ElDorado,
    markets::{MarketArchiveDetail, MarketCandleDetail, MarketDetail, MarketStatus},
    utilities::{DateRange, TimeFrame},
};
use chrono::{DateTime, Duration, DurationRound, Utc};

impl ElDorado {
    pub async fn archive(&self, market: &Option<MarketDetail>) {
        let markets = match market {
            Some(m) => {
                if m.status == MarketStatus::Active {
                    Some([m.clone()].to_vec())
                } else {
                    None
                }
            }
            None => self.select_markets_eligible_for_archive().await,
        };
        // Check there are markets to archive
        if let Some(m) = markets {
            // For each market: pick up from last state and archive trades and candles
            for market in m.iter() {
                self.archive_market(market).await;
            }
        }
    }

    async fn archive_market(&self, market: &MarketDetail) {
        // Check if there is a market archive detail
        match MarketArchiveDetail::select(&self.pools[&Database::ElDorado], market).await {
            // Yes - proceed from the market archive detail information
            Ok(mad) => self.archive_months(market, &None, &mad).await,
            Err(sqlx::Error::RowNotFound) => {
                // Check if there is a market candle detail
                match MarketCandleDetail::select(&self.pools[&Database::ElDorado], market).await {
                    Ok(mcd) => {
                        let mad = self.create_mad_and_archive_first_month(market, &mcd).await;
                        match mad {
                            Some(m) => self.archive_months(market, &Some(mcd), &m).await,
                            None => {
                                println!("Not enought trading days to start first month.");
                            }
                        }
                    }
                    Err(sqlx::Error::RowNotFound) => {
                        println!(
                            "No MCD found for {}. No months to archive.",
                            market.market_name
                        );
                    }
                    Err(e) => panic!("SQLX Error: {:?}", e),
                }
            }
            Err(e) => panic!("SQLX Error: {:?}", e),
        }
    }

    async fn archive_months(
        &self,
        market: &MarketDetail,
        mcd: &Option<MarketCandleDetail>,
        mad: &MarketArchiveDetail,
    ) {
        let mut mad = mad.to_owned();
        let mcd = match mcd {
            Some(m) => m.clone(),
            None => MarketCandleDetail::select(&self.pools[&Database::ElDorado], market)
                .await
                .expect("Failed to select mcd."),
        };
        // Determine months to archive and put in date range
        match DateRange::new_monthly(&mad.next_month, &ElDorado::trunc_month_dt(&mcd.last_candle)) {
            Some(dr) => {
                // For each month - load trade / make candles / write files
                for d in dr.dts.iter() {
                    mad = self.archive_month(market, &Some(mad), d).await;
                }
            }
            None => println!("No more months to archive."),
        }
    }

    async fn archive_month(
        &self,
        market: &MarketDetail,
        mad: &Option<MarketArchiveDetail>,
        dt: &DateTime<Utc>,
    ) -> MarketArchiveDetail {
        // Get trade and candle date ranges
        let next_month = ElDorado::next_month_dt(dt);
        let candle_dr = DateRange::new(dt, &next_month, &TimeFrame::S15).unwrap();
        let trade_days_dr = DateRange::new(dt, &next_month, &TimeFrame::D01).unwrap();
        // Make candles for month
        let candles = self.make_research_candles_for_month(market, mad, &candle_dr, &trade_days_dr);
        // Write candles for month
        self.write_research_candles_to_file_for_month(market, dt, &candles);
        // Update mad
        mad.as_ref()
            .unwrap()
            .update(
                &self.pools[&Database::ElDorado],
                &next_month,
                candles.last().unwrap(),
            )
            .await
            .expect("Failed to update mad.")
    }

    async fn create_mad_and_archive_first_month(
        &self,
        market: &MarketDetail,
        mcd: &MarketCandleDetail,
    ) -> Option<MarketArchiveDetail> {
        let next_month = ElDorado::next_month_dt(&mcd.first_candle);
        let current_month = ElDorado::trunc_month_dt(&mcd.last_candle);
        // Check that there are candles through the entire month
        if next_month <= current_month {
            let mad = self.archive_first_month(market, mcd, &next_month).await;
            // Insert and return mad
            mad.insert(&self.pools[&Database::ElDorado])
                .await
                .expect("Failed to insert mad.");
            Some(mad)
        } else {
            None
        }
    }

    async fn archive_first_month(
        &self,
        market: &MarketDetail,
        mcd: &MarketCandleDetail,
        next_month: &DateTime<Utc>,
    ) -> MarketArchiveDetail {
        let this_month = ElDorado::trunc_month_dt(&mcd.first_candle);
        // Make candle dr from first candle in mcd to begining of next month
        let candle_dr = DateRange::new(&mcd.first_candle, next_month, &TimeFrame::S15).unwrap();
        // Make dr for days to load trade files
        let trade_days_dr = DateRange::new(
            &mcd.first_candle.duration_trunc(Duration::days(1)).unwrap(),
            next_month,
            &TimeFrame::D01,
        )
        .unwrap();
        // Make the candles for the first month
        let candles =
            self.make_research_candles_for_month(market, &None, &candle_dr, &trade_days_dr);
        // Write the candles to file
        self.write_research_candles_to_file_for_month(market, &this_month, &candles);
        // Make the MAD and return it
        let mad = MarketArchiveDetail::new(
            market,
            &TimeFrame::S15,
            candles.first().unwrap(),
            candles.last().unwrap(),
        );
        mad
    }
}
