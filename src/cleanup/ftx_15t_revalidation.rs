use sqlx::PgPool;

// Pull all 01d candles that are left after the previous cleanups. The 01d that are not validated
// have all 15t validated candles but were validated using a 1bps tolerance instead of the an exact
// match. This cleanup script will grab all current 01d candles that are not validated. Set all
// associated 15t candles to not validated and the trade count to -1. It will also delete all trades
// that are associated with the 15t candles. Finally it will call the
// previous cleanup scripts to clean up the 15t candles and 01d candle validations.