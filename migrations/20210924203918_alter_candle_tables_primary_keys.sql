-- Add migration script here
/*
Candle tables were initially set up with only the datetime as the primary
key rather than datetime AND market id. Only ftx and ftxus exchanges impacted
along with daily candle table
*/

ALTER TABLE IF EXISTS candles_15t_ftx
DROP CONSTRAINT candles_15t_ftx_pkey;

ALTER TABLE IF EXISTS candles_15t_ftx
ADD PRIMARY KEY (datetime, market_id);

ALTER TABLE IF EXISTS candles_15t_ftxus
DROP CONSTRAINT candles_15t_ftxus_pkey;

ALTER TABLE IF EXISTS candles_15t_ftxus
ADD PRIMARY KEY (datetime, market_id);

ALTER TABLE IF EXISTS candles_01d
DROP CONSTRAINT candles_01d_pkey;

ALTER TABLE IF EXISTS candles_01d
ADD PRIMARY KEY (datetime, market_id);

