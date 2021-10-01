-- Add migration script here
/*
Trades tables were initially set up with no primary keys or index.
Only ftx and ftxus exchanges impacted as those two are the only exchanges.
*/

ALTER TABLE IF EXISTS trades_ftxus_rest
ADD PRIMARY KEY (trade_id);

ALTER TABLE IF EXISTS trades_ftxus_ws
ADD PRIMARY KEY (trade_id);

ALTER TABLE IF EXISTS trades_ftxus_processed
ADD PRIMARY KEY (trade_id);

CREATE INDEX IF EXISTS trades_ftxus_processed_market_id_asc
ON trades_ftxus_processed(market_id);

CREATE INDEX IF EXISTS trades_ftxus_processed_time_asc
ON trades_ftxus_processed(time);

ALTER TABLE IF EXISTS trades_ftxus_validated
ADD PRIMARY KEY (trade_id);

CREATE INDEX IF EXISTS trades_ftxus_validated_market_id_asc
ON trades_ftxus_validated(market_id);

CREATE INDEX IF EXISTS trades_ftxus_validated_time_asc
ON trades_ftxus_validated(time);

ALTER TABLE IF EXISTS trades_ftx_rest
ADD PRIMARY KEY (trade_id);

ALTER TABLE IF EXISTS trades_ftx_ws
ADD PRIMARY KEY (trade_id);

ALTER TABLE IF EXISTS trades_ftx_processed
ADD PRIMARY KEY (trade_id);

CREATE INDEX IF EXISTS trades_ftx_processed_market_id_asc
ON trades_ftxus_processed(market_id);

CREATE INDEX IF EXISTS trades_ftx_processed_time_asc
ON trades_ftxus_processed(time);

ALTER TABLE IF EXISTS trades_ftx_validated
ADD PRIMARY KEY (trade_id);

CREATE INDEX IF EXISTS trades_ftx_validated_market_id_asc
ON trades_ftxus_validated(market_id);

CREATE INDEX IF EXISTS trades_ftx_validated_time_asc
ON trades_ftxus_validated(time);

