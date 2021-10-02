-- Add migration script here
/*
Alter the candles tables to add two fields:
    first_trade_ts
    first_trade_id
This will improve select and delete queries for certain exchanges on trades
processed and validated.
Make trade count field not null
Drop 01D candle and re-create with correct fields
*/

ALTER TABLE IF EXISTS candles_15t_ftx
ADD COLUMN first_trade_ts timstamptz NULL;

ALTER TABLE IF EXISTS candles_15t_ftx
ADD COLUMN first_trade_id TEXT NULL;

ALTER TABLE IF EXISTS candles_15t_ftx
ALTER COLUMN trade_count SET NOT NULL;

ALTER TABLE IF EXISTS candles_15t_ftxus
ADD COLUMN first_trade_ts timstamptz NULL;

ALTER TABLE IF EXISTS candles_15t_ftxus
ADD COLUMN first_trade_id TEXT NULL;

ALTER TABLE IF EXISTS candles_15t_ftxus
ALTER COLUMN trade_count SET NOT NULL;

DROP TABLE IF EXISTS candles_01d;

CREATE TABLE IF NOT EXISTS candles_01d (
    datetime timestamptz NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    volume_net NUMERIC NOT NULL,
    volume_liquidation NUMERIC NOT NULL,
    value NUMERIC NOT NULL,
    trade_count BIGINT NOT NULL,
    liquidation_count BIGINT NOT NULL,
    last_trade_ts timestamptz NOT NULL,
    last_trade_id TEXT NOT NULL,
    is_validated BOOLEAN NOT NULL,
    market_id uuid NOT NULL,
    first_trade_ts timestamptz NOT NULL,
    last_trade_id TEXT NOT NULL,
    PRIMARY KEY (datetime, market_id)
);