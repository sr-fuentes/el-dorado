-- Add migration script here
-- Remove un-needed columns and add new columns
ALTER TABLE IF EXISTS markets
DROP COLUMN first_validated_trade_id;

ALTER TABLE IF EXISTS markets
DROP COLUMN first_validated_trade_ts;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_validated_trade_id;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_validated_trade_ts;

ALTER TABLE IF EXISTS markets
DROP COLUMN candle_base_interval;

ALTER TABLE IF EXISTS markets
DROP COLUMN candle_base_in_seconds;

ALTER TABLE IF EXISTS markets
DROP COLUMN first_validated_candle;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_validated_candle;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_update_ts;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_update_ip_address;

ALTER TABLE IF EXISTS markets
DROP COLUMN first_candle;

ALTER TABLE IF EXISTS markets
DROP COLUMN last_candle;

ALTER TABLE IF EXISTS markets
ADD COLUMN candle_timeframe TEXT;

ALTER TABLE IF EXISTS markets
ADD COLUMN last_candle timestamptz;
