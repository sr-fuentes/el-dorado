-- Add migration script here
-- Add duration to composite primary key

ALTER TABLE candle_validations
DROP CONSTRAINT candle_validations_pkey;

ALTER TABLE candle_validations
ADD PRIMARY KEY (exchange_name, market_id, datetime, duration);
