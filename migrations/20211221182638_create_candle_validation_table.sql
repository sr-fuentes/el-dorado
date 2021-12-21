-- Add migration script here
-- Create table to hold candles that need re-validation, either automatic
-- or manual
CREATE TABLE candle_validations (
    exchange_name TEXT NOT NULL,
    market_name TEXT NOT NULL,
    datetime timestamptz NOT NULL,
    duration BIGINT NOT NULL,
    validation_type TEXT NOT NULL,
    created_ts timestamptz NOT NULL,
    processed_ts timestamptz,
    validation_status TEXT NOT NULL,
    notes TEXT,
    PRIMARY KEY (exchange_name, market_name, datetime)
);

