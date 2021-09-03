-- Add migration script here
/*
Create Markets Table
This table will hold market data from multiple exchanges and save the state of validated
and raw trades and candles as well as metadata on the market and its status. Initial market
data will be fetched from exchange API and updated by the app.
*/
CREATE TABLE markets(
    market_id uuid NOT NULL,
    PRIMARY KEY (market_id),
    exchange_name TEXT NOT NULL,
    market_name TEXT NOT NULL,
    market_type TEXT NOT NULL,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    underlying TEXT,
    market_status TEXT NOT NULL,
    market_data_status TEXT NOT NULL,
    first_validated_trade_id TEXT,
    first_validated_trade_ts timestamptz,
    last_validated_trade_id TEXT,
    last_validated_trade_ts timestamptz,
    candle_base_interval TEXT NOT NULL,
    candle_base_in_seconds INT NOT NULL,
    first_validated_candle timestamptz,
    last_validated_candle timestamptz,
    last_update_ts timestamptz NOT NULL,
    last_update_ip_address INET NOT NULL
)