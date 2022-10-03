-- Add migration script here
DROP TABLE IF EXISTS market_candle_details;

CREATE TABLE market_candle_details (
    market_id uuid NOT NULL,
    PRIMARY KEY (market_id),
    exchange_name TEXT NOT NULL,
    market_name TEXT NOT NULL,
    time_frame TEXT NOT NULL,
    first_candle timestamptz NOT NULL,
    last_candle timestamptz NOT NULL,
    last_trade_ts timestamptz NOT NULL,
    last_trade_id TEXT NOT NULL
)

