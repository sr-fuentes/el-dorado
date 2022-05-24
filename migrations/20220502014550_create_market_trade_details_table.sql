-- Add migration script here
DROP TABLE IF EXISTS market_trade_details;

CREATE TABLE market_trade_details (
    market_id uuid NOT NULL,
    PRIMARY KEY (market_id),
    market_start_ts timestamptz,
    first_trade_ts timestamptz NOT NULL,
    first_trade_id TEXT NOT NULL,
    last_trade_ts timestamptz NOT NULL,
    last_trade_id TEXT NOT NULL,
    previous_trade_day timestamptz NOT NULL,
    previous_status TEXT NOT NULL,
    next_trade_day timestamptz,
    next_status TEXT
)