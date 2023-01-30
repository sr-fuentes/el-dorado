-- Add migration script here
DROP TABLE IF EXISTS market_archive_details;

CREATE TABLE market_archive_details (
    market_id uuid NOT NULL,
    PRIMARY KEY (market_id),
    exchange_name TEXT NOT NULL,
    market_name TEXT NOT NULL,
    tf TEXT NOT NULL,
    first_candle_dt timestamptz NOT NULL,
    first_trade_dt timestamptz NOT NULL,
    first_trade_price NUMERIC NOT NULL,
    first_trade_id TEXT NOT NULL,
    last_candle_dt timestamptz NOT NULL,
    last_trade_dt timestamptz NOT NULL,
    last_trade_price NUMERIC NOT NULL,
    last_trade_id TEXT NOT NULL,
    next_month timestamptz NOT NULL
)