-- Add migration script here
-- Moving create exchange candle tables to migration as previous creation was done on exchange
-- addition. Since moving exchange addition to migration this needs to move too.
CREATE TABLE IF NOT EXISTS candles_15t_ftx (
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
    first_trade_id TEXT NOT NULL,
    PRIMARY KEY (datetime, market_id)
);

CREATE TABLE IF NOT EXISTS candles_15t_ftxus (
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
    first_trade_id TEXT NOT NULL,
    PRIMARY KEY (datetime, market_id)
);

CREATE TABLE IF NOT EXISTS candles_15t_gdax (
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
    first_trade_id TEXT NOT NULL,
    PRIMARY KEY (datetime, market_id)
);