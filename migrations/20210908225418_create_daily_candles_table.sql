-- Add migration script here
/* Create the daily candles table. This will hold validated
Daily (1D) Candles from each market that is Active in El Dorado.
This can be used for end of day calculations, market to market
and validation of all heartbeat (15T) candles in each {exchange}_candles
table.
*/
CREATE TABLE daily_candles (
    datetime timestamptz NOT NULL,
    PRIMARY KEY (datetime),
    open FLOAT NOT NULL,
    high FLOAT NOT NULL,
    low FLOAT NOT NULL,
    close FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    volume_net FLOAT NOT NULL,
    volume_liquidation FLOAT NOT NULL,
    volume_liquidation_net FLOAT NOT NULL,
    value FLOAT NOT NULL,
    trade_count BIGINT NULL,
    liquidation_count BIGINT NOT NULL,
    last_trade_ts timestamptz NOT NULL,
    last_trade_id TEXT NOT NULL
);