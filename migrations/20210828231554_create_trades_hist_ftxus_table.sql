-- Add migration script here
-- Create table for historical trades from FTX US REST API
CREATE TABLE trades_hist_ftxus(
    id bigint NOT NULL,
    PRIMARY KEY (id),
    price TEXT,
    size TEXT,
    side TEXT,
    liquidation boolean,
    trade_time timestamptz
);


-- "id": 6371425,
-- "price": 39737.0,
-- "size": 0.0006,
-- "side": "sell",
-- "liquidation": false,
-- "time": "2021-08-02T03:37:32.722725+00:00"