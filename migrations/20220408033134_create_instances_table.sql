-- Add migration script here
DROP TABLE IF EXISTS instances;

CREATE TABLE instances (
    instance_type TEXT NOT NULL,
    droplet TEXT NOT NULL,
    exchange_name TEXT,
    instance_status TEXT NOT NULL,
    restart BOOLEAN NOT NULL,
    last_restart_ts timestamptz,
    restart_count INTEGER,
    num_markets INTEGER NOT NULL,
    last_update_ts timestamptz NOT NULL,
    PRIMARY KEY (droplet, exchange_name)
);