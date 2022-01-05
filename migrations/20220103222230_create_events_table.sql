-- Add migration script here

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    event_id uuid NOT NULL,
    droplet TEXT NOT NULL,
    event_type TEXT NOT NULL,
    exchange_name TEXT NOT NULL,
    market_id uuid NOT NULL,
    start_ts timestamptz,
    end_ts timestamptz,
    event_ts timestamptz NOT NULL,
    created_ts timestamptz NOT NULL,
    processed_ts timestamptz,
    event_status TEXT NOT NULL,
    notes TEXT,
    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS events_droplet
ON events (droplet);

CREATE INDEX IF NOT EXISTS events_status
ON events (event_status);

CREATE INDEX IF NOT EXISTS events_ts
ON events (event_ts);