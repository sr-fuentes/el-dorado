-- Add migration script here
DROP TABLE IF EXISTS alerts;

CREATE TABLE alerts (
    alert_id uuid NOT NULL,
    PRIMARY KEY (alert_id),
    instance_type TEXT NOT NULL,
    droplet TEXT NOT NULL,
    exchange_name TEXT,
    timestamp timestamptz NOT NULL,
    message TEXT NOT NULL
);