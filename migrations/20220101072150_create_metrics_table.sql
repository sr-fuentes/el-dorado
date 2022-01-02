-- Add migration script here
/*
Create table to hold all metrcis in the format for AP. This will not be the format going forward
but will allow for backwards compatibility until a new trade signal / execution engine is built.
Table will be unique on Exchange / Market / TimeFrame / Lbp / Datetime. Management of archiving or
deleting older metrics will be a job for the manager. Selecting correct metrics will be job for 
consumer (in this case AP)
*/
DROP TABLE IF EXISTS metrics_ap;

CREATE TABLE metrics_ap (
    exchange_name TEXT NOT NULL,
    market_name TEXT NOT NULL,
    datetime timestamptz NOT NULL,
    time_frame TEXT NOT NULL,
    lbp BIGINT NOT NULL,
    close NUMERIC NOT NULL,
    r NUMERIC NOT NULL,
    H004R NUMERIC NOT NULL,
    H004C NUMERIC NOT NULL,
    L004R NUMERIC NOT NULL,
    L004C NUMERIC NOT NULL,
    H008R NUMERIC NOT NULL,
    H008C NUMERIC NOT NULL,
    L008R NUMERIC NOT NULL,
    L008C NUMERIC NOT NULL,
    H012R NUMERIC NOT NULL,
    H012C NUMERIC NOT NULL,
    L012R NUMERIC NOT NULL,
    L012C NUMERIC NOT NULL,
    H024R NUMERIC NOT NULL,
    H024C NUMERIC NOT NULL,
    L024R NUMERIC NOT NULL,
    L024C NUMERIC NOT NULL,
    H048R NUMERIC NOT NULL,
    H048C NUMERIC NOT NULL,
    L048R NUMERIC NOT NULL,
    L048C NUMERIC NOT NULL,
    H096R NUMERIC NOT NULL,
    H096C NUMERIC NOT NULL,
    L096R NUMERIC NOT NULL,
    L096C NUMERIC NOT NULL,
    H192R NUMERIC NOT NULL,
    H192C NUMERIC NOT NULL,
    L192R NUMERIC NOT NULL,
    L192C NUMERIC NOT NULL,
    EMA1 NUMERIC NOT NULL,
    EMA2 NUMERIC NOT NULL,
    EMA3 NUMERIC NOT NULL,
    MV1 NUMERIC NOT NULL,
    MV2 NUMERIC NOT NULL,
    MV3 NUMERIC NOT NULL,
    ofs NUMERIC NOT NULL,
    vs NUMERIC NOT NULL,
    rs NUMERIC NOT NULL,
    n NUMERIC NOT NULL,
    trs NUMERIC NOT NULL,
    uws NUMERIC NOT NULL,
    mbs NUMERIC NOT NULL,
    lws NUMERIC NOT NULL,
    ma NUMERIC NOT NULL,
    vw NUMERIC NOT NULL,
    insert_ts timestamptz NOT NULL,
    PRIMARY KEY (exchange_name, market_name, datetime, time_frame, lbp)
);