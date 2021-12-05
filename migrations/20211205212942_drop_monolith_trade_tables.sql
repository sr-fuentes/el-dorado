-- Add migration script here
-- Drop any non market trade tables as the refactoring has split those into individual tables
-- For each market

DROP TABLE IF EXISTS trades_ftx_rest;
DROP TABLE IF EXISTS trades_ftx_ws;
DROP TABLE IF EXISTS trades_ftx_processed;
DROP TABLE IF EXISTS trades_ftx_validated;
DROP TABLE IF EXISTS trades_ftxus_rest;
DROP TABLE IF EXISTS trades_ftxus_ws;
DROP TABLE IF EXISTS trades_ftxus_processed;
DROP TABLE IF EXISTS trades_ftxus_validated;
