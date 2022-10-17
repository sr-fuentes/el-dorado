-- Add migration script here
-- Create schema space for Archive - this will house all candles for use in research and in building
-- the production candles on start/run
CREATE SCHEMA IF NOT EXISTS archive;