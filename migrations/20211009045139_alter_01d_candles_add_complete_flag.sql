-- Add migration script here
ALTER TABLE IF EXISTS candles_01d
ADD COLUMN is_complete BOOLEAN NULL;