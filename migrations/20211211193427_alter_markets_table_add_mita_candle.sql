-- Add migration script here
-- Add mita column to markets to mark which markets are under which mita
-- Add first candle and last candle columns

ALTER TABLE IF EXISTS markets
ADD COLUMN first_candle timestamptz NULL;

ALTER TABLE IF EXISTS markets
ADD COLUMN last_candle timestamptz NULL;

ALTER TABLE IF EXISTS markets
ADD COLUMN mita TEXT NULL;