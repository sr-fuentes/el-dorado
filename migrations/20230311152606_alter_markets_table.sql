-- Add migration script here
ALTER TABLE markets
RENAME COLUMN base_currency TO base;

ALTER TABLE markets
RENAME COLUMN quote_currency TO quote;

ALTER TABLE markets
RENAME COLUMN market_status TO status;

ALTER TABLE markets
RENAME COLUMN candle_timeframe TO tf;

ALTER TABLE markets
DROP COLUMN underlying,
DROP COLUMN market_data_status;

ALTER TABLE markets
ADD COLUMN base_step NUMERIC;

ALTER TABLE markets
ADD COLUMN base_min NUMERIC;

ALTER TABLE markets
ADD COLUMN quote_step NUMERIC;

ALTER TABLE markets
ADD COLUMN tradable BOOLEAN;

ALTER TABLE markets
ADD COLUMN asset_id UUID;

UPDATE markets set tf = 'd01' WHERE tf is NULL;
UPDATE markets SET tradable = false WHERE 1=1;
update markets set market_type = 'perpetual' where market_type = 'future';

alter table markets alter column tradable set not null;
Alter table markets alter column tf set not null;