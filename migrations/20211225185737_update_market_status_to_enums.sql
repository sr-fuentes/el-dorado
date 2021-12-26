-- Add migration script here
-- update any existing market and market data statuses

UPDATE markets
SET market_status = 'active'
WHERE market_status = 'Active';

UPDATE markets
SET market_data_status = 'new'
WHERE market_data_status = 'New';

UPDATE markets
SET market_date_status = 'sync'
WHERE market_data_status = 'Syncing';