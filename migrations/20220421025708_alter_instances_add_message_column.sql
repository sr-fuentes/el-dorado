-- Add migration script here
ALTER TABLE IF EXISTS instances
ADD COLUMN last_message_ts timestamptz;