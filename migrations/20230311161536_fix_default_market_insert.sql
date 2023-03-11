-- Add migration script here
INSERT INTO markets (
    market_id, exchange_name, market_name, market_type, base, quote, status, mita,
    tf, tradable) VALUES (
        '457076db-7daa-4e4c-927c-24f52f3fc3ea',
        'gdax',
        'LDO-USD',
        'spot',
        'LDO',
        'USD',
        'active',
        'localdev',
        't15',
        true
    ) ON CONFLICT DO NOTHING;