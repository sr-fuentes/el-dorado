-- Add migration script here
-- Insert SOL-PERP with localdev location for repo install
INSERT INTO markets (
    market_id, exchange_name, market_name, market_type, underlying, market_status, 
    market_data_status, mita) VALUES (
        '19994c6a-fa3c-4b0b-96c4-c744c43a9514',
        'ftx',
        'SOL-PERP',
        'future',
        'SOL',
        'active',
        'active',
        'localdev'
    ) ON CONFLICT DO NOTHING;