-- Add migration script here
-- Add exchanges that are supported explicitly through migration and remove add option from cli
INSERT INTO exchanges VALUES (
    '3220b1bd-b731-4eba-9a61-4aa3ba2b17f9',
    'gdax',
    2,
    true,
    false,
    'new',
    NOW(),
    NOW()
) ON CONFLICT DO NOTHING;

INSERT INTO exchanges VALUES (
    '1ccf3627-bbee-4148-9987-0085a920596b',
    'ftx',
    1,
    true,
    true,
    'new',
    NOW(),
    NOW()
) ON CONFLICT DO NOTHING;

INSERT INTO exchanges VALUES (
    'ffb75543-74ca-46b1-920b-81ffdd98ebdb',
    'ftxus',
    2,
    true,
    false,
    'new',
    NOW(),
    NOW()
) ON CONFLICT DO NOTHING;