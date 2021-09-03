-- Add migration script here
/* Create Exchanges Table
This table will hold all exchange details that are used in el-dorado. Rows may be added via
the exchanges route.
*/
CREATE TABLE exchanges(
    exchange_id uuid NOT NULL,
    PRIMARY KEY (exchange_id),
    exchange_name TEXT NOT NULL,
    exchange_rank INT NOT NULL,
    is_spot BOOLEAN NOT NULL,
    is_derivitive  BOOLEAN NOT NULL,
    exchange_status TEXT NOT NULL,
    added_date timestamptz NOT NULL,
    last_refresh_date timestamptz NOT NULL
);