-- Add migration script here
DROP TABLE IF EXISTS research_metrics;

CREATE TABLE research_metrics (
    market_id uuid NOT NULL,
    tf TEXT NOT NULL,
    datetime timestamptz NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    atr_l NUMERIC NOT NULL,
    atr_s NUMERIC NOT NULL,
    ma_filter TEXT NOT NULL,
    n_filter TEXT NOT NULL,
    prev TEXT NOT NULL,
    return_z_l NUMERIC NOT NULL,
    return_z_s NUMERIC NOT NULL,
    tr_z_l NUMERIC NOT NULL,
    tr_z_s NUMERIC NOT NULL,
    upper_wick_z_l NUMERIC NOT NULL,
    upper_wick_z_s NUMERIC NOT NULL,
    body_z_l NUMERIC NOT NULL,
    body_z_s NUMERIC NOT NULL,
    lower_wick_z_l NUMERIC NOT NULL,
    lower_wick_z_s NUMERIC NOT NULL,
    volume_z_l NUMERIC NOT NULL,
    volume_z_s NUMERIC NOT NULL,
    volume_net_z_l NUMERIC NOT NULL,
    volume_net_z_s NUMERIC NOT NULL,
    volume_pct_z_l NUMERIC NOT NULL,
    volume_pct_z_s NUMERIC NOT NULL,
    volume_liq_z_l NUMERIC NOT NULL,
    volume_liq_z_s NUMERIC NOT NULL,
    volume_liq_net_z_l NUMERIC NOT NULL,
    volume_liq_net_z_s NUMERIC NOT NULL,
    volume_liq_pct_z_l NUMERIC NOT NULL,
    volume_liq_pct_z_s NUMERIC NOT NULL,
    value_z_l NUMERIC NOT NULL,
    value_z_s NUMERIC NOT NULL,
    value_net_z_l NUMERIC NOT NULL,
    value_net_z_s NUMERIC NOT NULL,
    value_pct_z_l NUMERIC NOT NULL,
    value_pct_z_s NUMERIC NOT NULL,
    value_liq_z_l NUMERIC NOT NULL,
    value_liq_z_s NUMERIC NOT NULL,
    value_liq_net_z_l NUMERIC NOT NULL,
    value_liq_net_z_s NUMERIC NOT NULL,
    value_liq_pct_z_l NUMERIC NOT NULL,
    value_liq_pct_z_s NUMERIC NOT NULL,
    trade_count_z_l NUMERIC NOT NULL,
    trade_count_z_s NUMERIC NOT NULL,
    trade_count_net_z_l NUMERIC NOT NULL,
    trade_count_net_z_s NUMERIC NOT NULL,
    trade_count_pct_z_l NUMERIC NOT NULL,
    trade_count_pct_z_s NUMERIC NOT NULL,
    liq_count_z_l NUMERIC NOT NULL,
    liq_count_z_s NUMERIC NOT NULL,
    liq_count_net_z_l NUMERIC NOT NULL,
    liq_count_net_z_s NUMERIC NOT NULL,
    liq_count_pct_z_l NUMERIC NOT NULL,
    liq_count_pct_z_s NUMERIC NOT NULL,
    insert_dt timestamptz NOT NULL
)