CREATE OR REPLACE VIEW gold.v_ohlc_1h AS
SELECT
    symbol,
    exchange,
    tf_time,
    open,
    high,
    low,
    close,
    volume,
    pct_change
FROM silver.v_ohlc_1h;

CREATE OR REPLACE VIEW gold.v_ohlc_4h AS
SELECT
    symbol,
    exchange,
    tf_time,
    open,
    high,
    low,
    close,
    volume,
    pct_change
FROM silver.v_ohlc_4h;

CREATE OR REPLACE VIEW gold.v_ohlc_1d AS
SELECT
    symbol,
    exchange,
    tf_time,
    open,
    high,
    low,
    close,
    volume,
    pct_change
FROM silver.v_ohlc_1d;

CREATE OR REPLACE VIEW gold.v_ohlc_1w AS
SELECT
    symbol,
    exchange,
    tf_time,
    open,
    high,
    low,
    close,
    volume,
    pct_change
FROM silver.v_ohlc_1w;

CREATE OR REPLACE VIEW gold.v_ohlc_1mth AS
SELECT
    symbol,
    exchange,
    tf_time,
    open,
    high,
    low,
    close,
    volume,
    pct_change
FROM silver.v_ohlc_1mth;
