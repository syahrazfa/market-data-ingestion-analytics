CREATE TABLE IF NOT EXISTS silver.fact_candles (
    timestamp   BIGINT      NOT NULL,  -- epoch ms
    open        DOUBLE PRECISION NOT NULL,
    high        DOUBLE PRECISION NOT NULL,
    low         DOUBLE PRECISION NOT NULL,
    close       DOUBLE PRECISION NOT NULL,
    volume      DOUBLE PRECISION NOT NULL,

    symbol      TEXT        NOT NULL,
    interval    TEXT        NOT NULL,   -- "1m", "5m", etc
    exchange    TEXT        NOT NULL,   -- binance, bybit, etc

    PRIMARY KEY (timestamp, symbol, interval, exchange)
);

CREATE TABLE IF NOT EXISTS silver.dim_time(
	epoch,
	utc_timestamp,
	date,
	year, 
	month,
	day,
	hour,
	minute,
	iso_week,
	weekday,
	is_weekend,
	session
);

ALTER TABLE silver.dim_time
ADD COLUMN is_london_killzone BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN is_ny_killzone     BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE silver.dim_time
ADD COLUMN IF NOT EXISTS is_london_ny_overlap BOOLEAN NOT NULL DEFAULT FALSE;

CREATE OR REPLACE VIEW silver.v_candles AS
SELECT
  f.*,
  t.utc_timestamp,
  t.session,
  t.is_london_killzone,
  t.is_ny_killzone,
  t.is_london_ny_overlap
FROM silver.fact_candles f
JOIN silver.dim_time t
  ON f.timestamp = t.epoch;
