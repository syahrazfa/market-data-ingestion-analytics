import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# =========================================================
# ENV
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / "config" / "setting.env")

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def get_conn():
    return psycopg2.connect(**DB_PARAMS)

# =========================================================
# 1H
# =========================================================

def build_1h_ohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE MATERIALIZED VIEW silver.mv_ohlc_1h AS
        SELECT DISTINCT
            symbol,
            exchange,
            tf_time,
            FIRST_VALUE(open)  OVER w AS open,
            MAX(high)          OVER w AS high,
            MIN(low)           OVER w AS low,
            LAST_VALUE(close)  OVER w AS close,
            SUM(volume)        OVER w AS volume
        FROM (
            SELECT
                symbol,
                exchange,
                date_trunc('hour', utc_timestamp) AS tf_time,
                open,
                high,
                low,
                close,
                volume,
                utc_timestamp
            FROM silver.v_candles
        ) t
        WINDOW w AS (
            PARTITION BY symbol, exchange, tf_time
            ORDER BY utc_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        );
        """)

# =========================================================
# 4H
# =========================================================

def build_4h_ohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE MATERIALIZED VIEW silver.mv_ohlc_4h AS
        SELECT DISTINCT
            symbol,
            exchange,
            tf_time,
            FIRST_VALUE(open)  OVER w AS open,
            MAX(high)          OVER w AS high,
            MIN(low)           OVER w AS low,
            LAST_VALUE(close)  OVER w AS close,
            SUM(volume)        OVER w AS volume
        FROM (
            SELECT
                symbol,
                exchange,
                date_trunc('hour', utc_timestamp)
                  - (EXTRACT(hour FROM utc_timestamp) % 4) * INTERVAL '1 hour'
                  AS tf_time,
                open,
                high,
                low,
                close,
                volume,
                utc_timestamp
            FROM silver.v_candles
        ) t
        WINDOW w AS (
            PARTITION BY symbol, exchange, tf_time
            ORDER BY utc_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        );
        """)

# =========================================================
# 1D
# =========================================================

def build_1d_ohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE MATERIALIZED VIEW silver.mv_ohlc_1d AS
        SELECT DISTINCT
            symbol,
            exchange,
            tf_time,
            FIRST_VALUE(open)  OVER w AS open,
            MAX(high)          OVER w AS high,
            MIN(low)           OVER w AS low,
            LAST_VALUE(close)  OVER w AS close,
            SUM(volume)        OVER w AS volume
        FROM (
            SELECT
                symbol,
                exchange,
                date_trunc('day', utc_timestamp) AS tf_time,
                open,
                high,
                low,
                close,
                volume,
                utc_timestamp
            FROM silver.v_candles
        ) t
        WINDOW w AS (
            PARTITION BY symbol, exchange, tf_time
            ORDER BY utc_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        );
        """)

# =========================================================
# 1W
# =========================================================

def build_1w_ohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE MATERIALIZED VIEW silver.mv_ohlc_1w AS
        SELECT DISTINCT
            symbol,
            exchange,
            tf_time,
            FIRST_VALUE(open)  OVER w AS open,
            MAX(high)          OVER w AS high,
            MIN(low)           OVER w AS low,
            LAST_VALUE(close)  OVER w AS close,
            SUM(volume)        OVER w AS volume
        FROM (
            SELECT
                symbol,
                exchange,
                date_trunc('week', utc_timestamp) AS tf_time,
                open,
                high,
                low,
                close,
                volume,
                utc_timestamp
            FROM silver.v_candles
        ) t
        WINDOW w AS (
            PARTITION BY symbol, exchange, tf_time
            ORDER BY utc_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        );
        """)

# =========================================================
# 1D
# =========================================================

def build_1mth_ohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE MATERIALIZED VIEW silver.mv_ohlc_1mth AS
        SELECT DISTINCT
            symbol,
            exchange,
            tf_time,
            FIRST_VALUE(open)  OVER w AS open,
            MAX(high)          OVER w AS high,
            MIN(low)           OVER w AS low,
            LAST_VALUE(close)  OVER w AS close,
            SUM(volume)        OVER w AS volume
        FROM (
            SELECT
                symbol,
                exchange,
                date_trunc('month', utc_timestamp) AS tf_time,
                open,
                high,
                low,
                close,
                volume,
                utc_timestamp
            FROM silver.v_candles
        ) t
        WINDOW w AS (
            PARTITION BY symbol, exchange, tf_time
            ORDER BY utc_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        );
        """)

# =========================================================
# RUN
# =========================================================

def run():
    conn = get_conn()

    build_1h_ohlc(conn)
    build_4h_ohlc(conn)
    build_1d_ohlc(conn)

if __name__ == '__main__':
    run()