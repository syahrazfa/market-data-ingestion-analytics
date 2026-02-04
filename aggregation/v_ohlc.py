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

def build_1h_vohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE OR REPLACE VIEW silver.v_ohlc_1h AS
        SELECT
            *,
            (close - LAG(close) OVER w)
              / NULLIF(LAG(close) OVER w, 0) AS pct_change
        FROM silver.mv_ohlc_1h
        WINDOW w AS (
            PARTITION BY symbol, exchange
            ORDER BY tf_time
        );

        """)

# =========================================================
# 4H
# =========================================================

def build_4h_vohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE OR REPLACE VIEW silver.v_ohlc_4h AS
        SELECT
            *,
            (close - LAG(close) OVER w)
              / NULLIF(LAG(close) OVER w, 0) AS pct_change
        FROM silver.mv_ohlc_4h
        WINDOW w AS (
            PARTITION BY symbol, exchange
            ORDER BY tf_time
        );
        """)

# =========================================================
# 1D
# =========================================================

def build_1d_vohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE OR REPLACE VIEW silver.v_ohlc_1d AS
        SELECT
            *,
            (close - LAG(close) OVER w)
              / NULLIF(LAG(close) OVER w, 0) AS pct_change
        FROM silver.mv_ohlc_1d
        WINDOW w AS (
            PARTITION BY symbol, exchange
            ORDER BY tf_time
        );

        """)

# =========================================================
# 1W
# =========================================================

def build_1w_vohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE OR REPLACE VIEW silver.v_ohlc_1w AS
        SELECT
            *,
            (close - LAG(close) OVER w)
              / NULLIF(LAG(close) OVER w, 0) AS pct_change
        FROM silver.mv_ohlc_1w
        WINDOW w AS (
            PARTITION BY symbol, exchange
            ORDER BY tf_time
        );

        """)

# =========================================================
# 1MTH
# =========================================================

def build_1mth_vohlc(conn):
    with conn.cursor() as cur:
        cur.executemany("""
        CREATE OR REPLACE VIEW silver.v_ohlc_1mth AS
        SELECT
            *,
            (close - LAG(close) OVER w)
              / NULLIF(LAG(close) OVER w, 0) AS pct_change
        FROM silver.mv_ohlc_1mth
        WINDOW w AS (
            PARTITION BY symbol, exchange
            ORDER BY tf_time
        );

        """)

# =========================================================
# RUN
# =========================================================

def run():
    conn = get_conn()

    build_1h_vohlc(conn)
    build_4h_vohlc(conn)
    build_1d_vohlc(conn)
    build_1w_vohlc(conn)
    build_1mth_vohlc(conn)

if __name__ == '__main__':
    run()