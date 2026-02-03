import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

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
# FETCH RAW CANDLES
# =========================================================
def fetch_raw_candles(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                symbol,
                interval,
                exchange
            FROM bronze.raw_ohlc
            WHERE interval = '1m'
        """)
        return cur.fetchall()

# =========================================================
# INSERT FACT CANDLES
# =========================================================
def insert_fact_candles(conn, rows):
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO silver.fact_candles (
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                symbol,
                interval,
                exchange
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, symbol, exchange) DO NOTHING
        """, rows, page_size=1000)

    conn.commit()

def run():
    conn = get_conn()
    rows = fetch_raw_candles(conn)
    insert_fact_candles(conn, rows)
    conn.close()

if __name__ == "__main__":
    run()