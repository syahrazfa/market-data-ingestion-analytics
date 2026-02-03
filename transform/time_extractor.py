import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
from psycopg2.extras import execute_batch

# =========================================================
# ENV
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / "config" / "settingc.env")

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
# DISTINCT TIMESTAMP
# =========================================================

def get_distinct_timestamp(conn):
   with conn.cursor() as cur:
       cur.execute("""
       SELECT DISTINCT timestamp
       FROM bronze.raw_ohlc;
       """)
       return [row[0] for row in cur.fetchall()]

# =========================================================
# SESSION CHECKER
# =========================================================

def get_session(utc_hour: int) -> str:
    if 0 <= utc_hour < 8:
        return "ASIA"
    elif 8 <= utc_hour < 13:
        return "LONDON"
    elif 13 <= utc_hour < 22:
        return "NEW_YORK"
    else:
        return "OFF"

# =========================================================
# BUILDING DIM TIME
# =========================================================

def build_dim_time(timestamps):
    rows = []

    for ts in timestamps:
        # normalize timestamp (ms → s if needed)
        ts_sec = ts / 1000 if ts > 10_000_000_000 else ts

        utc = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        iso = utc.isocalendar()

        session = get_session(utc.hour)

        rows.append((
            ts,                     # original epoch (keep raw)
            utc,
            utc.date(),
            utc.year,
            utc.month,
            utc.day,
            utc.hour,
            utc.minute,
            iso.week,
            utc.weekday(),
            utc.weekday() >= 5,
            session
        ))

    return rows
# =========================================================
# INSERT INTO silver.dim_time
# =========================================================

def insert_into_dim_time(conn, rows):
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO silver.dim_time (
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
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (epoch) DO NOTHING
        """, rows, page_size=1000)

    conn.commit()

def run():
    conn = get_conn()

    timestamps = get_distinct_timestamp(conn)
    rows = build_dim_time(timestamps)
    insert_into_dim_time(conn, rows)

    conn.close()

if __name__ == '__main__':
    run()
