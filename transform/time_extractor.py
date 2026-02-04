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
# GET EXISTING TIMESTAMP
# =========================================================

def get_existing_dim_timestamps(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT epoch
            FROM silver.dim_time
        """)
        return {row[0] for row in cur.fetchall()}

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
        # normalize epoch (ms → sec if needed)
        ts_sec = ts / 1000 if ts > 10_000_000_000 else ts

        utc = datetime.fromtimestamp(ts_sec, tz=timezone.utc)
        iso = utc.isocalendar()

        hour = utc.hour

        # killzone flags (UTC)
        is_london_killzone = 7 <= hour < 10
        is_ny_killzone = 12 <= hour < 15

        # session overlaps
        is_london_ny_overlap = 13 <= hour < 16

        session = get_session(hour)

        rows.append((
            ts,                     # epoch (raw)
            utc,                    # utc_timestamp
            utc.date(),
            utc.year,
            utc.month,
            utc.day,
            utc.hour,
            utc.minute,
            iso.week,
            utc.weekday(),
            utc.weekday() >= 5,
            session,
            is_london_killzone,
            is_ny_killzone,
            is_london_ny_overlap
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
                session,
                is_london_killzone,
                is_ny_killzone
                is_ny_london_overlap
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (epoch) DO NOTHING
        """, rows, page_size=1000)

    conn.commit()


def run():
    conn = get_conn()

    raw_ts = get_distinct_timestamp(conn)
    existing_ts = get_existing_dim_timestamps(conn)

    new_ts = [ts for ts in raw_ts if ts not in existing_ts]

    if not new_ts:
        print("[dim_time] no new timestamps")
        conn.close()
        return

    rows = build_dim_time(new_ts)
    insert_into_dim_time(conn, rows)

    print(f"[dim_time] inserted {len(rows)} rows")
    conn.close()

if __name__ == '__main__':
    run()
