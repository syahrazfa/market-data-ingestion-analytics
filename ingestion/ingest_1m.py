import asyncio
import aiohttp
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
import socket

socket.setdefaulttimeout(15)

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

BINANCE_URL = "https://data-api.binance.vision/api/v3/klines"
KUCOIN_URL = "https://api-futures.kucoin.com/api/v1/market/candles"

# =========================================================
# DB
# =========================================================
def get_conn():
    return psycopg2.connect(**DB_PARAMS)


def get_last_timestamp(conn, symbol, interval, exchange):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(timestamp)
            FROM bronze.raw_ohlc
            WHERE symbol=%s AND interval=%s AND exchange=%s
            """,
            (symbol, interval, exchange),
        )
        row = cur.fetchone()
        return row[0]


def insert_rows(conn, rows):
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO bronze.raw_ohlc (
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
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (timestamp, symbol, interval, exchange) DO NOTHING
            """,
            rows,
        )
    conn.commit()

# =========================================================
# BINANCE
# =========================================================
async def fetch_binance(session, symbol, interval, start_ts):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": 1000,
    }
    if start_ts is not None:
        params["startTime"] = start_ts + 1

    async with session.get(BINANCE_URL, params=params) as r:
        r.raise_for_status()
        return await r.json()


def normalize_binance(candles, symbol):
    return [
        (
            int(c[0]),              # ms
            float(c[1]),
            float(c[2]),
            float(c[3]),
            float(c[4]),
            float(c[5]),
            symbol,
            "1m",
            "binance",
        )
        for c in candles
    ]

# =========================================================
# KUCOIN
# =========================================================
async def fetch_kucoin(session, symbol, interval, start_ts):
    params = {
        "symbol": symbol.replace("USDT", "-USDT"),
        "type": interval,  # "1min"
    }

    if start_ts is not None:
        params["startAt"] = int(start_ts / 1000)

    for attempt in range(3):  # retry 3 times
        try:
            async with session.get(
                "https://api.kucoin.com/api/v1/market/candles",
                params=params,
                timeout=aiohttp.ClientTimeout(
                    total=15,
                    connect=5,
                    sock_connect=5,
                    sock_read=10,
                ),
            ) as r:
                r.raise_for_status()
                data = await r.json()

                if data.get("code") != "200000":
                    raise RuntimeError(data)

                return data["data"]

        except Exception as e:
            if attempt == 2:
                raise
            await asyncio.sleep(2)



def normalize_kucoin(candles, symbol):
    rows = []

    for c in candles:
        ts = int(c[0]) * 1000  # seconds → ms
        rows.append(
            (
                ts,
                float(c[1]),   # open
                float(c[3]),   # high
                float(c[4]),   # low
                float(c[2]),   # close
                float(c[5]),   # volume
                symbol,
                "1m",
                "kucoin",
            )
        )

    return rows

# =========================================================
# ADAPTER REGISTRY
# =========================================================
EXCHANGES = {
    "binance": {
        "fetch": fetch_binance,
        "normalize": normalize_binance,
        "interval": "1m",
    },
    "kucoin": {
        "fetch": fetch_kucoin,
        "normalize": normalize_kucoin,
        "interval": "1min",
    },
}

# =========================================================
# INGESTION
# =========================================================
async def ingest(symbol: str, exchange: str):
    cfg = EXCHANGES[exchange]

    conn = get_conn()
    last_ts = get_last_timestamp(conn, symbol, "1m", exchange)

    async with aiohttp.ClientSession() as session:
        raw = await cfg["fetch"](
            session,
            symbol,
            cfg["interval"],
            last_ts,
        )

    if not raw:
        print(f"[{exchange}] no new data")
        conn.close()
        return

    rows = cfg["normalize"](raw, symbol)
    insert_rows(conn, rows)

    print(f"[{exchange}] inserted {len(rows)} candles")
    conn.close()

# =========================================================
# MAIN
# =========================================================
async def main():
    await asyncio.gather(
        ingest("BTCUSDT", "binance"),
        ingest("BTCUSDT", "kucoin"),
    )

if __name__ == "__main__":
    asyncio.run(main())
