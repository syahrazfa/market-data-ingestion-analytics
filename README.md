# Market Data Ingestion & Analytics Platform
---

A production-oriented data platform that ingests, validates, and transforms
multi-exchange cryptocurrency market data into analytics-ready tables.

---

## Problem

Financial market data from multiple exchanges is:
- Fragmented across APIs
- Inconsistent in schema and timestamps
- Prone to missing or duplicated records
- Difficult to analyze at scale without strong data guarantees

This project addresses these issues by building a reliable, reproducible
data pipeline that standardizes market data for downstream analytics.

---

## Architecture

Data flows through a layered architecture inspired by the medallion pattern:

Sources
→ Ingestion
→ Bronze (raw)
→ Silver (cleaned)
→ Gold (analytics)
→ Consumers

---

- **Sources**: Binance, KuCoin OHLC APIs
- **Ingestion**: Scheduled batch ingestion with idempotent writes
- **Storage**: PostgreSQL warehouse
- **Transformations**: SQL-based transformations and derived metrics
- **Consumers**: Analytical queries and downstream feature tables

---

## Data Model

### Silver Layer – `y_ohlc_1d`

| Column        | Description                          |
|--------------|--------------------------------------|
| symbol       | Trading pair (e.g., BTCUSDT)          |
| exchange     | Source exchange                       |
| tf_time      | Timeframe timestamp (UTC+8)           |
| open/high/low/close | OHLC price data               |
| volume       | Traded volume                         |
| pct_change  | Daily price return                    |

Primary key: (symbol, exchange, tf_time)

---

## Data Quality Guarantees

The pipeline enforces:
- No duplicate candles per (symbol, exchange, timeframe)
- Detection of missing intervals
- Schema consistency across exchanges
- Deterministic re-runs via idempotent ingestion

Invalid or incomplete records are logged and excluded from downstream tables.

---

## Running the Project

1. Configure environment variables
2. Run ingestion jobs
3. Execute transformation scripts
4. Query analytics tables

Detailed setup steps are documented in `/docs`.

---

## What This Project Demonstrates

- End-to-end data pipeline ownership
- Production-oriented data modeling
- Data quality and reliability thinking
- Financial domain familiarity without strategy coupling

---

# 🛡️ License

This project is released under Apache 2.0 License

---

# 🌟 About Me

Hi, im Raz,

I am building my career at the intersection of data engineering, financial logic, and operator decision systems.

My work focuses on freezing real business reality into decision-grade data structures that support capital allocation, margin optimization, and risk visibility, not just dashboards and reports.

This repository is part of a long-term effort to move from technical data roles into operator and investment-facing positions, where data is used to change outcomes, not simply describe them.
