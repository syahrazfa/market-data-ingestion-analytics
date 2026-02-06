CREATE TABLE IF NOT EXISTS bronze.raw_ohlc (
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
