CREATE TABLE IF NOT EXISTS coins(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS times(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS market_data(
    time_id INT NOT NULL REFERENCES times (id),
    coin_id INT NOT NULL REFERENCES coins (id),
    PRIMARY KEY (time_id, coin_id),
    price_usd NUMERIC(18, 6) NOT NULL,
    volume_usd_24h NUMERIC(18, 6) NOT NULL,
    market_cap_usd NUMERIC(18, 6) NOT NULL
);

CREATE TABLE IF NOT EXISTS indicators(
    time_id INT NOT NULL REFERENCES times (id),
    coin_id INT NOT NULL REFERENCES coins (id),
    PRIMARY KEY (time_id, coin_id),
    price_changes_pct_1h NUMERIC(6, 3),
    price_changes_pct_12h NUMERIC(6, 3),
    price_changes_pct_24h NUMERIC(6, 3),
    price_changes_pct_7d NUMERIC(8, 3),
    moving_avg_24h NUMERIC(18, 6),
    moving_avg_7d NUMERIC(18, 6)
);

