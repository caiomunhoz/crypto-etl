CREATE TABLE IF NOT EXISTS dim_coins(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_times(
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_market_data(
    time_id INT NOT NULL REFERENCES dim_times (id),
    coin_id INT NOT NULL REFERENCES dim_coins (id),
    PRIMARY KEY (time_id, coin_id),
    price_usd NUMERIC(14, 4) NOT NULL,
    volume_24h_usd NUMERIC(22, 6) NOT NULL,
    market_cap_usd NUMERIC(25, 4) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_market_features(
    time_id INT NOT NULL REFERENCES dim_times (id),
    coin_id INT NOT NULL REFERENCES dim_coins (id),
    PRIMARY KEY (time_id, coin_id),
    price_changes_pct_1h NUMERIC(7, 3),
    price_changes_pct_12h NUMERIC(7, 3),
    price_changes_pct_24h NUMERIC(7, 3),
    price_changes_pct_7d NUMERIC(7, 3),
    moving_avg_24h NUMERIC(18, 6),
    moving_avg_7d NUMERIC(18, 6)
);