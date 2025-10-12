WITH latest_time AS (
    SELECT MAX(time_id) AS time_id FROM fact_market_data
),
features AS (
    SELECT c.*
    FROM (
        SELECT
            time_id,
            coin_id,
            ROUND(((price_usd / LAG(price_usd, 1) OVER w) - 1) * 100, 3)  AS price_changes_pct_1h,
            ROUND(((price_usd / LAG(price_usd, 12) OVER w) - 1) * 100, 3) AS price_changes_pct_12h,
            ROUND(((price_usd / LAG(price_usd, 24) OVER w) - 1) * 100, 3) AS price_changes_pct_24h,
            ROUND(((price_usd / LAG(price_usd, 24*7) OVER w) - 1) * 100, 3) AS price_changes_pct_7d,
            ROUND(AVG(price_usd) OVER (w ROWS BETWEEN 23 PRECEDING AND CURRENT ROW), 6) AS moving_avg_24h,
            ROUND(AVG(price_usd) OVER (w ROWS BETWEEN (24*7 - 1) PRECEDING AND CURRENT ROW), 6) AS moving_avg_7d
        FROM fact_market_data md
        JOIN dim_times t ON t.id = md.time_id
        WINDOW w AS (PARTITION BY md.coin_id ORDER BY t.timestamp)
    ) c
    JOIN latest_time lt ON c.time_id = lt.time_id
)

INSERT INTO fact_market_features
SELECT * FROM features;