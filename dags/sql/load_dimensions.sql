INSERT INTO dim_coins (name)
VALUES (%(coins)s)
ON CONFLICT (name) DO NOTHING;

INSERT INTO dim_times (timestamp)
VALUES (%(timestamp)s);