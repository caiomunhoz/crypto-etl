INSERT INTO dim_coins (name)
VALUES
{% for coin in params.coins %}
    ('{{ coin }}'){% if not loop.last %},{% endif %}
{% endfor %}
ON CONFLICT (name) DO NOTHING;

INSERT INTO dim_times (timestamp)
VALUES (%(timestamp)s);