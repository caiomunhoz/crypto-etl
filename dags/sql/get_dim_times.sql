SELECT 
    id,
    TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI') AS timestamp
FROM dim_times 
WHERE timestamp = %(timestamp)s;