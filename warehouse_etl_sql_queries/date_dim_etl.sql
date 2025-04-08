WITH staging_table AS (
    SELECT DISTINCT 
        CAST(strftime(time_hour, '%Y%m%d%H') AS INTEGER) AS date_key,
        CAST(time_hour AS DATE)                        AS date,
        EXTRACT(YEAR FROM time_hour)                   AS year,
        EXTRACT(QUARTER FROM time_hour)                AS quarter,
        EXTRACT(MONTH FROM time_hour)                  AS month,
        EXTRACT(WEEK FROM time_hour)                   AS week,
        EXTRACT(DAY FROM time_hour)                    AS day,
        EXTRACT(HOUR FROM time_hour)                   AS hour,
        CASE 
            WHEN EXTRACT(ISODOW FROM time_hour) IN (6, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM lakehouse_packets
)


INSERT INTO date_dim (
    date_key, 
    date, 
    year, 
    quarter,
    month,
    week,
    day,
    hour,
    is_weekend
)
SELECT 
    staging_table.date_key,
    staging_table.date,
    staging_table.year,
    staging_table.quarter,
    staging_table.month,
    staging_table.week,
    staging_table.day,
    staging_table.hour,
    staging_table.is_weekend
FROM staging_table
LEFT JOIN date_dim
    ON staging_table.date_key = date_dim.date_key 
    AND date_dim.date = ?::DATE 
WHERE date_dim.date_key IS NULL