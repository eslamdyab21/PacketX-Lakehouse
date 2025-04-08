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


INSERT INTO db.warehouse.date_dim (
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
SELECT * 
    FROM staging_table 
    WHERE
        staging_table.date_key NOT IN (
            SELECT date_key FROM db.warehouse.date_dim 
            WHERE date = ?::DATE 
        )