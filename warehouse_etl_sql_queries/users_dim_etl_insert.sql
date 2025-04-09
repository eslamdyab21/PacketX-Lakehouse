WITH staging_table AS (
    SELECT 
        MAX(time_hour) as time_hour,
        user AS user_name,
        source_ip AS local_ip
    FROM lakehouse_packets
    WHERE 
        source_ip LIKE '192.168.%'
    GROUP BY user, local_ip
),


-- Current date users_dim
current_date_users_dim AS (
    SELECT * FROM users_dim
    WHERE CAST(start_date AS DATE) IN (?::DATE, ?::DATE - 1)
),


-- All new IPs not already in users_dim
new_ip_candidates AS (
    SELECT DISTINCT 
        s.user_name,
        s.local_ip,
        s.time_hour AS start_date
    FROM staging_table s
    LEFT JOIN current_date_users_dim u 
        ON s.user_name = u.user_name 
        AND s.local_ip = u.local_ip
    WHERE u.user_name IS NULL  -- this means it's a new record
),


-- Rank to find most recent IP per user
ranked_new_ips AS (
    SELECT 
        *,
        LAG(start_date) OVER(PARTITION BY user_name ORDER BY start_date DESC) as end_date,
        ROW_NUMBER() OVER (PARTITION BY user_name ORDER BY start_date DESC) AS rn
    FROM new_ip_candidates
),


-- Mark current_flag for each new IP
prepared_new_ip_records AS (
    SELECT
        user_name,
        local_ip,
        start_date,
        end_date,
        CASE 
            WHEN rn = 1 THEN TRUE
            ELSE FALSE
        END AS current_flag
    FROM ranked_new_ips
)


-- Insert new rows
INSERT INTO users_dim (
    user_name,
    local_ip,
    start_date,
    end_date,
    current_flag
)
SELECT 
    user_name,
    local_ip,
    start_date,
    end_date,
    current_flag
FROM prepared_new_ip_records