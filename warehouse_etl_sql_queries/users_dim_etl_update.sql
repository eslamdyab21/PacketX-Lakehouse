-- New batch data
WITH staging_table AS (
    SELECT 
        max(time_hour) as start_date,
        user as user_name,
        source_ip AS local_ip,
    FROM lakehouse_packets
    WHERE 
        source_ip LIKE '192.168%'
    GROUP BY user, source_ip
    ORDER BY start_date DESC
    LIMIT 1
),


-- Users that changed IP
changed_users AS (
    SELECT 
        s.user_name,
        s.local_ip,
        s.start_date,
        u.user_key
    FROM staging_table s
    LEFT JOIN users_dim u 
        ON s.user_name = u.user_name AND u.current_flag = TRUE
    WHERE u.local_ip IS DISTINCT FROM s.local_ip
)


-- Mark current_flag = FALSE for old records
UPDATE users_dim
SET 
    end_date = (SELECT start_date FROM changed_users),
    current_flag = FALSE
FROM changed_users cu
WHERE 
    users_dim.user_key = cu.user_key
    AND users_dim.current_flag = TRUE
