-- Prepare New batch data
WITH staging_table AS (
    SELECT 
        u.user_key,
        ips.ip_key as source_ip_key,
        ipd.ip_key as dest_ip_key,
        CAST(strftime(time_hour, '%Y%m%d%H') AS INTEGER) AS date_key,
        CASE 
            WHEN lp.source_ip = u.local_ip THEN d_in.direction_key
            ELSE d_out.direction_key
        END AS direction_key,
        bandwidth_kb_sum AS kb_bandwidth
    FROM 
        lakehouse_packets lp
    LEFT JOIN users_dim u         ON lp.user           = u.user_name AND u.current_flag = TRUE
    LEFT JOIN ip_dim ips          ON lp.source_ip      = ips.ip_address
    LEFT JOIN ip_dim ipd          ON lp.destination_ip = ipd.ip_address
    LEFT JOIN direction_dim d_in  ON d_in.direction    = 'inbound'
    LEFT JOIN direction_dim d_out ON d_out.direction   = 'outbound'
),


-- Compare with existing saved data to insert only new packets
new_packets AS (
    SELECT 
        staging_table.user_key,
        staging_table.source_ip_key,
        staging_table.dest_ip_key,
        staging_table.date_key,
        staging_table.direction_key,
        staging_table.kb_bandwidth
    FROM 
        staging_table
    LEFT JOIN packets_fact ON packets_fact.date_key = staging_table.date_key
                          AND packets_fact.user_key = staging_table.user_key
    WHERE packets_fact.date_key IS NULL
)


INSERT INTO packets_fact (
    user_key, 
    source_ip_key, 
    dest_ip_key, 
    date_key, 
    direction_key, 
    kb_bandwidth
)
SELECT * FROM new_packets