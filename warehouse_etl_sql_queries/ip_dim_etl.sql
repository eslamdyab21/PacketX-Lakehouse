-- Get unique Ips
WITH staging_table AS (
    SELECT source_ip AS ip FROM lakehouse_packets WHERE source_ip IS NOT NULL
    UNION
    SELECT destination_ip AS ip FROM lakehouse_packets WHERE source_ip IS NOT NULL
)


-- Upsert
INSERT INTO ip_dim (ip_address)
SELECT staging_table.ip AS ip_address FROM staging_table
LEFT JOIN ip_dim ON ip_dim.ip_address = staging_table.ip
WHERE ip_dim.ip_address IS NULL