INSERT INTO direction_dim (direction)
SELECT direction
FROM (SELECT 'inbound' AS direction
      UNION
      SELECT 'outbound' AS direction) AS new_directions
WHERE NOT EXISTS (
    SELECT 1 FROM direction_dim 
    WHERE direction = new_directions.direction
)