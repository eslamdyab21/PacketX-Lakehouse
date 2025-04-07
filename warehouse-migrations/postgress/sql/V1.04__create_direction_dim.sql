CREATE TABLE direction_dim (
    direction_key          SERIAL PRIMARY KEY,
    direction              VARCHAR(8),

    CHECK (direction IN ('inbound', 'outbound'))
);
