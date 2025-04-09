CREATE TABLE direction_dim (
    direction_key          SERIAL PRIMARY KEY,
    direction              VARCHAR(8) UNIQUE,

    CHECK (direction IN ('inbound', 'outbound'))
);
