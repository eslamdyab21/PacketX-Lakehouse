CREATE TABLE ip_dim(
    ip_key              SERIAL PRIMARY KEY,
    ip_address          VARCHAR(15) UNIQUE NOT NULL
)