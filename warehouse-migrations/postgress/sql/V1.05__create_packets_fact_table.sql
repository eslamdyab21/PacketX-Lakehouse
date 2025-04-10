CREATE TABLE packets_fact (
    packets_key          SERIAL PRIMARY KEY,
    user_key             INT REFERENCES users_dim(user_key),
    source_ip_key        INT REFERENCES ip_dim(ip_key),
    destination_ip_key   INT REFERENCES ip_dim(ip_key),
    external_ip_key      INT REFERENCES ip_dim(ip_key),
    date_key             INT REFERENCES date_dim(date_key),
    direction_key        INT REFERENCES direction_dim(direction_key),
    kb_bandwidth         FLOAT
)