CREATE TABLE users_dim(
    user_key              SERIAL        PRIMARY KEY,
    user_name             VARCHAR(20)   UNIQUE NOT NULL,
    local_ip              VARCHAR(15),

    -- scd type2
    start_date            TIMESTAMP     NOT NULL,
    end_date              TIMESTAMP,
    current_flag          BOOLEAN       NOT NULL
)