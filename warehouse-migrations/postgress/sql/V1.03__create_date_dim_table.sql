create table date_dim(
    date_key        INT NOT NULL PRIMARY KEY,
    date            DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month           SMALLINT NOT NULL,
    week            SMALLINT NOT NULL,
    day             SMALLINT NOT NULL,
    hour            SMALLINT NOT NULL,
    is_weekend      BOOLEAN
);