
CREATE TABLE raw_data.transactions_raw (
    cc_num          BIGINT,
    merchant        VARCHAR(255),
    category        VARCHAR(100),
    amt             DOUBLE PRECISION,
    first           VARCHAR(100),
    last            VARCHAR(100),
    gender          VARCHAR(10),
    street          VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip             INTEGER,
    lat             DOUBLE PRECISION,
    long            DOUBLE PRECISION,
    city_pop        INTEGER,
    job             VARCHAR(255),
    merch_lat       DOUBLE PRECISION,
    merch_long      DOUBLE PRECISION,
    is_fraud        INTEGER,
    trans_date      DATE,
    trans_hour      INTEGER,
    trans_month     INTEGER,
    trans_year      INTEGER,
    trans_dayofweek INTEGER,
    age             INTEGER
);

-- الكود الي هيجب الداتا من  s3  ويحطها في  redshift

COPY raw_data.transactions_raw
FROM 's3://silver-financial-data-ahmed/processed/'
IAM_ROLE default
FORMAT AS PARQUET;
