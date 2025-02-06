CREATE EXTERNAL TABLE time_dim_parquet (
    time_id INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
)
STORED AS PARQUET
LOCATION '/ParquetStore/TimeDim';