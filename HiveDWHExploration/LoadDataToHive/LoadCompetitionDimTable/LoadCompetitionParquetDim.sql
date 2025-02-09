CREATE EXTERNAL TABLE competition_dim_parquet (
    competition_sk INT,
    competition_id STRING,
    name STRING,
    type STRING,
    country_name STRING,
    confederation STRING,
    is_major_national_league BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode/Star_schema/CompetitionDim';
