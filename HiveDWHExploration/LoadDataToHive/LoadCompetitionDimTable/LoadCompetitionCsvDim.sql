CREATE EXTERNAL TABLE competition_dim_csv (
    competition_sk INT,
    competition_id STRING,
    name STRING,
    type STRING,
    country_name STRING,
    confederation STRING,
    is_major_national_league BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','  -- Delimiter for CSV
STORED AS TEXTFILE
LOCATION 'hdfs://namenode/Star_schema/CompetitionDim/';