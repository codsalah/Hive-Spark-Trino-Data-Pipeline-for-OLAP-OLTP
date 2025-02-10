CREATE TABLE hive.time_dim_csv (
    time_id INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
)
WITH (
    external_location = 'hdfs://namenode/Star_schema/TimeDim',
    format = 'CSV',
    csv_separator = ','
);
