CREATE EXTERNAL TABLE time_dim_csv (
    time_id INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/CsvStore/TimeDim';
