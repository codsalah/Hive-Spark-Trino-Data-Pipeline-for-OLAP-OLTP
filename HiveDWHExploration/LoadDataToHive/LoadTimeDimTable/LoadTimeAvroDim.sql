CREATE EXTERNAL TABLE time_dim_avro (
    time_id INT,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT
)
STORED AS AVRO
LOCATION '/AvroStore/TimeDim';