CREATE TABLE hive.competition_dim_csv (
    competition_sk INT,
    competition_id VARCHAR,
    name VARCHAR,
    type VARCHAR,
    country_name VARCHAR,
    confederation VARCHAR,
    is_major_national_league BOOLEAN
)
WITH (
    external_location = 'hdfs://namenode/Star_schema/CompetitionDim',
    format = 'CSV',
    csv_separator = ','
);
