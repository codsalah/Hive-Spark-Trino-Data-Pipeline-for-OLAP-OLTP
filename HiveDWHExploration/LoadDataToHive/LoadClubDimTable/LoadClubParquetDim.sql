CREATE EXTERNAL TABLE club_dim_parquet (
    club_id STRING,
    name STRING,
    squad_size STRING,
    average_age DOUBLE,
    foreigners_number STRING,
    foreigners_percentage DOUBLE,
    national_team_players STRING,
    last_season STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode/Star_schema/ClubDim';