CREATE EXTERNAL TABLE club_dim_parquet (
    club_id INT,
    name STRING,
    squad_size INT,
    average_age DOUBLE,
    foreigners_number INT,
    foreigners_percentage DOUBLE,
    national_team_players INT,
    last_season INT
)
STORED AS PARQUET
LOCATION 'hdfs://namenode/Star_schema/ClubDim';
