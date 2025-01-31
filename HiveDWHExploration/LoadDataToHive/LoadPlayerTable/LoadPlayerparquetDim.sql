CREATE EXTERNAL TABLE players_dim_parquet (
    player_id INT,
    name STRING,
    country_of_birth STRING,
    date_of_birth DATE,
    position STRING,
    sub_position STRING,
    last_season INT,
    height_in_cm INT,
    current_club_id INT,
    current_club_name STRING,
    country_of_citizenship STRING,
    agent_name STRING,
    image_url STRING,
    url STRING,
    goals INT,
    assists INT,
    minutes_played INT,
    player_club_domestic_competition_id STRING
)
STORED AS PARQUET
LOCATION '/ParquetStore/PlayersDim';