CREATE EXTERNAL TABLE players_dim_csv (
    player_sk BIGINT,
    player_id INT,
    name STRING,
    country_of_birth STRING,
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
    total_goals INT,
    total_assists INT,
    total_minutes_played INT,
    year INT,
    player_club_domestic_competition_id STRING,
    date_of_birth DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode/Star_schema/PlayersDim';