CREATE TABLE hive.players_dim_csv (
    player_sk BIGINT,
    player_id INT,
    name VARCHAR,
    country_of_birth VARCHAR,
    position VARCHAR,
    sub_position VARCHAR,
    last_season INT,
    height_in_cm INT,
    current_club_id INT,
    current_club_name VARCHAR,
    country_of_citizenship VARCHAR,
    agent_name VARCHAR,
    image_url VARCHAR,
    url VARCHAR,
    total_goals INT,
    total_assists INT,
    total_minutes_played INT,
    year INT,
    player_club_domestic_competition_id VARCHAR,
    date_of_birth DATE
)
WITH (
    external_location = 'hdfs://namenode/Star_schema/PlayersDim',
    format = 'CSV',
    csv_separator = ','
);
