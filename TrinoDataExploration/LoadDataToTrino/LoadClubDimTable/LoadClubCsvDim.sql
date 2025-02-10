CREATE TABLE hive.club_dim_csv (
    club_id INT,
    name VARCHAR,
    squad_size INT,
    average_age DOUBLE,
    foreigners_number INT,
    foreigners_percentage DOUBLE,
    national_team_players INT,
    last_season INT
)
WITH (
    external_location = 'hdfs://namenode/Star_schema/ClubDim',
    format = 'CSV',
    csv_separator = ','
);
