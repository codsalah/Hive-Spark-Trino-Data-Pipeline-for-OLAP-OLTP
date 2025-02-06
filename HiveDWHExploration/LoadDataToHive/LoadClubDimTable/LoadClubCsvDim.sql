CREATE EXTERNAL TABLE club_dim_csv (
    club_id INT,
    name STRING,
    squad_size INT,
    average_age DOUBLE,
    foreigners_number INT,
    foreigners_percentage DOUBLE,
    national_team_players INT,
    last_season INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/CsvStore/ClubDim';