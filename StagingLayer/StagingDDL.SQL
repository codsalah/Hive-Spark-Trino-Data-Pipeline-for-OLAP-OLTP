CREATE DATABASE Staging_transfer;
GO

USE Staging_transfer;

CREATE TABLE appearances (
  appearance_id VARCHAR(255) NOT NULL,
  game_id INT DEFAULT NULL,
  player_id INT DEFAULT NULL,
  player_club_id INT DEFAULT NULL,
  player_current_club_id INT DEFAULT NULL,
  date DATE DEFAULT NULL,
  player_name VARCHAR(255) DEFAULT NULL,
  competition_id VARCHAR(255) NOT NULL,
  yellow_cards INT DEFAULT NULL,
  red_cards INT DEFAULT NULL,
  goals INT DEFAULT NULL,
  assists INT DEFAULT NULL,
  minutes_played INT DEFAULT NULL,
  PRIMARY KEY (appearance_id)
);

CREATE TABLE club_games (
  club_games_id INT NOT NULL AUTO_INCREMENT,
  game_id INT DEFAULT NULL,
  club_id INT DEFAULT NULL,
  own_goals INT DEFAULT NULL,
  own_position VARCHAR(255) DEFAULT NULL,
  own_manager_name VARCHAR(255) DEFAULT NULL,
  opponent_id INT DEFAULT NULL,
  opponent_goals INT DEFAULT NULL,
  opponent_position VARCHAR(255) DEFAULT NULL,
  opponent_manager_name VARCHAR(255) DEFAULT NULL,
  hosting VARCHAR(255) DEFAULT NULL,
  is_win TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (club_games_id)
);

CREATE TABLE clubs (
  club_id INT NOT NULL,
  club_code VARCHAR(50) DEFAULT NULL,
  name VARCHAR(255) DEFAULT NULL,
  domestic_competition_id VARCHAR(255) DEFAULT NULL,
  total_market_value FLOAT DEFAULT NULL,
  squad_size INT DEFAULT NULL,
  average_age FLOAT DEFAULT NULL,
  foreigners_number INT DEFAULT NULL,
  foreigners_percentage FLOAT DEFAULT NULL,
  national_team_players INT DEFAULT NULL,
  stadium_name VARCHAR(255) DEFAULT NULL,
  stadium_seats INT DEFAULT NULL,
  net_transfer_record VARCHAR(255) DEFAULT NULL,
  coach_name VARCHAR(255) DEFAULT NULL,
  last_season VARCHAR(50) DEFAULT NULL,
  filename VARCHAR(255) DEFAULT NULL,
  url VARCHAR(500) DEFAULT NULL,
  PRIMARY KEY (club_id)
);

CREATE TABLE competitions (
  competition_id VARCHAR(255) NOT NULL,
  competition_code VARCHAR(50) DEFAULT NULL,
  name VARCHAR(255) DEFAULT NULL,
  sub_type VARCHAR(255) DEFAULT NULL,
  type VARCHAR(255) DEFAULT NULL,
  country_id INT DEFAULT NULL,
  country_name VARCHAR(255) DEFAULT NULL,
  domestic_league_code VARCHAR(50) DEFAULT NULL,
  confederation VARCHAR(255) DEFAULT NULL,
  url VARCHAR(500) DEFAULT NULL,
  is_major_national_league TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (competition_id)
);

CREATE TABLE game_events (
  game_event_id VARCHAR(255) NOT NULL,
  date DATE DEFAULT NULL,
  game_id INT DEFAULT NULL,
  minute INT DEFAULT NULL,
  type VARCHAR(255) DEFAULT NULL,
  club_id INT DEFAULT NULL,
  player_id INT DEFAULT NULL,
  description VARCHAR(500) DEFAULT NULL,
  player_in_id INT DEFAULT NULL,
  player_assist_id INT DEFAULT NULL,
  PRIMARY KEY (game_event_id)
);

CREATE TABLE game_lineups (
  game_lineups_id VARCHAR(255) NOT NULL,
  date DATE DEFAULT NULL,
  game_id INT DEFAULT NULL,
  player_id INT DEFAULT NULL,
  club_id INT DEFAULT NULL,
  player_name VARCHAR(255) DEFAULT NULL,
  type VARCHAR(255) DEFAULT NULL,
  position VARCHAR(50) DEFAULT NULL,
  number VARCHAR(25) DEFAULT NULL,
  team_captain TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (game_lineups_id)
);

CREATE TABLE games (
  game_id INT NOT NULL,
  competition_id VARCHAR(255) DEFAULT NULL,
  season VARCHAR(50) DEFAULT NULL,
  round VARCHAR(255) DEFAULT NULL,
  date DATE DEFAULT NULL,
  home_club_id VARCHAR(255) DEFAULT NULL,
  away_club_id VARCHAR(255) DEFAULT NULL,
  home_club_goals VARCHAR(255) DEFAULT NULL,
  away_club_goals VARCHAR(255) DEFAULT NULL,
  home_club_position VARCHAR(50) DEFAULT NULL,
  away_club_position VARCHAR(50) DEFAULT NULL,
  home_club_manager_name VARCHAR(255) DEFAULT NULL,
  away_club_manager_name VARCHAR(255) DEFAULT NULL,
  stadium VARCHAR(255) DEFAULT NULL,
  attendance VARCHAR(255) DEFAULT NULL,
  referee VARCHAR(255) DEFAULT NULL,
  url VARCHAR(500) DEFAULT NULL,
  home_club_formation VARCHAR(50) DEFAULT NULL,
  away_club_formation VARCHAR(50) DEFAULT NULL,
  home_club_name VARCHAR(255) DEFAULT NULL,
  away_club_name VARCHAR(255) DEFAULT NULL,
  aggregate VARCHAR(255) DEFAULT NULL,
  competition_type VARCHAR(100) DEFAULT NULL,
  PRIMARY KEY (game_id)
);

CREATE TABLE player_valuations (
  player_id BIGINT NOT NULL,
  date TEXT,
  market_value_in_eur BIGINT DEFAULT NULL,
  current_club_id BIGINT DEFAULT NULL,
  player_club_domestic_competition_id TEXT,
  PRIMARY KEY (player_id, date) -- Ensures unique records for each player's valuation at a given date
);

CREATE TABLE players (
  player_id INT NOT NULL,
  first_name VARCHAR(255) DEFAULT NULL,
  last_name VARCHAR(255) DEFAULT NULL,
  name VARCHAR(255) DEFAULT NULL,
  last_season VARCHAR(50) DEFAULT NULL,
  current_club_id INT DEFAULT NULL,
  player_code VARCHAR(50) DEFAULT NULL,
  country_of_birth VARCHAR(255) DEFAULT NULL,
  city_of_birth VARCHAR(255) DEFAULT NULL,
  country_of_citizenship VARCHAR(255) DEFAULT NULL,
  date_of_birth DATE DEFAULT NULL,
  sub_position VARCHAR(100) DEFAULT NULL,
  position VARCHAR(100) DEFAULT NULL,
  foot VARCHAR(50) DEFAULT NULL,
  height_in_cm INT DEFAULT NULL,
  contract_expiration_date DATE DEFAULT NULL,
  agent_name VARCHAR(255) DEFAULT NULL,
  image_url VARCHAR(500) DEFAULT NULL,
  url VARCHAR(500) DEFAULT NULL,
  current_club_domestic_competition_id VARCHAR(255) DEFAULT NULL,
  current_club_name VARCHAR(255) DEFAULT NULL,
  market_value_in_eur FLOAT DEFAULT NULL,
  highest_market_value_in_eur FLOAT DEFAULT NULL,
  PRIMARY KEY (player_id)
);

CREATE TABLE transfer (
  transfer_id INT NOT NULL AUTO_INCREMENT,
  player_id INT NOT NULL,
  transfer_date DATE NOT NULL,
  transfer_season VARCHAR(50) DEFAULT NULL,
  from_club_id INT DEFAULT NULL,
  to_club_id INT DEFAULT NULL,
  from_club_name VARCHAR(255) DEFAULT NULL,
  to_club_name VARCHAR(255) DEFAULT NULL,
  transfer_fee FLOAT DEFAULT NULL,
  market_value_in_eur FLOAT DEFAULT NULL,
  player_name VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (transfer_id),
  FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);
