CREATE TABLE hive.transfer_fact_csv (
    transfer_id INT,
    player_id INT,
    from_club_id INT,
    to_club_id INT,
    transfer_date DATE,
    transfer_season VARCHAR,
    transfer_fee REAL,
    market_value_in_eur REAL,
    highest_market_value_in_eur REAL,
    player_age_at_transfer REAL,
    transfer_profit_loss REAL,
    transfer_fee_ratio REAL,
    club_net_spend REAL,
    from_club_goals INT,
    from_club_assists INT, 
    from_club_total_minutes INT
)
WITH (
    external_location = 'hdfs://namenode/Star_schema/TransferFact_csv',
    format = 'CSV',
    csv_separator = ','
);
