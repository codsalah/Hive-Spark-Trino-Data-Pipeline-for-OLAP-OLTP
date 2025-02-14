CREATE EXTERNAL TABLE transfer_fact_avro (
    transfer_id INT,
    player_id INT,
    from_club_id INT,
    to_club_id INT,
    transfer_date DATE,
    transfer_season STRING,  
    transfer_fee DOUBLE,
    market_value_in_eur DOUBLE,
    highest_market_value_in_eur DOUBLE,
    player_age_at_transfer DOUBLE,
    transfer_profit_loss DOUBLE,
    transfer_fee_ratio DOUBLE,
    club_net_spend DOUBLE,
    from_club_goals INT,
    from_club_assists INT, 
    from_club_total_minutes INT
)
STORED AS AVRO
LOCATION 'hdfs://namenode/Star_schema/TransferFact_AVRO';
