import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import IntegerType
from SparkDataExploration.utils import (
    read_csv, create_spark_session,
    print_table, save_to_multiple_formats
)

class PlayerDim:
    def __init__(self, spark):
        self.spark = spark
    
    def get_player_dim(self, player_df, appearances_df):
        player_stats = appearances_df.groupBy("player_id").agg(
            _sum("goals").alias("total_goals"),
            _sum("assists").alias("total_assists"),
            _sum("minutes_played").alias("total_minutes_played")
        )

        players_dim = player_df.join(player_stats, "player_id", how="left").select(
            col("player_id").cast(IntegerType()),
            col("name"),
            col("country_of_birth"),
            col("position"),
            col("sub_position"),
            col("last_season").cast(IntegerType()),
            col("height_in_cm").cast(IntegerType()),
            col("current_club_id").cast(IntegerType()),
            col("current_club_name"),
            col("country_of_citizenship"),
            col("agent_name"),
            col("image_url"),
            col("url"),
            col("total_goals").cast(IntegerType()),
            col("total_assists").cast(IntegerType()),
            col("total_minutes_played").cast(IntegerType()),
            col("current_club_domestic_competition_id").alias("player_club_domestic_competition_id")
        )
        
        return players_dim
    

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session("Player Dimension Processing")

    # Read input data
    player_path = "/home/codsalah/Downloads/archive/players.csv"
    appearances_path = "/home/codsalah/Downloads/archive/appearances.csv"

    player_df = read_csv(spark, player_path)
    appearances_df = read_csv(spark, appearances_path)

    # Process PlayerDim
    player_dim_processor = PlayerDim(spark)
    players_dim = player_dim_processor.get_player_dim(player_df, appearances_df)

    # Show and save results
    print_table(players_dim, "Player Dimension")
    output_path = os.path.abspath("./DataSchema/Star_schema")
    save_to_multiple_formats(players_dim, "PlayersDim", output_path)

    spark.stop()