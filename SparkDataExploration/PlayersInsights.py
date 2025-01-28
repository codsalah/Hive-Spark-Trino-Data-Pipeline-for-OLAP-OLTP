from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, current_date
from pyspark.sql.functions import col
from utils import read_csv, create_spark_session, schema_insights, print_header, print_table


def player_report(spark, players_path):
    player_df = read_csv(spark, players_path)

    # Print schema insights
    print_header("Schema Insights")
    schema_insights(player_df)

    # Top 10 players by market value
    top_market_value = player_df.orderBy(col("market_value_in_eur").desc()).select("name", "market_value_in_eur")
    print_table(top_market_value, "Top 10 Players by Market Value")

    # Players with contracts expiring in the next 6 months
    players_with_expiring_contracts = player_df.filter(
        (datediff(col("contract_expiration_date"), current_date()) <= 180) &
        (col("contract_expiration_date").isNotNull())
    ).select("name", "contract_expiration_date")
    print_table(players_with_expiring_contracts, "Players with Contracts Expiring in the Next 6 Months")


def get_specific_player(spark, players_path, player_name):
    player_df = read_csv(spark, players_path)
    specific_player_df = player_df.filter(col("name").like(f"%{player_name}%"))
    return specific_player_df


def specific_player(spark, players_path, player_name):
    player_df = read_csv(spark, players_path)
    
    specific_player_df = get_specific_player(spark, players_path, player_name)
    print_table(specific_player_df, f"Players with '{player_name}' in their name")
    
    # Get the player's position and market value
    player_position = specific_player_df.select("name", "position", "market_value_in_eur")
    print_table(player_position, "Player's Position and Market Value")
    
    # Aggregated market value by position (mean, max, min)
    mean_market_value = player_df.groupBy("position").agg(mean("market_value_in_eur").alias("mean_market_value"))
    max_market_value = player_df.groupBy("position").agg(max("market_value_in_eur").alias("max_market_value"))
    min_market_value = player_df.groupBy("position").agg(min("market_value_in_eur").alias("min_market_value"))
    
    # Join the specific player's market value with aggregated market values by position
    position_market_value = player_position.join(mean_market_value, "position", "left") \
                                            .join(max_market_value, "position", "left") \
                                            .join(min_market_value, "position", "left")
    print_table(position_market_value, "Player's Market Value Compared to Others in Their Position")

    return specific_player_df


def compare_players(spark, players_path, player1_name, player2_name):
    player_df = read_csv(spark, players_path)
    player1_df = get_specific_player(spark, players_path, player1_name)
    player2_df = get_specific_player(spark, players_path, player2_name)

    if player1_df.count() == 0 or player2_df.count() == 0:
        return "One of the Players was not found"

    player1_df_position = player1_df.select("name", "position", "market_value_in_eur")
    player2_df_position = player2_df.select("name", "position", "market_value_in_eur")

    position1 = player1_df_position.collect()[0]["position"]
    position2 = player2_df_position.collect()[0]["position"]

    if position1 != position2:
        return "Players are not in the same position"

    if position1 == position2:
        # Aggregated statistics for players in the same position
        mean_market_value = player_df.groupBy("position").agg(mean("market_value_in_eur").alias("mean_market_value"))
        max_market_value = player_df.groupBy("position").agg(max("market_value_in_eur").alias("max_market_value"))
        min_market_value = player_df.groupBy("position").agg(min("market_value_in_eur").alias("min_market_value"))
        
        position_market_value = player1_df_position.join(mean_market_value, "position", "left") \
                                                  .join(max_market_value, "position", "left") \
                                                  .join(min_market_value, "position", "left")
        position_market_value = position_market_value.join(player2_df_position, "position", "left")
        
        print_table(position_market_value, f"Detailed Comparison Between {player1_name} and {player2_name}") 
    return


if __name__ == "__main__":
    # Initialize Spark session
    spark = create_spark_session("Players Analysis")

    # Path to the players CSV file
    players_path = "/home/codsalah/Downloads/archive/players.csv"
    
    # Analyze the players table
    player_report(spark, players_path)

    # Search for players with "Salah" in their name
    specific_player(spark, players_path, "Salah")

    # Compare players "Kylian Mbappé" and "Vinicius Junior"
    compare_players(spark, players_path, "Kylian Mbappé", "Vinicius Junior")

    # Stop the Spark session
    spark.stop()
