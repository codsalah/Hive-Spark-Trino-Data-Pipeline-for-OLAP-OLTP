from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, current_date
from pyspark.sql.functions import col
from utils import read_csv, create_spark_session, schema_insights, print_header, print_table

def get_specific_team(df, team_name):
    return df.filter(col("to_club_name").like(f"%{team_name}%"))

def transfer_report(spark, transfers_path, team_name=None):
    # Read the CSV into a DataFrame
    transfers_df = read_csv(spark, transfers_path)

    # Print schema insights
    print_header("Schema Insights")
    schema_insights(transfers_df)

    # Filter by team_name using get_specific_team if provided
    if team_name:
        transfers_df = get_specific_team(transfers_df, team_name)

    # Top 10 players by market value
    top_market_value = transfers_df.orderBy(col("market_value_in_eur").desc()).select("player_name", "market_value_in_eur")

    print_header("Top 10 Players by Market Value")
    print_table(top_market_value, "Top 10 Players by Market Value")

    # Top players by value in each team
    top_players_by_team = transfers_df.groupBy("to_club_name").agg(
        max("market_value_in_eur").alias("max_market_value"),
        min("market_value_in_eur").alias("min_market_value"),
        mean("market_value_in_eur").alias("mean_market_value")
    )

    print_header(f"Top Players by Team: {team_name if team_name else 'All Teams'}")
    print_table(top_players_by_team, "Top Players by Team")

if __name__ == "__main__":
    spark = create_spark_session()
    transfers_path = "/home/codsalah/Downloads/archive/transfers.csv"
    
    team_name = "Barcelona" 
    
    transfer_report(spark, transfers_path, team_name)
    spark.stop()
