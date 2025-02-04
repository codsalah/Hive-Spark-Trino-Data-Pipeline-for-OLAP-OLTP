import os
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, when, year, sum
from pyspark.sql.functions import col, sum, coalesce, lit, year

# Import helper functions from your utilities module.
# Adjust the module path as needed.
from SparkDataExploration.utils import create_spark_session, read_csv, print_table, save_to_multiple_formats

class TransferFact:
    def __init__(self, spark):
        self.spark = spark

    def read_csvs(self, transfer_path, players_path, appearances_path):
        """
        Reads the input CSV files using the helper read_csv function.
        """
        self.transfers_df = read_csv(self.spark, transfer_path)
        self.players_df = read_csv(self.spark, players_path)
        self.appearances_df = read_csv(self.spark, appearances_path)

    def player_appearances_per_club(self, column_name, club_type):
        """
        Aggregates a performance metric (e.g., goals, assists, minutes_played)
        by player and club.
        """
        df = self.appearances_df.groupBy("player_id", club_type).agg(
            sum(col(column_name)).alias("total_" + column_name + "_for_player_in_club")
        )
        return df
    ############################### Net Club ##############################################
    def join_from_club(self):
        return self.transfers_df \
            .withColumn("year", year("transfer_date")) \
            .groupBy("from_club_id", "year") \
            .agg(
                sum(coalesce(col("transfer_fee"), lit(0))).alias("total_income")
            ) \
            .withColumnRenamed("from_club_id", "club_id")
    
    def join_to_club(self):
        return self.transfers_df \
            .withColumn("year", year("transfer_date")) \
            .groupBy("to_club_id", "year") \
            .agg(
                sum(coalesce(col("transfer_fee"), lit(0))).alias("total_expenditure")  # Fixed column name
            ) \
            .withColumnRenamed("to_club_id", "club_id")
    
    def net_club(self):
        from_club = self.join_from_club()
        to_club = self.join_to_club()

        return from_club.join(
            to_club,
            ["club_id", "year"],
            "full_outer"
        ).fillna(0, subset=["total_income", "total_expenditure"]) \
         .withColumn(
             "net_transfer_record",
             col("total_income") - col("total_expenditure")
         ) \
         .select("club_id", "year", "net_transfer_record")

    def get_transfer_fact(self):
        # Get net club records first
        net_club_df = self.net_club()

        # Existing aggregations
        from_club_goals = self.player_appearances_per_club("goals", "player_club_id").alias("from_club_goals")
        to_club_goals = self.player_appearances_per_club("goals", "player_current_club_id").alias("to_club_goals")
        from_club_assists = self.player_appearances_per_club("assists", "player_club_id").alias("from_club_assists")
        to_club_assists = self.player_appearances_per_club("assists", "player_current_club_id").alias("to_club_assists")
        from_club_total_minutes = self.player_appearances_per_club("minutes_played", "player_club_id").alias("from_club_total_minutes")
        to_club_total_minutes = self.player_appearances_per_club("minutes_played", "player_current_club_id").alias("to_club_total_minutes")

        # Main fact table construction
        transfer_fact_df = self.transfers_df.alias("t") \
            .join(self.players_df.alias("p"), "player_id", "left") \
            .join(from_club_goals, (col("t.player_id") == col("from_club_goals.player_id")) & 
                  (col("t.from_club_id") == col("from_club_goals.player_club_id")), "left") \
            .join(to_club_goals, (col("t.player_id") == col("to_club_goals.player_id")) & 
                  (col("t.to_club_id") == col("to_club_goals.player_current_club_id")), "left") \
            .join(from_club_assists, (col("t.player_id") == col("from_club_assists.player_id")) & 
                  (col("t.from_club_id") == col("from_club_assists.player_club_id")), "left") \
            .join(to_club_assists, (col("t.player_id") == col("to_club_assists.player_id")) & 
                  (col("t.to_club_id") == col("to_club_assists.player_current_club_id")), "left") \
            .join(from_club_total_minutes, (col("t.player_id") == col("from_club_total_minutes.player_id")) & 
                  (col("t.from_club_id") == col("from_club_total_minutes.player_club_id")), "left") \
            .join(to_club_total_minutes, (col("t.player_id") == col("to_club_total_minutes.player_id")) & 
                  (col("t.to_club_id") == col("to_club_total_minutes.player_current_club_id")), "left") \
            .withColumn("transfer_year", year(col("t.transfer_date"))) \
            .join(net_club_df.alias("from_net"), 
                  (col("t.from_club_id") == col("from_net.club_id")) & 
                  (col("transfer_year") == col("from_net.year")), 
                  "left") \
            .join(net_club_df.alias("to_net"), 
                  (col("t.to_club_id") == col("to_net.club_id")) & 
                  (col("transfer_year") == col("to_net.year")), 
                  "left") \
            .withColumn("player_age_at_transfer", year(col("t.transfer_date")) - year(col("p.date_of_birth"))) \
            .withColumn("transfer_profit_loss", col("t.transfer_fee") - col("t.market_value_in_eur")) \
            .withColumn("transfer_fee_ratio", when(col("t.market_value_in_eur") > 0, 
                                                   col("t.transfer_fee") / col("t.market_value_in_eur"))) \
            .withColumn("net_transfer", col("from_net.net_transfer_record") - col("to_net.net_transfer_record")) \
            .select(
                col("t.player_id"),
                col("t.from_club_id").cast("string").alias("from_club_id"),
                col("t.to_club_id").cast("string").alias("to_club_id"),
                col("t.transfer_date"),
                col("t.transfer_season"),
                col("t.transfer_fee"),
                col("t.market_value_in_eur"),
                col("p.highest_market_value_in_eur"),
                col("player_age_at_transfer"),
                col("transfer_profit_loss"),
                col("transfer_fee_ratio"),
                col("from_club_goals.total_goals_for_player_in_club").alias("total_goals_in_previous_club"),
                col("to_club_goals.total_goals_for_player_in_club").alias("total_goals_in_current_club"),
                col("from_club_assists.total_assists_for_player_in_club").alias("total_assists_in_previous_club"),
                col("to_club_assists.total_assists_for_player_in_club").alias("total_assists_in_current_club"),
                col("from_club_total_minutes.total_minutes_played_for_player_in_club").alias("total_minutes_in_previous_club"),
                col("to_club_total_minutes.total_minutes_played_for_player_in_club").alias("total_minutes_in_current_club"),
                col("from_net.net_transfer_record").alias("from_club_net"),  # Net for selling club
                col("to_net.net_transfer_record").alias("to_club_net"),     # Net for buying club
                col("net_transfer")  # Difference between from_club_net and to_club_net
            )
        return transfer_fact_df

if __name__ == "__main__":
    # Create Spark session using the dynamic helper function.
    spark = create_spark_session("Transfer Fact Processing")

    # Define input file paths.
    transfer_path = "/home/codsalah/Downloads/archive/transfers.csv"
    players_path = "/home/codsalah/Downloads/archive/players.csv"
    appearances_path = "/home/codsalah/Downloads/archive/appearances.csv"

    # Read input data and process the fact table.
    transfer_fact_processor = TransferFact(spark)
    transfer_fact_processor.read_csvs(transfer_path, players_path, appearances_path)
    transfer_fact_df = transfer_fact_processor.get_transfer_fact()

    # Display the resulting fact table.
    print_table(transfer_fact_df, "Transfer Fact")

    # Save the fact table in multiple formats.
    output_path = os.path.abspath("./DataSchema/Star_schema")
    save_to_multiple_formats(transfer_fact_df, "TransferFact", output_path)

    # Stop the Spark session.
    spark.stop()