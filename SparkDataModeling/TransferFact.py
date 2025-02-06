import os
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, when, year, sum, coalesce, lit
from pyspark.sql import functions as F
# Import helper functions from your utilities module.
# # Adjust the module path as needed.
from SparkDataExploration.utils import create_spark_session, read_csv, print_table, save_to_multiple_formats


class TransferFact:
    def __init__(self, spark):
        self.spark = spark

    def extract_data(self, transfer_path, players_path, clubs_path, competitions_path, time_path):
        self.transfers_df = self.spark.read.csv(transfer_path, header=True, inferSchema=True)
        self.players_df = self.spark.read.csv(players_path, header=True, inferSchema=True)
        self.clubs_df = self.spark.read.csv(clubs_path, header=True, inferSchema=True)
        self.competitions_df = self.spark.read.csv(competitions_path, header=True, inferSchema=True)
        self.time_df = self.spark.read.csv(time_path, header=True, inferSchema=True)

    def player_appearances_per_club(self, column_name, club_type):
        return (
        self.players_df.alias("p")
        .join(self.transfers_df.alias("t"), col("p.player_id") == col("t.player_id"), "left")
        .join(self.clubs_df.alias("c"), col("t." + club_type) == col("c.club_id"), "left")
        .groupBy("p.player_id", club_type)
        .agg(sum(col(column_name)).alias(f"{column_name}_for_player_in_club"))
    )


    def join_from_club(self):
        return (
            self.transfers_df
            .join(self.clubs_df, self.transfers_df.from_club_id == self.clubs_df.club_id, "left")
            .withColumn("year", year("transfer_date"))
            .groupBy("from_club_id", "year")
            .agg(sum(coalesce(col("transfer_fee"), lit(0))).alias("total_income"))
            .withColumnRenamed("from_club_id", "club_id")
        )

    def join_to_club(self):
        return (
            self.transfers_df
            .join(self.clubs_df, self.transfers_df.to_club_id == self.clubs_df.club_id, "left")
            .withColumn("year", year("transfer_date"))
            .groupBy("to_club_id", "year")
            .agg(sum(coalesce(col("transfer_fee"), lit(0))).alias("total_expenditure"))
            .withColumnRenamed("to_club_id", "club_id")
        )

    def net_club(self):
        from_club = self.join_from_club()
        to_club = self.join_to_club()

        return (
            from_club
            .join(to_club, ["club_id", "year"], "full_outer")
            .fillna(0, subset=["total_income", "total_expenditure"])
            .withColumn("net_transfer_record", col("total_income") - col("total_expenditure"))
            .select("club_id", "year", "net_transfer_record")
        )

    def transsform_data(self):
        net_club_df = self.net_club()

        from_club_goals = self.player_appearances_per_club("total_goals", "from_club_id")
        from_club_assists = self.player_appearances_per_club("total_assists", "from_club_id")
        from_club_total_minutes = self.player_appearances_per_club("total_minutes_played", "from_club_id")
        print(self.players_df.columns)  
        transfer_fact_df = (
            self.transfers_df.alias("t")
            .join(self.players_df.alias("p"), "player_id", "left")
            .join(self.clubs_df.alias("c_from"), col("t.from_club_id") == col("c_from.club_id"), "left")
            .join(self.clubs_df.alias("c_to"), col("t.to_club_id") == col("c_to.club_id"), "left")
            .join(from_club_goals.alias("from_club_goals"),
                  (col("t.player_id") == col("from_club_goals.player_id")) &
                  (col("t.from_club_id") == col("from_club_goals.from_club_id")), "left")
            .join(from_club_assists.alias("from_club_assists"),
                  (col("t.player_id") == col("from_club_assists.player_id")) &
                  (col("t.from_club_id") == col("from_club_assists.from_club_id")), "left")
            .join(from_club_total_minutes.alias("from_club_total_minutes"),
                  (col("t.player_id") == col("from_club_total_minutes.player_id")) &
                  (col("t.from_club_id") == col("from_club_total_minutes.from_club_id")), "left")
            .withColumn("transfer_year", year(col("t.transfer_date")))
            .join(net_club_df.alias("from_net"),
                  (col("t.from_club_id") == col("from_net.club_id")) & 
                  (col("transfer_year") == col("from_net.year")), "left")
            .join(net_club_df.alias("to_net"),
                  (col("t.to_club_id") == col("to_net.club_id")) & 
                  (col("transfer_year") == col("to_net.year")), "left")
            .withColumn("player_age_at_transfer", col("transfer_year") - year(col("p.date_of_birth").cast("timestamp")))
            .withColumn("net_transfer", col("from_net.net_transfer_record") - col("to_net.net_transfer_record"))
            .groupBy("t.player_id", "transfer_year")
            .agg(
                sum(col("t.transfer_fee")).alias("total_transfer_fee"),
                sum(col("t.market_value_in_eur")).alias("total_market_value")
            )
            .withColumn("transfer_profit_loss", col("total_transfer_fee") - col("total_market_value"))
            .withColumn("transfer_fee_ratio",
                when(col("total_market_value") > 0, col("total_transfer_fee") / col("total_market_value"))
                .otherwise(None))
        )

        return transfer_fact_df

    def load_data(self, df, output_path):
        save_to_multiple_formats(df, "TransferFact")
        print(f"Data successfully saved at: {output_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Transfer Fact Processing").getOrCreate()
    
    transfer_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/transfers.csv"
    players_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/Star_schema/PlayersDim_csv/part-00000-deb394d1-38e4-449b-9535-c011b0606f53-c000.csv"
    clubs_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/Star_schema/ClubDim_csv/part-00000-672bd8d7-0931-4e19-adac-1ad282229cc0-c000.csv"
    competitions_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/Star_schema/CompetitionDim_csv/part-00000-c13b3388-4d0a-4f0e-9660-11f1846ff2ee-c000.csv"
    time_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/Star_schema/TimeDim_csv/part-00000-9f93db38-57ea-44d5-bd8b-55a889650684-c000.csv"

    transfer_fact_processor = TransferFact(spark)
    transfer_fact_processor.extract_data(transfer_path, players_path, clubs_path, competitions_path, time_path)
   
    transfer_fact_df = transfer_fact_processor.transsform_data()

    transfer_fact_df.show(10, truncate=False)

    output_path = "file:///root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/FactTable"
    transfer_fact_processor.load_data(transfer_fact_df, output_path)

    spark.stop()
