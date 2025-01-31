from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from SparkDataExploration.utils import (
    read_csv, create_spark_session, schema_insights, print_header,
    print_table, save_to_multiple_formats
)
import os
from pyspark.sql.types import StringType

class ClubsDim:
    def __init__(self, spark):
        # Initialize the Spark session
        self.spark = spark
    
    def get_clubs_dim(self, clubs_df):
        # Extract the clubs dimension
        clubs_dim = clubs_df.select(
            col("club_id").cast(StringType()),
            col("name"),
            col("squad_size").cast(StringType()),
            col("average_age").cast(StringType()),
            col("foreigners_number").cast(StringType()),
            col("foreigners_percentage").cast(StringType()),
            col("national_team_players").cast(StringType()),
            col("last_season").cast(StringType())
        )
        return clubs_dim


# Main execution
if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session("Club Dimension Transformation")

    # Path to the clubs dataset
    clubs_path = "/home/codsalah/Downloads/archive/clubs.csv"
    clubs_df = read_csv(spark, clubs_path)

    # Print schema insights
    clubs_dim_processor = ClubsDim(spark)
    clubs_dim = clubs_dim_processor.get_clubs_dim(clubs_df)

    # Show the extracted clubs dimension in a table format
    print_table(clubs_dim, "Club Dimension")
    
    # Save the clubs dimension in CSV, Parquet, and Avro formats
    output_path = os.path.abspath("./DataSchema/Star_schema")
    save_to_multiple_formats(clubs_dim, "ClubDim", output_path)
    
    # Stop the Spark session
    spark.stop()
