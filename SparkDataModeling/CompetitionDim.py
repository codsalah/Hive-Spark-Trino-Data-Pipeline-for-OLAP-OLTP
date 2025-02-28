from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from SparkDataExploration.utils import (
    read_csv, create_spark_session, schema_insights, print_header,
    print_table, save_to_multiple_formats
)
import os
from pyspark.sql.types import StringType

class CompetitionDim:
    def __init__(self, spark):
        # Initialize the Spark session
        self.spark = spark
    
    def get_competition_dim(self, competition_df):
        # Add surrogate key first
        competition_dim = competition_df.withColumn("competition_sk", monotonically_increasing_id()).select(
            col("competition_sk"),  # Surrogate key first
            col("competition_id"),  # Keep natural key
            col("name"),
            col("type"),
            col("country_name"),
            col("confederation"),
            col("is_major_national_league").cast(StringType()).alias("is_major_national_league")
        )
        
        return competition_dim


# Main execution
if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session("Competition Dimension Analysis")

    # Path to the competition dataset
    competition_path = "/home/codsalah/Downloads/archive/competitions.csv"
    competition_df = read_csv(spark, competition_path)

    # Print schema insights
    competition_dim_processor = CompetitionDim(spark)
    competition_dim = competition_dim_processor.get_competition_dim(competition_df)

    # Show the extracted competition dimension in a table format
    print_table(competition_dim, "Competition Dimension")
    
    # Save the competition dimension in CSV, Parquet, and Avro formats
    output_path = os.path.abspath("./DataSchema/Star_schema") 
    save_to_multiple_formats(competition_dim, "CompetitionDim", output_path)
    
    # Stop the Spark session
    spark.stop()
