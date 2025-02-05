from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, quarter, month, dayofmonth, monotonically_increasing_id
import sys
import os
sys.path.append('/root/Hive-Based_TransferMarket_Data_Modeling/SparkDataExploration')
from pyspark.sql.types import StringType

class TimeDim:
    def __init__(self, spark, transfer_path, output_path):
        # Initialize the Spark session and file paths
        self.spark = spark
        self.transfer_path = transfer_path
        self.output_path = output_path

    def load_transfer_data(self):
        # Load transfer data from the Transfer file
        return self.spark.read.csv(self.transfer_path, header=True, inferSchema=True)

    def get_time_dim(self, transfer_df):
        """Create the Time Dimension based on transfer data"""
        # Select and create necessary columns for the time dimension
        time_dim = transfer_df.select(
            col("transfer_date").alias("full_date").cast(StringType()),
            year(col("transfer_date")).alias("year").cast(StringType()),
            quarter(col("transfer_date")).alias("quarter").cast(StringType()),
            month(col("transfer_date")).alias("month").cast(StringType()),
            dayofmonth(col("transfer_date")).alias("day").cast(StringType())
        ).distinct()

        # Add the surrogate key as the first column
        time_dim = time_dim.withColumn("time_id", monotonically_increasing_id().cast(StringType()))

        # Reorder columns to place the surrogate key first
        time_dim = time_dim.select(
            col("time_id"),  # Surrogate key first
            col("full_date"),
            col("year"),
            col("quarter"),
            col("month"),
            col("day")
        )
        return time_dim

    def save_time_dimension(self, time_dim):
        # Save the Time Dimension to the specified output path
        save_to_multiple_formats(time_dim, "TimeDim", self.output_path)

    def transform(self):
        # Load, transform and save the Time Dimension
        transfer_df = self.load_transfer_data()
        time_dim = self.get_time_dim(transfer_df)
        self.save_time_dimension(time_dim)
        self.show_time_dimension(time_dim)

    def show_time_dimension(self, time_dim):
        # Print the time dimension table
        print_table(time_dim, "Time Dimension")


# Main execution
if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session("Time Dimension Transformation")

    # Path to the transfer data
    transfer_path = "/home/codsalah/Downloads/archive/transfers.csv"
    output_path = "/root/Hive-Based_TransferMarket_Data_Modeling/DataSchema/Star_schema"

    # Create an instance of TimeDim class and run transformation
    time_dim_processor = TimeDim(spark, transfer_path, output_path)
    time_dim_processor.transform()

    spark.stop()
