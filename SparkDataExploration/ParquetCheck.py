from pyspark.sql import SparkSession
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Path to the Star Schema directory
base_path = "/path/to/DataSchema/Star_schema"

# List all parquet directories
parquet_dirs = [
    "ClubDim_parquet",
    "CompetitionDim_parquet",
    "PlayersDim_parquet",
    "TransferFact_parquet"
]

# Read all parquet files and store them in a dictionary
dfs = {dir_name: spark.read.parquet(os.path.join(base_path, dir_name)) for dir_name in parquet_dirs}

# Show data from each DataFrame
for name, df in dfs.items():
    print(f"\n{name} Data:")
    df.show(5)  # Show first 5 rows