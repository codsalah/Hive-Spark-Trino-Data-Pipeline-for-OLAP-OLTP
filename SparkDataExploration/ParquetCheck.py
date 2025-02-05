from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("ParquetCheck") \
    .getOrCreate()

# Replace this with the folder you want to check at a time
club_dim_path = "DataSchema/Star_schema/ClubDim_parquet"

# Read the Parquet files
club_dim_df = spark.read.parquet(club_dim_path)

print("ClubDim Schema:")
club_dim_df.printSchema()

print("\nClubDim Data Sample:")
club_dim_df.show(5, truncate=False)

print(f"\nTotal rows in ClubDim: {club_dim_df.count()}")