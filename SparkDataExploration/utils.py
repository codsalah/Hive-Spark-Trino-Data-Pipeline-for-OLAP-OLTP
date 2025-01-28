from pyspark.sql import SparkSession
from fastavro import writer
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType
import os


def create_spark_session(app_name="Data Analysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_csv(spark, path, header=True, inferSchema=True):
    df = spark.read.csv(path, header=header, inferSchema=inferSchema)
    return df


def save_to_multiple_formats(df, base_name, relative_path="../DataSchema/StarSchema"):
    # Get the absolute path relative to the current script's directory
    absolute_path = os.path.abspath(os.path.join(os.path.dirname(__file__), relative_path))
    
    # Ensure the directory exists
    os.makedirs(absolute_path, exist_ok=True)

    # Construct file paths
    csv_path = os.path.join(absolute_path, f"{base_name}_csv")
    df.write.csv(csv_path, mode='overwrite', header=True)
    print(f"Data saved as CSV: {csv_path}")

    parquet_path = os.path.join(absolute_path, f"{base_name}_parquet")
    df.write.parquet(parquet_path, mode='overwrite')
    print(f"Data saved as Parquet: {parquet_path}")

    avro_path = os.path.join(absolute_path, f"{base_name}_avro")
    save_as_avro(df, avro_path)
    print(f"Data saved as Avro: {avro_path}")


def save_as_avro(df, avro_file_path):
    # Convert boolean columns to string
    for col_name, dtype in df.dtypes:
        if dtype == "boolean":
            df = df.withColumn(col_name, col(col_name).cast(StringType()))

    # Create the Avro schema based on the DataFrame schema
    schema = {
        "type": "record",
        "name": "AvroSchema",
        "fields": [
            {
                "name": col,
                "type": get_avro_type(dtype)
            }
            for col, dtype in df.dtypes
        ]
    }

    # Convert DataFrame to a list of records
    records = df.toPandas().to_dict(orient="records")

    # Write records to Avro file
    with open(avro_file_path, "wb") as avro_file:
        writer(avro_file, schema, records)

def get_avro_type(dtype):
    # Returns the Avro type corresponding to the Spark DataFrame column types
    if isinstance(dtype, IntegerType):
        return "int"
    elif isinstance(dtype, FloatType):
        return "float"
    elif isinstance(dtype, BooleanType):
        return "boolean"
    elif isinstance(dtype, TimestampType):
        return {"type": "string", "logicalType": "timestamp-millis"}
    elif isinstance(dtype, StringType):
        return "string"
    else:
        return "string"


def schema_insights(df):
    print("\n schema: ")
    print(df.printSchema())
    print("\n column types: ")
    print(df.dtypes)
    print("\n column names: ")
    print(df.columns)
    print("\n first 5 rows: ")
    df.show(5)

def print_header(title):
    border = "=" * (len(title) + 4)
    print(f"\033[1m\033[92m{border}\033[0m")
    print(f"\033[1m\033[92m| {title} |\033[0m")
    print(f"\033[1m\033[92m{border}\033[0m")

def print_table(df, title, limit=10):
    print_header(title)
    df.show(limit, truncate=False)
    print("\n")
