from pyspark.sql import SparkSession

def create_spark_session(app_name="Data Analysis"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def read_csv(spark, path, header=True, inferSchema=True):
    df = spark.read.csv(path, header=header, inferSchema=inferSchema)
    return df
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