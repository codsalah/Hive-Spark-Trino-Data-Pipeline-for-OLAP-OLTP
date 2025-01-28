from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import datediff, current_date
from pyspark.sql.functions import col
from utils import read_csv, create_spark_session, schema_insights, print_header, print_table


def get_specific_club(spark, clubs_path, club_name):
    specific_club_df = read_csv(spark, clubs_path).filter(col("name").like(f"%{club_name}%"))
    return specific_club_df

def specific_club(spark, clubs_path, club_name):
    """
    Analyze a specific club's metrics compared to others in the same domestic competition.
    """
    # Read the clubs data
    clubs_df = read_csv(spark, clubs_path)
    
    # Filter clubs by name
    specific_club_df = get_specific_club(spark, clubs_path, club_name)

    # Print the filtered clubs
    print_table(specific_club_df, f"Clubs with '{club_name}' in their name")
    
    # If no clubs are found, return early
    if specific_club_df.count() == 0:
        print(f"No clubs found with the name '{club_name}'.")
        return None
    
    # Get the domestic competition ID of the specific club
    club_competition_id = specific_club_df.select("domestic_competition_id").first()[0]
    
    # Filter clubs in the same domestic competition
    clubs_in_same_competition = clubs_df.filter(col("domestic_competition_id") == club_competition_id)
    
    # Calculate insights for clubs in the same competition
    competition_insights = clubs_in_same_competition.agg(
        mean("total_market_value").alias("avg_total_market_value"),
        max("total_market_value").alias("max_total_market_value"),
        min("total_market_value").alias("min_total_market_value"),
        mean("squad_size").alias("avg_squad_size"),
        mean("average_age").alias("avg_average_age"),
        mean("foreigners_percentage").alias("avg_foreigners_percentage"),
        mean("national_team_players").alias("avg_national_team_players")
    )
    
    # Show insights
    print_header(f"Insights for Domestic Competition: {club_competition_id}")
    competition_insights.show(truncate=False)
    
    # Compare the specific club's metrics to the competition's averages
    specific_club_metrics = specific_club_df.select(
        "total_market_value",
        "squad_size",
        "average_age",
        "foreigners_percentage",
        "national_team_players"
    ).first()
    
    avg_metrics = competition_insights.select(
        "avg_total_market_value",
        "avg_squad_size",
        "avg_average_age",
        "avg_foreigners_percentage",
        "avg_national_team_players"
    ).first()
    
    print_header(f"Comparison for {club_name}")
    print(f"Club's Total Market Value: {specific_club_metrics['total_market_value']}")
    print(f"Average Total Market Value for Competition: {avg_metrics['avg_total_market_value']}")
    print(f"Club's Squad Size: {specific_club_metrics['squad_size']}")
    print(f"Average Squad Size for Competition: {avg_metrics['avg_squad_size']}")
    print(f"Club's Average Age: {specific_club_metrics['average_age']}")
    print(f"Average Age for Competition: {avg_metrics['avg_average_age']}")
    print(f"Club's Foreigners Percentage: {specific_club_metrics['foreigners_percentage']}")
    print(f"Average Foreigners Percentage for Competition: {avg_metrics['avg_foreigners_percentage']}")
    print(f"Club's National Team Players: {specific_club_metrics['national_team_players']}")
    print(f"Average National Team Players for Competition: {avg_metrics['avg_national_team_players']}")
    
    return specific_club_df



if __name__ == "__main__":
    # Create a Spark session
    spark = create_spark_session()
    
    # Path to the clubs CSV file
    clubs_path = "/home/codsalah/Downloads/archive/clubs.csv"
    
    # Name of the club to analyze
    club_name = "Real Madrid"
    
    # Run the analysis
    specific_club(spark, clubs_path, club_name)
    
    # Stop the Spark session
    spark.stop()