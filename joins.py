from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def broadcast_join_maps(spark):
    # Create DataFrame
    matches_df = spark.read.csv('.../..data/matches.csv', header=True)
    matches_df.show()
    maps_df = spark.read.csv('.../..data/maps.csv', header=True)
    maps_df.show()
    
def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .appName('Matchstats') \
        .getOrCreate()
    broadcast_join_maps(spark)

if __name__ == "__main__":
    main()