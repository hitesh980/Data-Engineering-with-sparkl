#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


@author: allakkihome
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import shutil
import os
import time

def broadcast_join_maps(spark):
    # Ensure correct absolute paths
    matches_path = "file:///Users/allakkihome/data/matches.csv"
    maps_path = "file:///Users/allakkihome/maps.csv"

    # Read DataFrames using correct syntax
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv(matches_path)
    maps_df = spark.read.option("header", "true").option("inferSchema", "true").csv(maps_path)

    
    #Broadcast joins maps_df to matches_df

    matches_maps_df = matches_df.join(broadcast(maps_df), 'mapid')
    
    return matches_maps_df

def broadcast_join_medals(spark):
    #create medals on medals matches Players dataframes
    spark.sql("CREATE DATABASE IF NOT EXISTS medals_db")
    medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/Downloads/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals.csv")
    medals_matches_players_df =  spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/Downloads/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals_matches_players.csv")
    
   
    
    #boradcast join medals_df to medals_matches_players_df
    medals_players_df = medals_matches_players_df.join(broadcast(medals_df), 'medal_id')
    return medals_players_df

def bucketed_join_matches(spark):

    num_buckets = 16

    #create matches dataframes
   
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/Downloads/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/matches.csv")
    #repartition and sort within partitions
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id") 
   
    matches_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("Overwrite").saveAsTable('matches_bucketed')

#read the bucketed table
    matches_bucketed_df = spark.table('matches_bucketed')
    matches_bucketed_df.show()

def bucket_join_match_details(spark):
    num_buckets = 16

    #create match_details dataframes
    match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/match_details.csv")
    #repartition and sort within partitions
    spark.sql("DROP TABLE IF EXISTS match_details_bucketed")
    match_details_df = match_details_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id")

    #remove table path
    path = "file:///Users/allakkihome/data/match_details_bucketed"
    try:
        shutil.rmtree(path)
    except OSError as e:
        print("Error: %s : %s" % (path, e.strerror))
        
        
            #write the bucketed table
    match_details_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("Overwrite").saveAsTable('match_details_bucketed')

    #read the bucketed table
    match_details_bucketed_df = spark.table('match_details_bucketed')
    match_details_bucketed_df.show()
def bucket_join_medal_matches_players(spark):
    num_buckets = 16

    #create medals and medals_matches_players dataframes
 
    medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/medals.csv")
    medals_matches_players_df =  spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/medals_matches_players.csv")
    
    #repartition and sort within partitions
    medals_df = medals_df.repartition(num_buckets, "medal_id").sortWithinPartitions("medal_id")
    medals_matches_players_df = medals_matches_players_df.repartition(num_buckets, "medal_id").sortWithinPartitions("medal_id")

    #write the bucketed tables
    medals_df.write.format("parquet").bucketBy(num_buckets,"medal_id").mode("Overwrite").saveAsTable('medals_bucketed')
    medals_matches_players_df.write.format("parquet").bucketBy(num_buckets,"medal_id").mode("Overwrite").saveAsTable('medals_matches_players_bucketed')

    #read the bucketed tables
    medals_bucketed_df = spark.table('medals_bucketed')
    medals_matches_players_bucketed_df = spark.table('medals_matches_players_bucketed')

    medals_matches_players_bucketed_df.show()
    return medals_matches_players_bucketed_df

def bucket_join_everything(matches_df,maps_df,medals_df,medals_matches_players_df,match_details_df):
    #bucketed join all dataframes
    bucketed_df = matches_df.join(match_details_df, 'match_id').join(medals_matches_players_df,["match_id","player_gamertag"])
    return bucketed_df


def get_aggregated_stats(spark,df):
    start_time = time.time() # Get the start time

    # Get the average number of kills per game for each player
    players_average_killing_per_game = df.groupby('match_id','player_gamertag').avg("player_total_kills").alias("avg_kills").alias("avg_kills_per_game").show()
    
    #get the most commoin play list
    most_common_playlist = df.groupby('playlist_id').count().orderBy('count', ascending=False).take(1)
    print("the most common playlist is : {most_common_playlist}")

    #most played map
    most_common_map = df.groupby('mapid').count().orderBy('count', ascending=False).take(1)
    print("the most common map is : {most_common_map}")
    
    # Get the most common map for medals where classification is killingspree
    medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/medals.csv")
    df =df.join(broadcast(medals_df), 'medal_id')
    killing_sprees = df.filter(df['classification'] == 'killingspree').groupby('mapid').count().orderBy('count', ascending=False).take(1)
    print("the most common map for killing spree is : {killing_sprees}")

    end_time = time.time() #record the end time

    print(f"Time taken to get aggregated stats: {end_time - start_time} seconds")

def bucketed_join_matches_v2(spark):
    num_buckets = 16

    #create matches dataframes
     
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/matches.csv")
    #repartition and sort within partitions
    matches_df = matches_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id","mapid") 
   
    matches_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("Overwrite").saveAsTable("matches_bucketed_v2")

    #read the bucketed table
    matches_bucketed_df = spark.table("matches_bucketed_v2")
    return matches_bucketed_df

def bucket_join_match_details_v3(spark):
    num_buckets = 16

    #create match_details dataframes
    match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/match_details.csv")
    #repartition and sort within partitions
    match_details_df = match_details_df.repartition(num_buckets, "match_id").sortWithinPartitions("match_id","playlist_id")
    match_details_df.write.format("parquet").bucketBy(num_buckets,"match_id").mode("Overwrite").saveAsTable("match_details_bucketed_v3")

    #read the bucketed table
    match_details_bucketed_df = spark.table("match_details_bucketed_v3")
    return match_details_bucketed_df
def compare_files_sizes ():
    #compare the files sizes of three bucketed daraframes
    tables = ['matches_bucketed', 'matches_bucketed_v2', 'match_bucketed_v3']

    for table in tables:
        file_path ="spark-warehouse/"+table
        files_size = []
        if os.path.exists(file_path):
            for file in os.listdir(file_path):
                files_size.append(os.path.getsize(file))
            print(f"Table {table} ,Files size: {sorted(files_size)}")
def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .master("local") \
        .appName("Matchstats") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()
    #broadcast join maps and matches Dataframes
    broadcast_map = broadcast_join_maps(spark)
    broadcast_map.show()
    #broadcast join medals and matches Dataframes
    broadcast_medals = broadcast_join_medals(spark)
    broadcast_medals.show()

    #bucketed join matches Dataframes
    bucketed_matches = bucketed_join_matches(spark)

    #bucket join match_details DateFrame
    bucketed_match_details = bucketed_join_matches(spark)

    #bucket join match details dataframes
    bucketed_match_details = bucket_join_medal_matches_players(spark)
    

    # Read DataFrames
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/matches.csv")
    maps_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/maps.csv")
    medals_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/medals.csv")
    medals_matches_players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/medals_matches_players.csv")
    match_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/allakkihome/data/match_details.csv")

    # bucket join all dataframes
    bucketed_df = bucket_join_everything(matches_df, maps_df, medals_df, medals_matches_players_df, match_details_df)
    

    #get aggregated stats
    get_aggregated_stats(spark,bucketed_df)
    
    #bucketed join matches Dataframes
    bucketed_matches_v2 = bucketed_join_matches_v2(spark)
    bucketed_df_v2 = bucket_join_everything(bucketed_join_matches_v2,bucket_join_match_details,bucket_join_medal_matches_players)
    
    #get aggregated stats
    get_aggregated_stats(spark,bucketed_df_v2)

    #bucketed join match details Dataframe v3
    bucketed_matches_v2 = bucket_join_match_details_v3(spark)
    bucketed_df_v3 = bucket_join_everything(bucketed_matches,bucketed_match_details,bucket_join_medal_matches_players)

    #get aggregated stats
    get_aggregated_stats(spark,bucketed_df_v3)
    spark.stop()
    
    #compare match_bucketed, match_bucketed_v2 and match_bucketed_v3
    compare_files_sizes()


if __name__ == "__main__":
    main()
