#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import configparser   # used to read cfg file
import os   # used to make directories
from datetime import datetime  
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
    Creating a spark connection"""
    spark = SparkSession.builder            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")            .getOrCreate()
    return spark
            
            


def process_song_data(spark, input_data, output_data):
    """
    Process
    1. Load Song Data from S3 and reads it
    2. extract columns for song and artist table
    3. write parquet files back to S3
    
    Parameter
    spark = spark session
    input_data = S3 location for original json files
    output_data = S3 location where new parquet files will be written
    """
    # get filepath to song data file
    song_data_path = input_data + "song_data/*/*/*/*.json"
    # read song data file
    df = spark.read.json(song_data_path)
    # extract columns to create songs table
    song_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')                .dropDuplicates(subset =['song_id'])
    song_table.createOrReplaceTempView('songs')   
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy('year', 'artist_id')            .parquet(os.path.join(output_data,'song/songs.parquet'),'overwrite')
    # extract columns to create artists table
    artist_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')                .dropDuplicates(subset=['artist_id'])
    artist_table.createOrReplaceTempView('artists')
    # write artists table to parquet files
    artist_table.write.parquet(os.path.join(output_data,'artist/artists.parquet'),'overwrite')

def process_log_data(spark, input_data, output_data):
    """
    Process
    1. Load Event Data from S3 and reads it
    2. extract columns for songplays, time, user table
    3. write parquet files back to S3
    
    Parameter
    spark = spark session
    input_data = S3 location for original json files
    output_data = S3 location where new parquet files will be written
    """
    # get filepath to log data file
    log_data_path = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    df_facts=df.filter(df.page =='NextSong').select('ts','userId','level','song','artist','sessionId','location','userAgent').dropDuplicates()

    # extract columns for users table    
    user_table = df.select('userId', 'firstName','lastName','gender','level').dropDuplicates(subset=['userId'])
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data,'log/user/users.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : str(int(int(x/1000))))
    df_facts = df_facts.withColumn('timestamp', get_timestamp(df_facts.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df_facts = df_facts.withColumn('datetime', get_datetime(df_facts.ts))
    
    # extract columns to create time table
    time_table = df_facts.select('datetime')                    .withColumn('start_time', df_facts.datetime)                     .withColumn('hour', hour('datetime'))                     .withColumn('day', dayofmonth('datetime'))                     .withColumn('week', weekofyear('datetime'))                     .withColumn('month', month('datetime'))                     .withColumn('year', year('datetime'))                     .withColumn('weekday', dayofweek('datetime'))                     .dropDuplicates(subset=['start_time'])

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month')            .parquet(os.path.join(output_data, 'log/time/time.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_data_path = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data_path)
    

    # extract columns from joined song and log datasets to create songplays table 
    df_facts = df_facts.alias('event_df')
    song_df = song_df.alias('song_df')
    join_df = df_facts.join(song_df, col('event_df.artist')==col('song_df.artist_name'),'inner')
    songplays_table = join_df.select(
                        col('event_df.ts').alias('start_time'),
                        col('event_df.userId').alias('user_id'),
                        col('event_df.level').alias('level'),
                        col('song_df.song_id').alias('song_id'),
                        col('song_df.artist_id').alias('artist_id'),
                        col('event_df.sessionId').alias('session_id'),
                        col('event_df.location').alias('location'),
                        col('event_df.userAgent').alias('userAgent'),
                        year('event_df.datetime').alias('year'),
                        month('event_df.datetime').alias('month'))\
                        .withColumn('songplay_id', monotonically_increasing_id())\
                        .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')                    .parquet(os.path.join(output_data, 'log/songplay/songplay.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

