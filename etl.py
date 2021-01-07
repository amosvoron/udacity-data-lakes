import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
import os

# create config parser
config = configparser.ConfigParser()
config.read('dl.cfg')

# store access keys into environmental variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates spark session.
    
    Returns:
        None
    """    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_path, output_path):
    """Processes song data.
    
    Upon importing the raw song data from the S3 JSON source 
    two following tables are created:
        - songs_table (dimension)
        - artists_table (dimension)
    
    Args:
        spark: The spark session object.
        input_path: The S3 path of the input data.
        output_path: The S3 path of the output data (in parquet format).
        
    Returns:
        None
    """    
    # get filepath to song data file
    path = os.path.join(input_path, 'song_data/*/*/*/*.json')
    
    # read song data file
    songs = spark.read.json(path)
    
    # created temp view of song data
    songs.createOrReplaceTempView('songs')
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id
            , MIN(title) AS title
            , MIN(artist_id) AS artist_id
            , MIN(year) AS year
            , MIN(duration) AS duration
        FROM songs
        GROUP BY song_id
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_path, 'songs/songs.parquet'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id
            , MIN(artist_name) AS name
            , MIN(artist_location) AS location
            , MIN(artist_latitude) AS latitude
            , MIN(artist_longitude) AS longitude
        FROM songs
        GROUP BY artist_id
    """)
    
    # write artists table to parquet files
    artists_table.write \
        .parquet(os.path.join(output_path, 'artists/artists.parquet'), 'overwrite')
    

def process_log_data(spark, input_path, output_path):
    """Processes log event data.
    
    Upon importing the raw log event data from the S3 JSON source 
    the following tables are created:
        - users_table (dimension)
        - time_table (dimension)
        - songplays_table (fact)
    
    Args:
        spark: The spark session object.
        input_path: The S3 path of the input data.
        output_path: The S3 path of the output data (in parquet format).
        
    Returns:
        None
    """    
    # get filepath to log data file
    path = os.path.join(input_path, 'log_data/*/*/*.json')

    # read log data file
    logs = spark.read.json(path)
    
    # add start_time column (by converting ts into timestamp)
    logs = logs \
        .withColumn('start_time', (F.col('ts') / 1000.0).cast(TimestampType()))  
    
    # filter by actions for song plays
    logs = logs.where(logs.page == 'NextSong') \
        .select('ts', 'start_time', 'userId', 'firstName', 'lastName', 'gender', 'level', 
                'song', 'artist', 'sessionId', 'location', 'userAgent')

    # create temp view of log data
    logs.createOrReplaceTempView('logs')
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT 
            CAST(A.userId AS int) AS user_id
            , A.firstName AS first_name
            , A.lastName AS last_name
            , A.gender
            , A.level
        FROM logs AS A
        INNER JOIN (
            SELECT userId, MAX(ts) AS tsLast
            FROM logs
            GROUP BY userId
        ) AS B
            ON A.userId = B.userId
            AND A.ts = B.tsLast
        WHERE A.userId IS NOT NULL
    """)
    
    # write users table to parquet files
    users_table.write \
        .parquet(os.path.join(output_path, 'users/users.parquet'), 'overwrite')
    
    # extract columns to create time table
    time_table = logs \
        .withColumn('hour', F.hour(F.col('start_time'))) \
        .withColumn('day', F.dayofmonth(F.col('start_time'))) \
        .withColumn('week', F.weekofyear(F.col('start_time'))) \
        .withColumn('month', F.month(F.col('start_time'))) \
        .withColumn('year', F.year(F.col('start_time'))) \
        .withColumn('weekday', F.dayofweek(F.col('start_time'))) \
        .select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') \
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_path, 'time/time.parquet'), 'overwrite')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT A.start_time 
            , CAST(A.userId AS int) AS user_id
            , A.level
            , B.song_id
            , B.artist_id
            , A.sessionId AS session_id
            , A.location
            , A.userAgent AS user_agent
            , YEAR(A.start_time) AS year
            , MONTH(A.start_time) AS month
        FROM logs AS A
        INNER JOIN songs AS B
            ON A.song = B.title
            AND A.artist = B.artist_name
    """)
    
    # add songplay_id column
    w =  Window.orderBy('start_time')
    songplays_table = songplays_table \
        .withColumn('songplay_id', F.row_number().over(w))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_path, 'songplays/songplays.parquet'), 'overwrite')


def main():
    """Processes the ETL pipeline.
    
    Steps:
        - Creates the spark session.
        - Imports song data from the S3 source into spark dataframe and performs
          necessary transformations to create and fill corresponding analytic tables.
        - Imports log event data from the S3 source into spark dataframe and performs
          necessary transformations to create and fill corresponding analytic tables.
        - Stores analytic tables into S3 using parquet format.
    """    
    spark = create_spark_session()
    input_path = 's3://udacity-dend'  
    output_path = 's3://amosvoron-udacity'
    
    process_song_data(spark, input_path, output_path)    
    process_log_data(spark, input_path, output_path)


if __name__ == "__main__":
    main()
