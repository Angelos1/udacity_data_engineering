import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import udf, col, desc, row_number, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS","AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    The function to handle inserting data into the songs and artists tables.
    
    Creates a dataframe of the song data extracts the relevant fields and writes the data
    for song and artist tables into parquet files in S3.
    
    Parameters:
        spark : The spark session.
        input_data : Path to the input data on S3.
        input_data : Path to the otput data on S3.
    '''
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"     
    
    #to test on smaller data commend the line above and uncomment this one
#     song_data = input_data + "song_data/A/A/A/*.json" 
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option("header",True) \
        .partitionBy("year","artist_id") \
        .mode("overwrite") \
        .parquet(output_data + "songs")
    
    # extract columns to create artists table
    artists_table = df.select("artist_id", \
                              df["artist_name"].alias("name"), \
                              df["artist_location"].alias("location"), \
                              df["artist_latitude"].alias("latitude"), \
                              df["artist_longitude"].alias("longitude") \
                             ).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    '''
    The function to handle inserting data into the users, time and songplays tables.
    
    Creates dataframes of the log and song data extracts the relevant fields and writes the data
    forusers, time and songplays tables into parquet files in S3.
    
    Parameters:
        spark : The spark session.
        input_data : Path to the input data on S3.
        input_data : Path to the otput data on S3.
    '''
    
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"
    
    #to test on smaller data commend the line above and uncomment this one
#     log_data = input_data + "log_data/2018/11/2018-11-01-events.json"
    
    song_data = input_data + "song-data/*/*/*/*.json" 
    
    #to test on smaller data commend the line above and uncomment this one
#     song_data = input_data + "song_data/A/A/A/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(df["userId"].alias("user_id"), \
                        df["firstName"].alias("first_name"), \
                        df["lastName"].alias("last_name"), \
                        "gender", "level", "ts").dropDuplicates()
    
    # The 3 lines below are for getting the most recent record for each user
    # in order to have his most recent level of subscription.
    user_window = Window.partitionBy('user_id').orderBy(desc('ts'))
    
    users_table = users_table.withColumn("row_number", row_number().over(user_window))
  
    users_table = users_table.filter(users_table.row_number == 1).drop("row_number").drop("ts")
    
    # write users table to parquet files
    users_table.write.option("header",True).mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : int(int(ts)/1000),  IntegerType())
    df = df.withColumn("timestamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts : datetime.fromtimestamp(ts), TimestampType())
    df = df.withColumn("start_time", get_datetime("timestamp"))
    
    # extract columns to create time table
    time_table = df.select("start_time", hour("start_time").alias("hour"), dayofmonth("start_time").alias("day"), \
                       weekofyear("start_time").alias("week"), month("start_time").alias("month"), \
                       year("start_time").alias("year")).withColumn("weekday", date_format(col("start_time"), "u")).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option("header",True) \
        .partitionBy("year","month") \
        .mode("overwrite") \
        .parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table artist_id
    songplays_table = df.join(song_df, \
                              (df["song"] == song_df["title"]) & \
                              ( df["artist"] == song_df["artist_name"]),"left").dropDuplicates()

    songplays_table = songplays_table.select("start_time", df["userId"].alias("user_id"), "level", "song_id",\
                                         "artist_id", df["sessionId"].alias("session_id"), "location",\
                                         month("start_time").alias("month"), year("start_time").alias("year"), \
                                         df["userAgent"].alias("user_agent")).withColumn("songplay_id", monotonically_increasing_id())
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option("header",True) \
        .partitionBy("year","month") \
        .mode("overwrite") \
        .parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-assignment/output_data/"
    
    process_song_data(spark, input_data, output_data)  
    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
