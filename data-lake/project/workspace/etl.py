import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = df.select("artist_id", col("artist_name").alias("name"), col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_longitude").alias("longitude")).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"), col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), "gender", "level").distinct()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", to_timestamp(col("ts") / 1000))

    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'), hour(col('timestamp')).alias('hour'),
                           dayofmonth(col('timestamp')).alias('day'), weekofyear(col('timestamp')).alias('week'),
                           month(col('timestamp')).alias('month'), year(col('timestamp')).alias('year'),
                           dayofweek(col('timestamp')).alias('weekday')).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [song_df.title == df.song, song_df.duration == df.length], 'left').select(
        df.timestamp.alias('start_time'), year(df.timestamp).alias('year'), month(df.timestamp).alias('month'),
        df.userId.alias('user_id'), df.level, song_df.song_id, song_df.artist_id, df.sessionId.alias('session_id'),
        df.location, df.userAgent.alias('user_agent')).withColumn('songplays_id', expr("uuid()")).distinct()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://minh-udacity-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
