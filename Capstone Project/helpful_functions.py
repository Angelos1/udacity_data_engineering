import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType, StructField
from pyspark.sql.functions import udf, col, date_add
import csv


def create_spark_session():
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
    os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    return spark


'''
    Creates csv files from the labels in I94_SAS_Labels_Descriptions.SAS file
    Inspired from https://knowledge.udacity.com/questions/125439

    
    Parameters:
        file : Should be the I94_SAS_Labels_Descriptions.SAS
        out_folder : folder to create the csv files into.
        *labels : strings in the file that signify the starting point of the parsing
'''
def create_data_sources_from_label_file(file, out_folder, *labels):
    with open(file) as f:
        content = f.read()
        content = content.replace('\t', '')
        for label in labels:
            label_content = content[content.index(label):]
            label_content = label_content[:label_content.index(';')].split('\n')
            label_content = [i.replace("'", "") for i in label_content]
            data = [i.split('=') for i in label_content[1:]]
            data = dict([i[0].strip(), i[1].strip()] for i in data if len(i) == 2)
            file_name = label[3:-1]
            with open('./{}/{}.csv'.format(out_folder, file_name), 'w+', newline='') as f2:
                writer = csv.writer(f2)
                for row in data.items():
                    writer.writerow(row)
 

'''
    Creates a Spark dataframe from the state codes csv file
    
    Parameters:
        spark : Spark session
        file : location of the csv file with the state codes.
'''
def read_state_data(spark, file):

    schema = StructType([
        StructField("code", StringType()),
        StructField("state", StringType())
    ])

    state_df = spark.read.csv(file, header=False, schema=schema)
    
    return state_df


'''
    Creates a Spark dataframe from the country codes csv file
    
    Parameters:
        spark : Spark session
        file : location of the csv file with the country codes.
'''
def read_country_data(spark, file):

    schema = StructType([
        StructField("code", IntegerType()),
        StructField("country", StringType())
    ])

    state_df = spark.read.csv(file, header=False, schema=schema)
    
    return state_df


'''
    Creates a Spark dataframe from the mode codes csv file
    
    Parameters:
        spark : Spark session
        file : location of the csv file with the mode codes.
'''
def read_mode_data(spark, file):

    schema = StructType([
        StructField("code", IntegerType()),
        StructField("mode", StringType())
    ])

    state_df = spark.read.csv(file, header=False, schema=schema)
    
    return state_df


'''
    Performs the cleaning of the immigration data and returns the results in a dataframe
    
    Parameters:
        immigration_df : Spark dataframe that contains the immigration data
'''
def clean_immigration(immigration_df):
    
    clean_immigration_df = immigration_df.withColumn("cicid", col("cicid").cast(IntegerType())) \
                                    .withColumn("i94yr", col("i94yr").cast(IntegerType())) \
                                    .withColumn("i94mon", col("i94mon").cast(IntegerType())) \
                                    .withColumn("i94res", col("i94res").cast(IntegerType())) \
                                    .withColumn("i94mode", col("i94mode").cast(IntegerType())) \
                                    .withColumn("biryear", col("biryear").cast(IntegerType())) \
                                    .withColumn("count", col("count").cast(IntegerType())) 

    # coverting days to date format for arrival date
    get_date = udf(lambda days : datetime(1960, 1, 1) + timedelta(days=int(days)), TimestampType())
    clean_immigration_df = clean_immigration_df.withColumn("arr_date", get_date("arrdate")) 
    
    return clean_immigration_df


'''
    Performs the cleaning of the immigration data and returns the results in a dataframe
    
    Parameters:
        spark : Spark session
        airport_df : Spark dataframe that contains the airport data
'''
def clean_airports(spark, airport_df):
    
    airport_df.createOrReplaceTempView("airports_staging")
    
    non_duplicate_iata_codes = spark.sql("""
    select iata_code
    from
    (SELECT iata_code, COUNT(*) AS count
    FROM airports_staging
    GROUP BY iata_code
    having count = 1
    )
    """
    )
    
    non_duplicate_iata_codes.createOrReplaceTempView("non_duplicate_iata_codes")

    # selecting the records that their iata_codes appear only once
    clean_airports_df = spark.sql("""
    select * 
    from airports_staging
    where iata_code in (select * from non_duplicate_iata_codes)
    """
    )

    # selecting the records that their iata_codes appear only once
    clean_airports_df = spark.sql("""
    select * 
    from airports_staging
    where iata_code in (select * from non_duplicate_iata_codes)
    """
    )    
    
    return clean_airports_df


'''
    Creates the the airports dimension dataframe
    
    Parameters:
        spark : Spark session
        clean_airport_df : Dataframe with cleaned airport data
'''
def create_airports_dim(spark, clean_airport_df):
    
    clean_airport_df.createOrReplaceTempView("clean_airports")
    
    airports_dim_df = spark.sql("""
    SELECT
        iata_code,
        ident AS identifier,
        type, 
        name,
        elevation_ft AS elevation_ft,
        continent,
        iso_country AS country,
        iso_region AS region,
        municipality
    FROM clean_airports
    """
    )
    
    return airports_dim_df


'''
    Creates the the airports dimension dataframe
    
    Parameters:
        spark : Spark session
        clean_immigration_df : Dataframe with cleaned immigration data
        clean_airports_df : Dataframe with cleaned airport data
        state_df : State codes dataframe
        mode_df :  Mode codes dataframe
        country_df : Country codes dataframe
'''
def create_immigration_fact(spark, clean_immigration_df):
    
    clean_immigration_df.createOrReplaceTempView("clean_immigration")
    
    fact_df = spark.sql("""
    SELECT
        im.cicid AS cicid,
        im.i94yr AS year, 
        im.i94mon AS month, 
        im.i94res AS country_code,
        im.i94port AS airport_code,
        im.arr_date AS arrival_date,
        im.i94mode AS mode_code,
        im.i94addr AS state,
        im.count AS count,
        im.biryear AS birth_year,
        im.gender AS gender,
        im.airline AS airline,
        im.visatype AS visa_type
    FROM clean_immigration im
    """
    )
    
    return fact_df
