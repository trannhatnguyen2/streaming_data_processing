import sys
import warnings
import traceback
import logging
from time import sleep


from pyspark import SparkConf, SparkContext
from delta import *


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
            
warnings.filterwarnings('ignore')
# checkpointDir = "file:///"

def create_spark_session():
    """
    Creates the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try:
        spark = (SparkSession.builder \
                .appName("Streaming Kafka") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:2.8.2")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .getOrCreate())
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

    return spark


def load_minio_config(spark_context: SparkContext):
    """
    Established the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.warning(f"MinIO config could not be created successfully due to exception: {e}")

    
def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly
    """
    try: 
        df = (spark_session
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "device_0")
            .option("failOnDataLoss", "false")
            .load())
        logging.info("Initial dataframe created successfully!")
    except Exception as e:
        logging.warning(f"Initial dataframe could not be created due to exception: {e}")

    return df
    
    
def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe
    """
    from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField
    from pyspark.sql.functions import col, from_json

    df2 = df.selectExpr("CAST(value AS STRING) AS json_value")

    payload_schema = StructType([
    StructField("created", StringType(), True),
    StructField("device_id", IntegerType(), True),
    StructField("feature_5", FloatType(), True),
    StructField("feature_3", FloatType(), True),
    StructField("feature_1", FloatType(), True),
    StructField("feature_8", FloatType(), True),
    StructField("feature_6", FloatType(), True),
    StructField("feature_0", FloatType(), True),
    StructField("feature_4", FloatType(), True)
    ])

    # Parse the JSON and extract the payload
    parsed_df = df2 \
        .select(from_json(col("json_value"), 
                      StructType([StructField("schema", StructType([]), True), 
                                  StructField("payload", payload_schema, True)]))
            .alias("data")) \
        .select("data.payload.*")   

    parsed_df.createOrReplaceTempView("device_view")

    df4 = spark.sql("""
        SELECT 
            created,
            device_id,
            feature_0,
            feature_1,
            feature_3,
            feature_4,
            feature_5,
            feature_6,
            feature_8,
            CASE
                WHEN feature_0 > 0.75 THEN 'movement'
                ELSE 'no_movement'
            END AS if_movement
        FROM device_view 
    """)

    ### TEST ###
    delta_path = "s3a://datalake/device"

    # Write the parsed data to Delta Lake with the specified configurations
    query = df4 \
        .writeStream \
        .format("csv") \
        .outputMode("append") \
        .option("header", "true") \
        .option("checkpointLocation", "datalake/checkpoints") \
        .start(delta_path)

    query.awaitTermination()


    ### TEST ###

    # logging.info("Final dataframe created successfully!")

    # return df4


def start_streaming(df):
    """
    Converts data to delta lake format and store into MinIO
    """
    minio_bucket = "datalake"

    logging.info("Streaming is being started...")
    stream_query = (df.writeStream
                        .format("csv") \
                        .outputMode("append") \
                        .option('header', 'true') \
                        .option("checkpointLocation", f"{minio_bucket}/checkpoints") \
                        .option("path", f"s3a://{minio_bucket}/device")
                        # .trigger("processing=30 seconds") \
                        .partitionBy("if_movement") \
                        .start())

    return stream_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df5 = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df5, spark)
    # start_streaming(df_final)