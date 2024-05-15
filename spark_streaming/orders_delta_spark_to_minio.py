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

def create_spark_session():
    """
    Creates the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    try:
        builder = SparkSession.builder \
                    .appName("Streaming Kafka") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.apache.hadoop:hadoop-aws:2.8.2"]).getOrCreate()

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
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
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
            .option("subscribe", "sales.public.orders")
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
    from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField, DecimalType
    from pyspark.sql.functions import col, from_json, udf

    payload_after_schema = StructType([
            StructField("order_date", StringType(), True),
            StructField("order_time", StringType(), True),
            StructField("order_number", StringType(), True),
            StructField("order_line_number", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("store", StringType(), True),
            StructField("promotion", StringType(), True),
            StructField("order_quantity", IntegerType(), True),
            StructField("unit_price", StringType(), True),
            StructField("unit_cost", StringType(), True),
            StructField("unit_discount", StringType(), True),
            StructField("sales_amount", StringType(), True)
    ])

    schema = StructType([
        StructField("payload", StructType([
            StructField("after", payload_after_schema, True)
        ]), True)
    ])

    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
                .select(from_json(col("json"), schema).alias("data")) \
                .select("data.payload.after.*")

    decode_base64_to_decimal_udf = udf(decode_base64_to_decimal, DecimalType(12, 2))

    decoded_df = parsed_df \
            .withColumn("unit_price", decode_base64_to_decimal_udf(col("unit_price"))) \
            .withColumn("unit_cost", decode_base64_to_decimal_udf(col("unit_cost"))) \
            .withColumn("unit_discount", decode_base64_to_decimal_udf(col("unit_discount"))) \
            .withColumn("sales_amount", decode_base64_to_decimal_udf(col("sales_amount")))

    decoded_df.createOrReplaceTempView("orders_view")

    df_final = spark.sql("""
        SELECT 
            *
        FROM orders_view 
    """)

    logging.info("Final dataframe created successfully!")
    return df_final

def decode_base64_to_decimal(base64_str):
    """
    Decode base64 to decimal
    """
    import base64
    from decimal import Decimal

    if base64_str:
        # Fix padding
        missing_padding = len(base64_str) % 4
        if missing_padding:
            base64_str += '=' * (4 - missing_padding)
        decoded_bytes = base64.b64decode(base64_str)
        # Convert bytes to integer and then to decimal with scale 2
        return Decimal(int.from_bytes(decoded_bytes, byteorder='big')) / Decimal(100)
    return None

def start_streaming(df):
    """
    Converts data to delta lake format and store into MinIO
    """
    minio_bucket = "lakehouse"

    logging.info("Streaming is being started...")
    stream_query = df.writeStream \
                        .format("delta") \
                        .outputMode("append") \
                        .option("checkpointLocation", f"minio_streaming/{minio_bucket}/orders/checkpoints") \
                        .option("path", f"s3a://{minio_bucket}/orders") \
                        .partitionBy("store") \
                        .start()

    return stream_query.awaitTermination()


if __name__ == '__main__':
    spark = create_spark_session()
    load_minio_config(spark.sparkContext)
    df5 = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df5, spark)
    start_streaming(df_final)