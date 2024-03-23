from kafka import KafkaConsumer
from json import loads

import logging

#from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


consumer = KafkaConsumer(
    'users_created',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    value_deserializer=lambda x: loads(x.decode('ISO-8859-1')))

for message in consumer:
    print(message.value)

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        print("Spark connection created successfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡¡")

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn



def connect_to_kafka(spark_conn):
    try:
        df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
        print(f'les données sortant : {df}')
        print("kafka dataframe created successfully successfully!!!!!!!!!!!!!!!!!!!")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
        df = None  # Assign None in case of failure to prevent UnboundLocalError

    return df


if __name__=="__main__":
    spark = create_spark_connection()
    spark_df = connect_to_kafka(spark)