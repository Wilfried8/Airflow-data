from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from kafka import KafkaConsumer
from json import loads
from pyspark.sql.functions import from_json

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Stockage de messages Kafka en streaming") \
    .getOrCreate()

# Définition du schéma pour le DataFrame
schema = StructType([
    StructField("id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("post_code", StringType(), False),
    StructField("email", StringType(), False),
    StructField("username", StringType(), False),
    StructField("registered_date", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("picture", StringType(), False)
])

# Initialisation du KafkaConsumer
consumer = KafkaConsumer(
    'users_created',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='test-consumer-group',
    value_deserializer=lambda x: loads(x.decode('ISO-8859-1')))

# Création du flux de données en streaming
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users_created") \
    .option('startingOffsets', 'earliest') \
    .load()

# Conversion de la valeur du message en JSON
json_df = streaming_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Écriture en streaming dans un fichier JSON
query = json_df \
    .writeStream \
    .format("json") \
    .option("./", "output_json") \
    .option("checkpointLocation", "checkpoint_json") \
    .start()

query.awaitTermination()

# Arrêt de la session Spark
spark.stop()
