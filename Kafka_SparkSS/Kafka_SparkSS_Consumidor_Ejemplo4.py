# spark_kafka_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("Kafka_Consumidor_api") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-spark-api") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir el valor binario a string (JSON en este caso)
json_df = df.selectExpr("CAST(value AS STRING) as json")

# Mostrar los mensajes en consola
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
