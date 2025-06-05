# spark_kafka_consumer_parsed.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Kafka_Weather_Consumer_Parsed") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Esquema actualizado del JSON de OpenWeatherMap
schema = StructType([
    StructField("name", StringType()),
    StructField("weather", 
        ArrayType(
            StructType([
                StructField("main", StringType()),
                StructField("description", StringType())
            ])
        )
    ),
    StructField("main", 
        StructType([
            StructField("temp", DoubleType()),
            StructField("humidity", IntegerType())
        ])
    )
])

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-spark-api") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear y seleccionar campos
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.name").alias("city"),
        col("data.weather")[0]["main"].alias("weather"),
        col("data.weather")[0]["description"].alias("description"),
        col("data.main.temp").alias("temperature"),
        col("data.main.humidity").alias("humidity")
    )

# Mostrar en consola
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
