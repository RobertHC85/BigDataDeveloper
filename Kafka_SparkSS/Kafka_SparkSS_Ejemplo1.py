from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("Test_KafkasparkSS_ejemplo1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer datos de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-test-sparkss") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir valor binario a string
datos = df.selectExpr("CAST(value AS STRING)").alias("mensaje")

# Mostrar en consola
query = datos.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
