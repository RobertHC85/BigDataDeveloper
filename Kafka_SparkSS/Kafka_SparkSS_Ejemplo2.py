from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("KafkasparkSS_Conteo_ejemplo2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer datos de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-test-sparkss") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir a texto
lineas = df.selectExpr("CAST(value AS STRING) as linea")

# Separar palabras y hacer el conteo
palabras = lineas.select(explode(split(col("linea"), " ")).alias("palabra"))

conteo = palabras.groupBy("palabra").count()

# Mostrar en consola
query = conteo.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
