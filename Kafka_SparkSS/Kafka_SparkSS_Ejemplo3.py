from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# Crear sesión Spark
spark = SparkSession.builder \
    .appName("Filtrar_Mensajes_Ejemplo3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Leer mensajes de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-test-sparkss") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir valor binario a string y filtrar los que contienen "error" (insensible a mayúsculas)
mensajes = df.selectExpr("CAST(value AS STRING) AS mensaje")
#mensajes = df.selectExpr("CAST(value AS STRING) AS mensaje") \
#    .filter(lower(col("mensaje")).contains("error"))

# Palabras a buscar
palabras_clave = ["error", "fail", "warning"]

# Crear condición compuesta (case insensitive)
condicion = None
for palabra in palabras_clave:
    filtro = lower(col("mensaje")).contains(palabra)
    condicion = filtro if condicion is None else condicion | filtro

# Filtrar los mensajes que contienen al menos una palabra clave
mensajes_filtrados = mensajes.filter(condicion)

# Mostrar en consola
#query = mensajes.writeStream \
query = mensajes_filtrados.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
