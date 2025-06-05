from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Kafka_Weather_Consumer_MySQL") \
    .config("spark.jars", "/path/to/mysql-connector-java-8.0.33.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Esquema con arreglo en weather y país
schema = StructType([
    StructField("name", StringType()),
    StructField("weather", ArrayType(
        StructType([
            StructField("main", StringType()),
            StructField("description", StringType())
        ])
    )),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("sys", StructType([
        StructField("country", StringType())
    ]))
])

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kafka-spark-api") \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON y seleccionar campos
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.name").alias("city"),
        col("data.sys.country").alias("country"),
        col("data.weather")[0]["main"].alias("weather"),
        col("data.weather")[0]["description"].alias("description"),
        (col("data.main.temp") - 273.15).alias("temperature_celsius"),
        col("data.main.humidity").alias("humidity")
    )

# Función para guardar en MySQL
def save_to_mysql(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/tu_basedatos") \
        .option("dbtable", "weather_data") \
        .option("user", "tu_usuario") \
        .option("password", "tu_password") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# Ejecutar el stream y guardar en MySQL
query = json_df.writeStream \
    .outputMode("append") \
    .foreachBatch(save_to_mysql) \
    .start()

query.awaitTermination()
