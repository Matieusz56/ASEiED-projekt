from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Tworzymy sesję Spark
spark = SparkSession.builder \
    .appName("EnergyMonitoringConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definicja schematu danych (np. energia elektryczna z datasetu)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("appliance", StringType(), True),
    StructField("consumption", DoubleType(), True)
])

# 3. Czytanie z Apache Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "energy-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsowanie wartości JSON
value_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Prosta analiza – np. średnie zużycie energii na urządzenie
agg_df = value_df.groupBy("appliance").avg("consumption")

# 6. Zapis do pliku CSV (w trybie streamingu)
query = agg_df.writeStream \
    .outputMode("complete") \
    .format("csv") \
    .option("path", "output/energy_consumption") \
    .option("checkpointLocation", "output/checkpoints") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
