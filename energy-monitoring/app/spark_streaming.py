from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, sum, expr, count
)
from pyspark.sql.types import StructType, StringType, DoubleType

# Użyj nazwy serwisu Dockera zamiast localhost
KAFKA_BOOTSTRAP = "kafka:9092"  # Zmiana z localhost na kafka
KAFKA_TOPIC = "energy_usage"
OUTPUT_DIR = "/app/output/energy_aggregations"  # Ścieżka wewnątrz kontenera
CHECKPOINT_DIR = "/app/output/checkpoint"
ALERTS_DIR = "/app/output/alerts"

def build_spark():
    spark = (
        SparkSession.builder
        .appName("HouseholdEnergyMonitoring")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = build_spark()

    # Schemat danych
    value_schema = (
        StructType()
        .add("timestamp", StringType())
        .add("household_id", StringType())
        .add("energy_kwh", DoubleType())
    )

    # Odczyt z Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")  # Zacznij od najnowszych wiadomości
        .option("failOnDataLoss", "false")  # NIE przerywaj przy utracie danych
        .load()
    )

    # Parsowanie JSON
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), value_schema).alias("data"))
        .select(
            to_timestamp(col("data.timestamp")).alias("event_time"),
            col("data.household_id"),
            col("data.energy_kwh"),
        )
        .na.drop(subset=["event_time", "household_id", "energy_kwh"])
    )

    # Agregacja okienna: 15-minutowe okna
    windowed_agg = (
        parsed
        .withWatermark("event_time", "5 minutes")
        .groupBy(
            col("household_id"),
            window(col("event_time"), "15 minutes").alias("time_window")
        )
        .agg(
            sum("energy_kwh").alias("total_energy_kwh"),
            count("energy_kwh").alias("reading_count")
        )
        .select(
            col("household_id"),
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            col("total_energy_kwh"),
            col("reading_count")
        )
    )

    # Wykrywanie anomalii: zużycie > 2 kWh w 15-minutowym oknie
    alerts = (
        windowed_agg
        .where(col("total_energy_kwh") > 2.0)
        .select(
            col("household_id"),
            col("window_start"),
            col("window_end"),
            col("total_energy_kwh"),
            expr("'HIGH_USAGE'").alias("alert_type")
        )
    )

    # Zapis agregacji do CSV
    agg_query = (
        windowed_agg.writeStream
        .outputMode("append")
        .format("csv")
        .option("path", OUTPUT_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR + "/aggregations")
        .option("header", True)
        .start()
    )

    # Zapis alertów
    alerts_query = (
        alerts.writeStream
        .outputMode("append")
        .format("csv")
        .option("path", ALERTS_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR + "/alerts")
        .option("header", True)
        .start()
    )

    # Wyświetlanie alertów w konsoli
    console_alerts = (
        alerts.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .start()
    )

    print("Rozpoczęto przetwarzanie strumieniowe...")
    print("Agregacje są zapisywane w: " + OUTPUT_DIR)
    print("Alerty są zapisywane w: " + ALERTS_DIR)

    # Oczekiwanie na zakończenie
    agg_query.awaitTermination()
    alerts_query.awaitTermination()
    console_alerts.awaitTermination()

if __name__ == "__main__":
    main()