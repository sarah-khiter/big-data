"""
Job Spark Structured Streaming — Bronze.

Kafka (topic mesures) → parsing JSON typé → Delta append (/lakehouse/bronze/mesures).
Checkpoint dédié, partitionnement par event_date.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "mesures"
BRONZE_PATH = "file:///lakehouse/bronze/mesures"
CHECKPOINT = "file:///lakehouse/checkpoints/bronze_mesures"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("capteur_id", StringType(), False),
        StructField("machine_id", StringType(), False),
        StructField("site_id", StringType(), False),
        StructField("type_mesure", StringType(), False),
        StructField("valeur", DoubleType(), False),
        StructField("unite", StringType(), False),
        StructField("qualite_signal", DoubleType(), False),
        StructField("batterie_pourcentage", IntegerType(), False),
        StructField("timestamp", StringType(), False),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("bronze_mesures")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("event_date", to_date(col("event_ts")))
    )

    # Capture brute : tous les champs + colonnes dérivées pour le partitionnement
    bronze = parsed.select(
        "event_id",
        "capteur_id",
        "machine_id",
        "site_id",
        "type_mesure",
        "valeur",
        "unite",
        "qualite_signal",
        "batterie_pourcentage",
        "timestamp",
        col("event_ts").cast(TimestampType()),
        "event_date",
    )

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )

    print(f"Bronze streaming démarré → {BRONZE_PATH} (checkpoint: {CHECKPOINT})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
