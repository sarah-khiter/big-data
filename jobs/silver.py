"""
Job Spark Structured Streaming — Silver.

Bronze (Delta, stream) → dédoublonnage (watermark + dropDuplicates) →
détection d'anomalie par seuil (jointure broadcast avec referentiel/capteurs.csv,
colonne is_anomalie, aucune ligne supprimée) → Delta Silver (append).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

BRONZE_PATH = "file:///lakehouse/bronze/mesures"
SILVER_PATH = "file:///lakehouse/silver/mesures"
CHECKPOINT = "file:///lakehouse/checkpoints/silver_mesures"
CAPTEURS_CSV = "file:///referentiel/capteurs.csv"

CAPTEURS_SCHEMA = StructType(
    [
        StructField("capteur_id", StringType(), False),
        StructField("type_mesure", StringType(), False),
        StructField("plage_nominale_min", DoubleType(), False),
        StructField("plage_nominale_max", DoubleType(), False),
        StructField("fabricant", StringType(), True),
        StructField("date_installation", StringType(), True),
        StructField("precision_capteur", DoubleType(), True),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("silver_mesures")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Référentiel capteurs : statique, chargé une fois, diffusé (broadcast join)
    capteurs = (
        spark.read.option("header", "true")
        .schema(CAPTEURS_SCHEMA)
        .csv(CAPTEURS_CSV)
        .select(
            "capteur_id",
            col("plage_nominale_min").alias("ref_min"),
            col("plage_nominale_max").alias("ref_max"),
        )
    )

    bronze = spark.readStream.format("delta").load(BRONZE_PATH)

    # Dédoublonnage : watermark sur le temps évènement + clé métier event_id
    deduped = bronze.withWatermark("event_ts", "10 minutes").dropDuplicates(
        ["event_id"]
    )

    # Détection d'anomalie par seuil (hors plage nominale du capteur) — rien n'est
    # supprimé, la ligne est conservée avec is_anomalie=true/false
    enriched = deduped.join(broadcast(capteurs), "capteur_id", "left")
    silver = enriched.withColumn(
        "is_anomalie",
        when(
            col("valeur").isNull() | col("ref_min").isNull() | col("ref_max").isNull(),
            False,
        ).otherwise((col("valeur") < col("ref_min")) | (col("valeur") > col("ref_max"))),
    ).select(
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
        "event_ts",
        "event_date",
        "is_anomalie",
    )

    query = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )

    print(f"Silver streaming démarré → {SILVER_PATH} (checkpoint: {CHECKPOINT})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
