"""Utilitaire de preuve : DESCRIBE HISTORY + contenu de la table Gold etat_courant_capteur."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("describe_gold_etat")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sql(
    "DESCRIBE HISTORY delta.`file:///lakehouse/gold/etat_courant_capteur`"
).select("version", "timestamp", "operation", "operationParameters").show(
    20, truncate=False
)

df = spark.read.format("delta").load("file:///lakehouse/gold/etat_courant_capteur")
print("COUNT=", df.count())
df.orderBy("capteur_id").show(30, truncate=False)
spark.stop()
