"""Utilitaire de preuve : DESCRIBE HISTORY + contenu de la table Gold agg_fenetre_machine."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("describe_gold_agg")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sql(
    "DESCRIBE HISTORY delta.`file:///lakehouse/gold/agg_fenetre_machine`"
).select("version", "timestamp", "operation", "operationParameters").show(
    20, truncate=False
)

df = spark.read.format("delta").load("file:///lakehouse/gold/agg_fenetre_machine")
print("COUNT=", df.count())
df.orderBy("machine_id", "window_start").show(20, truncate=False)
spark.stop()
