"""Utilitaire : affiche le count et un échantillon de la table Silver."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("count_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
df = spark.read.format("delta").load("file:///lakehouse/silver/mesures")
print("COUNT=", df.count())
print("COUNT anomalies=", df.where("is_anomalie = true").count())
df.show(5, truncate=False)
spark.stop()
