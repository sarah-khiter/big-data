"""Utilitaire : affiche le count et un échantillon de la table Bronze."""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("count_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
df = spark.read.format("delta").load("file:///lakehouse/bronze/mesures")
print("COUNT=", df.count())
df.show(3, truncate=False)
spark.stop()
