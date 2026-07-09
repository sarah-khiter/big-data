"""
Utilitaire de preuve — DESCRIBE HISTORY + contenu de la table Gold 2
(gold/etat_courant_capteur). Exécuter en local[1] pour ne pas consommer
les cœurs réservés aux jobs streaming.
"""
from pyspark.sql import SparkSession

GOLD_PATH = "/lakehouse/gold/etat_courant_capteur"

spark = (
    SparkSession.builder.appName("describe_gold_etat")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=== DESCRIBE HISTORY gold/etat_courant_capteur ===")
spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_PATH}`").show(truncate=False)

print("=== Contenu actuel ===")
df = spark.read.format("delta").load(GOLD_PATH)
print(f"COUNT= {df.count()}")
df.orderBy("capteur_id").show(30, truncate=False)
