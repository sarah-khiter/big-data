"""
Job Spark Structured Streaming — Gold 2 : état courant par capteur.

Silver (Delta, stream) → dernière valeur/statut connu par capteur_id →
foreachBatch :
  - MERGE INTO gold.etat_courant_capteur (Delta, /lakehouse/gold/etat_courant_capteur)
  - upsert Postgres (gold.etat_courant_capteur) via psycopg2 ON CONFLICT

Contrairement à Silver/Gold 1, ce job n'a pas d'opérateur stateful
(pas de watermark/groupBy sur le stream) : la déduplication "dernière valeur
par capteur" se fait par micro-batch avec une window function classique, pas
un state store Spark. Pas besoin de spark.hadoop.fs.permissions.umask-mode
ici (cf. Incident 1 du rôle C).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_PATH = "file:///lakehouse/silver/mesures"
GOLD_PATH = "file:///lakehouse/gold/etat_courant_capteur"
CHECKPOINT = "file:///lakehouse/checkpoints/gold_etat_courant_capteur"

PG_CONN = dict(
    host="postgres", port=5432,
    dbname="capteurs", user="capteurs", password="capteurs",
)

GOLD_SCHEMA = StructType(
    [
        StructField("capteur_id", StringType(), False),
        StructField("machine_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("derniere_valeur", DoubleType(), True),
        StructField("derniere_unite", StringType(), True),
        StructField("dernier_statut_anomalie", BooleanType(), True),
        StructField("derniere_qualite_signal", DoubleType(), True),
        StructField("dernier_batterie_pct", IntegerType(), True),
        StructField("derniere_maj", TimestampType(), True),
    ]
)


def ensure_gold_table(spark: SparkSession) -> None:
    """Crée la table Delta Gold (vide, schéma figé) si elle n'existe pas encore,
    pour que le tout premier micro-batch puisse déjà passer par MERGE INTO."""
    if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        empty = spark.createDataFrame([], GOLD_SCHEMA)
        empty.write.format("delta").mode("overwrite").save(GOLD_PATH)


def upsert_postgres(rows: list[dict]) -> None:
    import psycopg2
    from psycopg2.extras import execute_values

    if not rows:
        return
    conn = psycopg2.connect(**PG_CONN)
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO gold.etat_courant_capteur
                    (capteur_id, machine_id, site_id, derniere_valeur,
                     derniere_unite, dernier_statut_anomalie,
                     derniere_qualite_signal, dernier_batterie_pct, derniere_maj)
                VALUES %s
                ON CONFLICT (capteur_id) DO UPDATE SET
                    machine_id = EXCLUDED.machine_id,
                    site_id = EXCLUDED.site_id,
                    derniere_valeur = EXCLUDED.derniere_valeur,
                    derniere_unite = EXCLUDED.derniere_unite,
                    dernier_statut_anomalie = EXCLUDED.dernier_statut_anomalie,
                    derniere_qualite_signal = EXCLUDED.derniere_qualite_signal,
                    dernier_batterie_pct = EXCLUDED.dernier_batterie_pct,
                    derniere_maj = EXCLUDED.derniere_maj
                WHERE EXCLUDED.derniere_maj >= gold.etat_courant_capteur.derniere_maj
                """,
                [
                    (
                        r["capteur_id"],
                        r["machine_id"],
                        r["site_id"],
                        r["derniere_valeur"],
                        r["derniere_unite"],
                        r["dernier_statut_anomalie"],
                        r["derniere_qualite_signal"],
                        r["dernier_batterie_pct"],
                        r["derniere_maj"],
                    )
                    for r in rows
                ],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def make_upsert_batch(spark: SparkSession):
    def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Un micro-batch peut contenir plusieurs mesures pour le même capteur :
        # on ne garde que la plus récente (event_ts max) par capteur_id.
        latest_per_capteur = Window.partitionBy("capteur_id").orderBy(col("event_ts").desc())
        batch_df = (
            batch_df.withColumn("rn", row_number().over(latest_per_capteur))
            .filter(col("rn") == 1)
            .select(
                "capteur_id",
                "machine_id",
                "site_id",
                col("valeur").alias("derniere_valeur"),
                col("unite").alias("derniere_unite"),
                col("is_anomalie").alias("dernier_statut_anomalie"),
                col("qualite_signal").alias("derniere_qualite_signal"),
                col("batterie_pourcentage").alias("dernier_batterie_pct"),
                col("event_ts").alias("derniere_maj"),
            )
        )
        batch_df.persist()
        n = batch_df.count()
        print(f"[gold_etat_courant_capteur] batch {batch_id} : {n} lignes")
        if n == 0:
            batch_df.unpersist()
            return

        # MERGE INTO côté Delta — ne met à jour que si la mesure est plus récente
        # que l'état déjà enregistré (protège contre un micro-batch en retard)
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)
        (
            gold_table.alias("t")
            .merge(batch_df.alias("s"), "t.capteur_id = s.capteur_id")
            .whenMatchedUpdateAll(condition="s.derniere_maj >= t.derniere_maj")
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Upsert côté Postgres (couche de service Power BI)
        rows = [row.asDict() for row in batch_df.collect()]
        upsert_postgres(rows)

        batch_df.unpersist()

    return upsert_batch


def main() -> None:
    spark = (
        SparkSession.builder.appName("gold_etat_courant_capteur")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_gold_table(spark)

    silver = spark.readStream.format("delta").load(SILVER_PATH)

    query = (
        silver.writeStream.outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="15 seconds")
        .foreachBatch(make_upsert_batch(spark))
        .start()
    )

    print("Gold etat_courant_capteur streaming démarré (MERGE INTO Delta + upsert Postgres)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
