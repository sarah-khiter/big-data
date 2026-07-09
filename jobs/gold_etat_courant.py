"""
Job Spark Structured Streaming — Gold 2 : état courant par capteur.

Silver (Delta, stream) → dernière valeur/statut par capteur_id (dans chaque
micro-batch, via row_number() partitionné par capteur_id, ordonné par
event_ts desc) → foreachBatch :
  - MERGE INTO gold.etat_courant_capteur (Delta, /lakehouse/gold/etat_courant_capteur)
  - upsert Postgres (gold.etat_courant_capteur) via psycopg2 ON CONFLICT

Pas de groupBy/watermark sur le stream lui-même : la déduplication "dernière
valeur" se fait dans le DataFrame statique reçu par foreachBatch, l'état
persistant est porté par la table cible (Delta + Postgres), pas par un state
store Spark.
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
    pour que le tout premier micro-batch passe déjà par MERGE INTO."""
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
                        r["capteur_id"], r["machine_id"], r["site_id"],
                        r["derniere_valeur"], r["derniere_unite"],
                        r["dernier_statut_anomalie"], r["derniere_qualite_signal"],
                        r["dernier_batterie_pct"], r["derniere_maj"],
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
        # Ne garder que la ligne la plus récente par capteur_id dans ce micro-batch
        w = Window.partitionBy("capteur_id").orderBy(col("event_ts").desc())
        latest = (
            batch_df.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select(
                "capteur_id",
                "machine_id",
                "site_id",
                col("valeur").alias("derniere_valeur"),
                col("unite").alias("derniere_unite"),
                col("is_anomalie").alias("dernier_statut_anomalie"),
                col("qualite_signal").alias("derniere_qualite_signal"),
                col("batterie_pourcentage").cast(IntegerType()).alias("dernier_batterie_pct"),
                col("event_ts").alias("derniere_maj"),
            )
        )
        latest.persist()
        n = latest.count()
        print(f"[gold_etat_courant_capteur] batch {batch_id} : {n} capteurs mis à jour")
        if n == 0:
            latest.unpersist()
            return

        # MERGE INTO côté Delta — n'écrase que si la donnée est plus récente
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)
        (
            gold_table.alias("t")
            .merge(latest.alias("s"), "t.capteur_id = s.capteur_id")
            .whenMatchedUpdateAll(condition="s.derniere_maj >= t.derniere_maj")
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Upsert côté Postgres (couche de service Power BI)
        rows = [row.asDict() for row in latest.collect()]
        upsert_postgres(rows)

        latest.unpersist()

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
        .trigger(processingTime="30 seconds")
        .foreachBatch(make_upsert_batch(spark))
        .start()
    )

    print("Gold etat_courant_capteur streaming démarré (MERGE INTO Delta + upsert Postgres)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
