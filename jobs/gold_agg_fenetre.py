"""
Job Spark Structured Streaming — Gold 1 : agrégation fenêtre glissante par machine.

Silver (Delta, stream) → fenêtre glissante (moyenne/max valeur, nb mesures,
nb anomalies) par machine → foreachBatch :
  - MERGE INTO gold.agg_fenetre_machine (Delta, /lakehouse/gold/agg_fenetre_machine)
  - upsert Postgres (gold.agg_fenetre_machine) via psycopg2 ON CONFLICT

Fenêtre : 2 minutes glissante toutes les 1 minute (watermark 2 minutes).
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, sum as spark_sum, when, window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SILVER_PATH = "file:///lakehouse/silver/mesures"
GOLD_PATH = "file:///lakehouse/gold/agg_fenetre_machine"
CHECKPOINT = "file:///lakehouse/checkpoints/gold_agg_fenetre_machine"

PG_CONN = dict(
    host="postgres", port=5432,
    dbname="capteurs", user="capteurs", password="capteurs",
)

GOLD_SCHEMA = StructType(
    [
        StructField("machine_id", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("valeur_moyenne", DoubleType(), True),
        StructField("valeur_max", DoubleType(), True),
        StructField("nb_mesures", IntegerType(), True),
        StructField("nb_anomalies", IntegerType(), True),
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
                INSERT INTO gold.agg_fenetre_machine
                    (machine_id, window_start, window_end, valeur_moyenne,
                     valeur_max, nb_mesures, nb_anomalies)
                VALUES %s
                ON CONFLICT (machine_id, window_start) DO UPDATE SET
                    window_end = EXCLUDED.window_end,
                    valeur_moyenne = EXCLUDED.valeur_moyenne,
                    valeur_max = EXCLUDED.valeur_max,
                    nb_mesures = EXCLUDED.nb_mesures,
                    nb_anomalies = EXCLUDED.nb_anomalies
                """,
                [
                    (
                        r["machine_id"],
                        r["window_start"],
                        r["window_end"],
                        r["valeur_moyenne"],
                        r["valeur_max"],
                        r["nb_mesures"],
                        r["nb_anomalies"],
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
        batch_df = batch_df.select(
            "machine_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "valeur_moyenne",
            "valeur_max",
            "nb_mesures",
            "nb_anomalies",
        )
        batch_df.persist()
        n = batch_df.count()
        print(f"[gold_agg_fenetre_machine] batch {batch_id} : {n} lignes")
        if n == 0:
            batch_df.unpersist()
            return

        # MERGE INTO côté Delta
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)
        (
            gold_table.alias("t")
            .merge(
                batch_df.alias("s"),
                "t.machine_id = s.machine_id AND t.window_start = s.window_start",
            )
            .whenMatchedUpdateAll()
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
        SparkSession.builder.appName("gold_agg_fenetre_machine")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_gold_table(spark)

    silver = spark.readStream.format("delta").load(SILVER_PATH)

    agg = (
        silver.withWatermark("event_ts", "2 minutes")
        .groupBy(window(col("event_ts"), "2 minutes", "1 minute"), col("machine_id"))
        .agg(
            avg("valeur").alias("valeur_moyenne"),
            spark_max("valeur").alias("valeur_max"),
            count("*").cast(IntegerType()).alias("nb_mesures"),
            spark_sum(when(col("is_anomalie"), 1).otherwise(0))
            .cast(IntegerType())
            .alias("nb_anomalies"),
        )
    )

    query = (
        agg.writeStream.outputMode("update")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .foreachBatch(make_upsert_batch(spark))
        .start()
    )

    print("Gold agg_fenetre_machine streaming démarré (MERGE INTO Delta + upsert Postgres)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
