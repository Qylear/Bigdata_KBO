"""
Ingestion — chargement brut des CSV KBO dans MongoDB via Spark.

Couche d'ingestion : on atterrit la donnée *telle quelle*.
  - aucune jointure, aucune traduction des codes (ça, c'est l'étape suivante) ;
  - toutes les colonnes restent en texte (pas d'inférence de type) ;
  - on ajoute seulement des colonnes de provenance.

Chaque CSV → une collection  ingestion.kbo_<nom>  (overwrite = rechargeable).
Le connecteur mongo-spark est pré-résolu dans l'image (cache Ivy /opt/spark-ivy).
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

MONGO_SPARK_PKG = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"

KBO_FILES = {
    "branch": "branch.csv",
    "code": "code.csv",
    "contact": "contact.csv",
    "establishment": "establishment.csv",
    "enterprise": "enterprise.csv",
    "denomination": "denomination.csv",
    "address": "address.csv",
    "activity": "activity.csv",
}
META_FILE = "meta.csv"


def _cfg():
    return {
        "kbo_dir": os.getenv("KBO_DIR", "/data/KBO"),
        "mongo_uri": os.getenv(
            "MONGO_URI",
            "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin",
        ),
        "db": os.getenv("INGESTION_DB", "kbo_db"),
    }


def build_spark(mongo_uri: str) -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("ingestion-kbo-load")
        .config("spark.jars.packages", MONGO_SPARK_PKG)
        .config("spark.jars.ivy", "/opt/spark-ivy")
        .config("spark.mongodb.read.connection.uri", mongo_uri)
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_snapshot_date(spark: SparkSession, kbo_dir: str) -> str | None:
    """Lit meta.csv (clé/valeur) pour récupérer SnapshotDate (provenance)."""
    path = f"{kbo_dir}/{META_FILE}"
    try:
        rows = (
            spark.read.option("header", True).option("quote", '"').csv(path).collect()
        )
        meta = {r["Variable"]: r["Value"] for r in rows}
        return meta.get("SnapshotDate")
    except Exception as exc:  # noqa: BLE001
        log.warning("meta.csv illisible (%s) — provenance sans snapshot", exc)
        return None


def _read_csv(spark: SparkSession, path: str):
    """Lecture brute : tout en string, pas d'inférence de type."""
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", False)
        .option("mode", "PERMISSIVE")
        .csv(path)
    )


def load_csv_to_mongo(spark, key, filename, cfg, snapshot) -> dict:
    path = f"{cfg['kbo_dir']}/{filename}"
    coll = f"kbo_{key}"
    log.info("Chargement %s → %s.%s", filename, cfg["db"], coll)

    df = _read_csv(spark, path)
    df = (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
        .withColumn("_snapshot_date", F.lit(snapshot))
    )

    (
        df.write.format("mongodb")
        .mode("overwrite")
        .option("database", cfg["db"])
        .option("collection", coll)
        .save()
    )
    n = df.count()
    log.info("  → %s : %d documents", coll, n)
    return {"collection": coll, "rows": n}


def run_load() -> dict:
    """Charge tous les CSV KBO + meta dans Mongo. Une seule SparkSession."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    spark = build_spark(cfg["mongo_uri"])
    try:
        snapshot = read_snapshot_date(spark, cfg["kbo_dir"])
        log.info("Snapshot KBO : %s", snapshot)

        results = []
        for key, filename in KBO_FILES.items():
            results.append(load_csv_to_mongo(spark, key, filename, cfg, snapshot))

        meta_df = (
            spark.read.option("header", True).option("quote", '"')
            .csv(f"{cfg['kbo_dir']}/{META_FILE}")
            .withColumn("_ingested_at", F.current_timestamp())
        )
        (
            meta_df.write.format("mongodb").mode("overwrite")
            .option("database", cfg["db"]).option("collection", "kbo_meta").save()
        )
    finally:
        spark.stop()

    summary = {"snapshot": snapshot, "collections": results}
    log.info("Ingestion KBO terminée : %s", summary)
    return summary


def verify_counts() -> dict:
    """Compte les documents de chaque collection (contrôle d'intégrité)."""
    from pymongo import MongoClient

    cfg = _cfg()
    client = MongoClient(cfg["mongo_uri"], serverSelectionTimeoutMS=10000)
    db = client[cfg["db"]]
    counts = {}
    for key in KBO_FILES:
        counts[f"kbo_{key}"] = db[f"kbo_{key}"].count_documents({})
    counts["kbo_meta"] = db["kbo_meta"].count_documents({})

    empty = [c for c, n in counts.items() if n == 0]
    if empty:
        raise AssertionError(f"Collections vides : {empty}")
    log.info("Vérification OK : %s", counts)
    return counts


if __name__ == "__main__":
    run_load()
    print(verify_counts())
