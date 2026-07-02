"""
Matérialise sur HDFS les CSV financiers déjà présents dans Mongo comptes_annuels,
sous la structure attendue par la couche Gold : {HBB_ROOT}/{bce}/hbb/{ref}.csv.

Utile pour alimenter Spark sans re-scraper : on relit raw_csv et on l'écrit sur
HDFS. Idempotent (overwrite). Ne touche pas comptes_annuels (lecture seule).

    docker compose exec airflow-scheduler python /opt/airflow/scripts/export_comptes_to_hdfs.py
    # limiter aux hôtels ciblés :
    docker compose exec -e ONLY_HOTELS=true airflow-scheduler python /opt/airflow/scripts/export_comptes_to_hdfs.py
"""
import io
import os

from hdfs import InsecureClient
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HBB_ROOT = os.getenv("HBB_ROOT", "")
ONLY_HOTELS = os.getenv("ONLY_HOTELS", "false").lower() in {"1", "true", "yes", "on"}


def run():
    db = MongoClient(MONGO_URI)[DB]
    hdfs = InsecureClient(HDFS_URL, user=HDFS_USER)

    q = {}
    if ONLY_HOTELS:
        hotels = [d["_id"] for d in db["hotel_targets"].find({}, {"_id": 1})]
        q = {"enterprise": {"$in": hotels}}
        print(f"Filtre hôtels : {len(hotels)} entreprises cibles", flush=True)

    cur = db["comptes_annuels"].find(
        q, {"enterprise": 1, "reference": 1, "raw_csv": 1}).batch_size(500)
    ok = skip = 0
    for d in cur:
        ent, ref, raw = d.get("enterprise"), d.get("reference"), d.get("raw_csv")
        if not (ent and ref and raw):
            skip += 1
            continue
        path = f"{HBB_ROOT}/{ent}/hbb/{ref}.csv"
        hdfs.write(path, data=io.BytesIO(raw.encode("utf-8")), overwrite=True)
        ok += 1
        if ok % 500 == 0:
            print(f"… {ok} CSV écrits sur HDFS", flush=True)

    print(f"Terminé : {ok} CSV écrits sous {HBB_ROOT or '/'}{{bce}}/hbb/, {skip} ignorés.",
          flush=True)
    return ok


if __name__ == "__main__":
    run()
