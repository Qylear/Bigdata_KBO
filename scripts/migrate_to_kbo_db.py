"""
Consolide les collections récentes (base `ingestion`) vers la cible (kbo_db).

Avant la correction de l'env, les runs NBB/notaire/kbopub ont écrit dans
`ingestion`. Ce script copie ces collections vers `kbo_db` via $merge.

- Collections déjà présentes dans la cible → ignorées (pas d'écrasement)
  (ex. kbopub, qu'on a déjà re-scrapé proprement dans kbo_db).
- Les `kbo_*` bruts (base `bronze`, des millions de docs) ne sont PAS copiés
  ici : recharge-les plutôt dans kbo_db avec ingestion_kbo.py (depuis les CSV).

    docker compose exec airflow-scheduler python /opt/airflow/scripts/migrate_to_kbo_db.py
"""
import os

from pymongo import MongoClient

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
TARGET = os.getenv("INGESTION_DB", "kbo_db")
SOURCES = [s for s in os.getenv("MIGRATE_FROM", "ingestion").split(",") if s.strip()]


def main():
    client = MongoClient(URI)
    dbs = client.list_database_names()
    moved = skipped = 0

    for src in SOURCES:
        if src == TARGET or src not in dbs:
            continue
        target_colls = set(client[TARGET].list_collection_names())
        for coll in client[src].list_collection_names():
            if coll in target_colls:
                print(f"  skip (déjà dans {TARGET}) : {coll}")
                skipped += 1
                continue
            n = client[src][coll].estimated_document_count()
            print(f"  copie {src}.{coll} ({n} docs) → {TARGET}.{coll} …")
            client[src][coll].aggregate([
                {"$merge": {"into": {"db": TARGET, "coll": coll},
                            "whenMatched": "keepExisting",
                            "whenNotMatched": "insert"}}
            ])
            moved += 1

    print(f"\nTerminé : {moved} collections copiées, {skipped} ignorées.")
    print(f"Dans {TARGET} :", sorted(client[TARGET].list_collection_names()))


if __name__ == "__main__":
    main()
