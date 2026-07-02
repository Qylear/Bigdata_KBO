"""
Ne garde que le SIÈGE SOCIAL enregistré dans les adresses de enterprise_silver
(et RIEN d'autre). Les autres adresses sont retirées.

- `adresses` (issu de entities, type traduit)  : on garde type == \"Siège\"
  (REGO traduit ; on tolère aussi \"REGO\" au cas où non traduit).
- `rich.adresses` (données brutes)             : on garde TypeOfAddress == \"REGO\".

Idempotent. N'agit que sur enterprise_silver.

    docker compose exec airflow-scheduler python /opt/airflow/scripts/filter_address_silver.py
"""
import os

from pymongo import MongoClient

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
TARGET = os.getenv("SILVER_TARGET", "enterprise_silver")


def keep(array_ref, field, values):
    return {"$filter": {
        "input": {"$ifNull": [array_ref, []]},
        "as": "a",
        "cond": {"$in": [f"$$a.{field}", values]}}}


def run():
    col = MongoClient(URI)[DB][TARGET]

    r1 = col.update_many(
        {"adresses": {"$type": "array"}},
        [{"$set": {"adresses": keep("$adresses", "type", ["Siège", "REGO"])}}])
    print(f"adresses       : {r1.modified_count} documents")

    r2 = col.update_many(
        {"rich.adresses": {"$type": "array"}},
        [{"$set": {"rich.adresses": keep("$rich.adresses", "TypeOfAddress", ["REGO"])}}])
    print(f"rich.adresses  : {r2.modified_count} documents")

    print(f"Total : {r1.modified_count + r2.modified_count} filtrages dans {DB}.{TARGET}")


if __name__ == "__main__":
    run()
