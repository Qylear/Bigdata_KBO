"""
Déduplique les activités de kbo_db.enterprise_silver (et RIEN d'autre).

Chaque entreprise liste ses activités une fois par version NACE (2003/2008/2025).
Règle : deux activités sont des DOUBLONS uniquement si elles ont le MÊME NaceCode
ET la MÊME Classification → on n'en garde qu'une. Des codes différents (ex.
70220 vs 70200) sont conservés tous les deux. MAIN et SECO (classifications
différentes) sont donc conservés.

Applique la dédup à `activites` (champs nace_code/classification) et à
`rich.activites` (champs NaceCode/Classification). Idempotent. Ordre préservé
(on garde la 1re occurrence de chaque clé).

    docker compose exec airflow-scheduler python /opt/airflow/scripts/dedup_activities_silver.py
"""
import os

from pymongo import MongoClient

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
TARGET = os.getenv("SILVER_TARGET", "enterprise_silver")


def dedup_expr(array_ref, code_field, class_field):
    """Expression Mongo : dédup d'un tableau d'activités par (code, classification)."""
    key = {"$concat": [
        {"$toString": {"$ifNull": [f"$$this.{code_field}", ""]}}, "|",
        {"$toString": {"$ifNull": [f"$$this.{class_field}", ""]}}]}
    reduce = {"$reduce": {
        "input": {"$ifNull": [array_ref, []]},
        "initialValue": {"seen": [], "out": []},
        "in": {"$let": {
            "vars": {"k": key},
            "in": {"$cond": [
                {"$in": ["$$k", "$$value.seen"]},
                "$$value",
                {"seen": {"$concatArrays": ["$$value.seen", ["$$k"]]},
                 "out": {"$concatArrays": ["$$value.out", ["$$this"]]}}]}}}}}
    return {"$let": {"vars": {"r": reduce}, "in": "$$r.out"}}


def run():
    col = MongoClient(URI)[DB][TARGET]

    r1 = col.update_many(
        {"activites": {"$type": "array"}},
        [{"$set": {"activites": dedup_expr("$activites", "nace_code", "classification")}}])
    print(f"activites       : {r1.modified_count} documents")

    r2 = col.update_many(
        {"rich.activites": {"$type": "array"}},
        [{"$set": {"rich.activites": dedup_expr("$rich.activites", "NaceCode", "Classification")}}])
    print(f"rich.activites  : {r2.modified_count} documents")

    print(f"Total : {r1.modified_count + r2.modified_count} dédup dans {DB}.{TARGET}")


if __name__ == "__main__":
    run()
