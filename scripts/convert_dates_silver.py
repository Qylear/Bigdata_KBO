"""
Convertit les dates de kbo_db.enterprise_silver (et RIEN d'autre).

La StartDate KBO est une string \"DD-MM-YYYY\". On la met au format \"YYYY-MM-DD\"
(chaîne, sans heure) : propre à l'affichage ET comparable/triable directement en
MongoDB (l'ordre lexical d'ISO = l'ordre chronologique).

Idempotent : gère les valeurs déjà en Date (issues d'un run précédent), les
strings \"DD-MM-YYYY\", et laisse intactes celles déjà en \"YYYY-MM-DD\".
N'agit que sur enterprise_silver.

    docker compose exec airflow-scheduler python /opt/airflow/scripts/convert_dates_silver.py
"""
import os

from pymongo import MongoClient

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
TARGET = os.getenv("SILVER_TARGET", "enterprise_silver")

FIELDS = ["date_creation", "rich.StartDate"]


def ymd_expr(field):
    """Expression Mongo : valeur (Date OU 'DD-MM-YYYY' OU 'YYYY-MM-DD') → 'YYYY-MM-DD'."""
    ref = f"${field}"
    return {"$cond": [
        {"$eq": [{"$type": ref}, "date"]},
        {"$dateToString": {"format": "%Y-%m-%d", "date": ref}},
        {"$let": {
            "vars": {"d": {"$dateFromString": {
                "dateString": ref, "format": "%d-%m-%Y",
                "onError": None, "onNull": None}}},
            "in": {"$cond": [
                {"$eq": ["$$d", None]}, ref,
                {"$dateToString": {"format": "%Y-%m-%d", "date": "$$d"}}]}}}]}


def run():
    col = MongoClient(URI)[DB][TARGET]
    total = 0
    for field in FIELDS:
        res = col.update_many(
            {field: {"$type": ["date", "string"]}},
            [{"$set": {field: ymd_expr(field)}}])
        print(f"{field} : {res.modified_count} documents convertis")
        total += res.modified_count
    print(f"Total : {total} conversions dans {DB}.{TARGET}")
    return total


if __name__ == "__main__":
    run()
