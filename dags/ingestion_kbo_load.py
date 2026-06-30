"""
DAG ingestion_kbo_load — couche d'ingestion brute, étape 1.

Charge tous les CSV du dump KBO dans MongoDB, bruts, via Spark.
  load   : job Spark CSV → ingestion.kbo_*  (overwrite, rechargeable)
  verify : comptage exact des documents par collection (intégrité)

Déclenchable manuellement depuis l'UI Airflow (http://localhost:8080).
Attention : activity.csv = 1,5 Go → le chargement peut durer plusieurs minutes.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

import ingestion_kbo


@dag(
    dag_id="ingestion_kbo_load",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["ingestion", "kbo"],
)
def ingestion_kbo_load():
    @task
    def load() -> dict:
        return ingestion_kbo.run_load()

    @task
    def verify() -> dict:
        return ingestion_kbo.verify_counts()

    load() >> verify()


ingestion_kbo_load()
