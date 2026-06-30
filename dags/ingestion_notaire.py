"""
DAG ingestion_notaire — statuts notaire.be (séparé car challenge F5 fragile).

Par défaut sur TOUTES les entreprises (run_notaire_all, reprenable). Pour le
limiter aux 3 entreprises de test, déclencher plutôt run_documents.py en CLI
avec --scope sample.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

import ingestion_documents


@dag(
    dag_id="ingestion_notaire",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["ingestion", "documents", "notaire"],
)
def ingestion_notaire_dag():
    @task
    def notaire() -> dict:
        return ingestion_documents.run_notaire_all()

    @task
    def verify() -> dict:
        return ingestion_documents.verify()

    notaire() >> verify()


ingestion_notaire_dag()
