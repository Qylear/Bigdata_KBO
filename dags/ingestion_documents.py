"""
DAG ingestion_documents — comptes annuels NBB sur TOUTES les entreprises,
EN AUTOMATIQUE.

  nbb     : run_nbb_all (parcours kbo_enterprise, lots + reprise, 2025/2026)
  verify  : comptage du catalogue documents

Planifié chaque nuit à 2h. Grâce aux checkpoints, chaque exécution REPREND là
où la précédente s'est arrêtée (les entreprises déjà faites sont sautées).
max_active_runs=1 → jamais deux runs en même temps : il avance en continu, nuit
après nuit, jusqu'à tout couvrir. Si un run plante, le suivant reprend.

notaire est dans son DAG dédié (ingestion_notaire).
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import ingestion_documents


@dag(
    dag_id="ingestion_documents",
    schedule="0 2 * * *",          # chaque nuit à 02:00 (modifiable)
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    max_active_runs=1,             # un seul run à la fois → reprise propre
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["ingestion", "documents", "nbb"],
)
def ingestion_documents_dag():
    @task
    def nbb() -> dict:
        return ingestion_documents.run_nbb_all()

    @task
    def verify() -> dict:
        return ingestion_documents.verify()

    nbb() >> verify()


ingestion_documents_dag()
