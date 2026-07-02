"""
DAG build_silver — couche silver enterprise_silver (jointure globale Mongo).

Unifie entities + enterprises_rich et jointe kbopub, ejustice, documents,
comptes_annuels par numéro d'entreprise → kbo_db.enterprise_silver.
À relancer après chaque avancée des scrapers pour rafraîchir la vue unifiée.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import build_silver


@dag(
    dag_id="build_silver",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["silver", "kbo"],
)
def build_silver_dag():
    @task
    def build() -> int:
        return build_silver.build()

    build()


build_silver_dag()
