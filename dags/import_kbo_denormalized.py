"""
DAG import_kbo_denormalized — dénormalisation KBO via SQLite → MongoDB.

Alternative au job Spark (sans OOM) : les jointures se font dans un SQLite
indexé sur disque, les entreprises sont traitées par lots de 1000.
Écrit par défaut dans kbo_db.enterprises_rich (paramétrable par env).
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

import import_kbo_denormalized


@dag(
    dag_id="import_kbo_denormalized",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["kbo", "denormalized", "sqlite"],
)
def import_kbo_denormalized_dag():
    @task
    def load() -> int:
        return import_kbo_denormalized.run()

    load()


import_kbo_denormalized_dag()
