"""
DAG ingestion_ejustice — publications eJustice / Moniteur belge (section 2.9).

  scrape : run_ejustice (toutes les entreprises, pagination, lots + reprise)
  verify : comptage de kbo_db.ejustice

HTML brut archivé sur HDFS (/ejustice/{num}/p{page}.html), publications parsées
dans Mongo. Planifié chaque nuit ; reprenable (checkpoint 'ejustice').
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import ejustice


@dag(
    dag_id="ingestion_ejustice",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "ejustice", "web"],
)
def ingestion_ejustice_dag():
    @task
    def scrape() -> dict:
        return ejustice.run_ejustice()

    @task
    def verify() -> dict:
        return ejustice.verify()

    scrape() >> verify()


ingestion_ejustice_dag()
