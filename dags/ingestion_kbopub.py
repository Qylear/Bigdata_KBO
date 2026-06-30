"""
DAG ingestion_kbopub — scraping de la fiche publique KBO (2.1–2.5, 2.8, 2.10).

  scrape : run_kbopub (toutes les entreprises, lots + reprise, via Tor)
  verify : comptage de kbo_db.kbopub

HTML brut archivé sur HDFS (/kbopub/{num}.html), champs parsés dans Mongo.
Planifié chaque nuit ; reprenable (checkpoint 'kbopub').
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import kbopub


@dag(
    dag_id="ingestion_kbopub",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingestion", "kbopub", "web"],
)
def ingestion_kbopub_dag():
    @task
    def scrape() -> dict:
        return kbopub.run_kbopub()

    @task
    def verify() -> dict:
        return kbopub.verify()

    scrape() >> verify()


ingestion_kbopub_dag()
