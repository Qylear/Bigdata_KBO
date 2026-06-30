"""
DAG pipeline_kbo — pipeline d'ingestion complet, de bout en bout.

Enchaîne automatiquement, dans l'ordre :
  1. kbo_load     : CSV KBO bruts → kbo_db.kbo_*            (Spark)
  2. consolidate  : jointures + traduction → enterprises_rich (SQLite, reprenable)
  3. documents    : comptes annuels NBB → HDFS + Mongo      (toutes entreprises)
  4. verify       : rapport de comptage

Planifié tous les mois (le KBO publie un snapshot mensuel), mais en pause à la
création : active-le dans l'UI Airflow quand tu veux qu'il tourne seul.
notaire reste hors de ce pipeline (DAG ingestion_notaire dédié).
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="pipeline_kbo",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["pipeline", "kbo"],
)
def pipeline_kbo():
    @task
    def kbo_load() -> dict:
        import ingestion_kbo
        return ingestion_kbo.run_load()

    @task
    def consolidate() -> int:
        import import_kbo_denormalized
        return import_kbo_denormalized.run()

    @task
    def documents() -> dict:
        import ingestion_documents
        return ingestion_documents.run_nbb_all()

    @task
    def verify() -> dict:
        import ingestion_documents
        return ingestion_documents.verify()

    kbo_load() >> consolidate() >> documents() >> verify()


pipeline_kbo()
