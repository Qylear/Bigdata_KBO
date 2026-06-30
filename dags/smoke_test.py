"""
DAG smoke_test — une tâche par brique de la stack.

Déclenchable manuellement depuis l'UI Airflow (http://localhost:8080).
Vert sur toute la ligne = la stack est opérationnelle.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from checks import check_hdfs, check_mongo, check_spark, check_tor


@dag(
    dag_id="smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["infra", "smoke"],
)
def smoke_test():
    @task
    def hdfs():
        return check_hdfs()

    @task
    def mongo():
        return check_mongo()

    @task
    def tor():
        return check_tor()

    @task
    def spark():
        return check_spark()

    # Indépendantes : s'exécutent en parallèle
    [hdfs(), mongo(), tor(), spark()]


smoke_test()
