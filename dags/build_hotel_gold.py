"""
DAG build_hotel_gold — Chantier 4 : recalcul annuel de la couche Gold.

Enchaîne, en s'appuyant sur la StateDB (hotel_targets) pour l'incrémental :
  1. targets  : (re)construit hotel_targets depuis enterprise_silver
                (nouvelles entreprises hôtelières prises en compte)
  2. scrape   : récupère les dépôts NBB manquants (>=2021) — run_nbb_hotels ne
                traite QUE les entreprises dont status != 'done' (incrémental)
  3. kbopub   : rafraîchit la fiche kbopub des hôtels (dirigeants, contacts…)
  4. export   : matérialise les CSV sur HDFS sous {bce}/hbb/{ref}.csv
  5. gold     : Spark relit {bce}/hbb/, recalcule les ratios et upsert hotel_gold

Planifié @yearly (nouveaux dépôts NBB annuels), en pause à la création : active-le
dans l'UI Airflow. Les entreprises déjà 'done' ne sont pas re-scrapées → on peut
relancer chaque année sans retraiter tout le dataset.
"""
from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="build_hotel_gold",
    schedule="@yearly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["gold", "hotel", "finance"],
)
def build_hotel_gold():

    @task
    def targets() -> int:
        import build_hotel_targets
        return build_hotel_targets.build()

    @task
    def scrape() -> dict:
        import ingestion_documents
        return ingestion_documents.run_nbb_hotels()

    @task
    def kbopub() -> dict:
        # fiche kbopub des hôtels (dirigeants, contacts…) — reprise via checkpoint
        import kbopub as k
        return k.run_kbopub_hotels()

    @task
    def export() -> int:
        # matérialise uniquement les CSV des hôtels sous {bce}/hbb/
        os.environ["ONLY_HOTELS"] = "true"
        import export_comptes_to_hdfs
        return export_comptes_to_hdfs.run()

    @task
    def gold() -> int:
        import build_hotel_gold as g
        return g.build()

    targets() >> scrape() >> kbopub() >> export() >> gold()


build_hotel_gold()
