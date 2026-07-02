"""
Point d'entrée — scraping financier NBB ciblé HÔTELLERIE.

Deux étapes :
  1. build  : filtre enterprise_silver (codes NACE 55xxx) → StateDB hotel_targets
  2. scrape : parcourt hotel_targets et récupère les dépôts NBB depuis 2021
              (PDF → HDFS, CSV → comptes_annuels), reprise via `status`.

Exemples :
    # 1. construire / rafraîchir la liste cible
    python run_hotels.py --build
    # 2. scraper les dépôts (reprenable : relancer la même commande)
    python run_hotels.py --scrape
    # tout d'un coup
    python run_hotels.py --build --scrape

Variables utiles (env) :
    DOC_MIN_YEAR      année plancher des dépôts (déf. 2021)
    MAX_ENTERPRISES   limite le nb de cibles traitées (0 = toutes) — pour tester
"""
from __future__ import annotations

import argparse
import json
import logging

import build_hotel_targets
import ingestion_documents as m
import kbopub


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description="Scraping NBB — secteur hôtellerie")
    p.add_argument("--build", action="store_true", help="construire hotel_targets depuis enterprise_silver")
    p.add_argument("--scrape", action="store_true", help="scraper les dépôts NBB des cibles")
    p.add_argument("--kbopub", action="store_true", help="scraper la fiche kbopub (dirigeants…) des cibles")
    args = p.parse_args(argv)
    if not (args.build or args.scrape or args.kbopub):
        p.error("préciser au moins --build, --scrape et/ou --kbopub")

    out = {}
    if args.build:
        out["targets"] = build_hotel_targets.build()
    if args.scrape:
        out["nbb"] = m.run_nbb_hotels()
    if args.kbopub:
        out["kbopub"] = kbopub.run_kbopub_hotels()
    if args.scrape or args.kbopub:
        out["verify"] = m.verify()
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
