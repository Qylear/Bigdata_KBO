"""
Point d'entrée — récupération des documents.

Périmètre :
  --scope sample : les 3 entreprises du notebook (test)
  --scope all    : TOUTES les entreprises de kbo_enterprise (lots + reprise)

Sources : --source nbb | notaire | all

Exemples :
    # test sur 3 entreprises
    python run_documents.py --scope sample --source all
    # tout le registre, NBB seulement, entreprises actives
    python run_documents.py --scope all --source nbb
    # reprendre (les entreprises déjà 'done' sont sautées automatiquement)
    python run_documents.py --scope all --source nbb

Variables utiles (env) :
    KBO_SRC_DB        base contenant kbo_enterprise (déf. ingestion ; mettre bronze si besoin)
    ONLY_ACTIVE       true|false  — ne traiter que les entreprises actives (déf. true)
    MAX_ENTERPRISES   limite (0 = toutes) — pratique pour tester
    DOC_BATCH_SIZE    fréquence des logs de progression (déf. 1000)
"""
from __future__ import annotations

import argparse
import json
import logging

import ingestion_documents as m


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description="Récupération des documents (NBB / notaire)")
    p.add_argument("--scope", choices=["sample", "all"], default="all")
    p.add_argument("--source", choices=["nbb", "notaire", "all"], default="nbb")
    args = p.parse_args(argv)

    out = {}
    if args.scope == "sample":
        if args.source in ("nbb", "all"):
            out["nbb"] = m.run_nbb()
        if args.source in ("notaire", "all"):
            out["notaire"] = m.run_notaire()
    else:  # all
        if args.source in ("nbb", "all"):
            out["nbb"] = m.run_nbb_all()
        if args.source in ("notaire", "all"):
            out["notaire"] = m.run_notaire_all()

    out["verify"] = m.verify()
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
