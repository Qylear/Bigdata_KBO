"""
Couche SILVER — construit kbo_db.enterprise_silver.

Un document par entreprise (clé = numéro SANS points), qui réunit :
  - base `entities`   : consolidation traduite (statut, forme juridique, NACE…)
  - `rich`            : `enterprises_rich` (activités FR/NL, succursales, …)
  - `kbopub`          : fiche web (dirigeants, capital, contact, liens…)
  - `ejustice`        : publications au Moniteur belge
  - `documents`       : PDF NBB/notaire liés (catalogue)
  - `comptes_annuels` : CSV financiers NBB liés

⚠ Les collections n'utilisent pas le même format de numéro : `entities` a des
POINTS (0203.430.576), les autres n'en ont PAS (0203430576). On normalise donc
la clé de jointure (_num = numéro sans points) et l'_id du silver est sans points.

Aggregation MongoDB uniquement ($lookup + $merge côté serveur) — pas de Spark,
pas d'OOM. Ne modifie AUCUNE collection source (lecture seule) ; écrit seulement
enterprise_silver.

    docker compose exec airflow-scheduler python /opt/airflow/scripts/build_silver.py
"""
import os

from pymongo import MongoClient

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
BASE = os.getenv("SILVER_BASE", "entities")
TARGET = os.getenv("SILVER_TARGET", "enterprise_silver")


def _nodots(field):
    return {"$replaceAll": {"input": {"$toString": field}, "find": ".", "replacement": ""}}


def build():
    db = MongoClient(URI)[DB]
    existing = set(db.list_collection_names())
    if BASE not in existing:
        raise SystemExit(f"Base '{BASE}' absente. Mets SILVER_BASE=enterprises_rich au besoin.")

    # Index sur la clé de jointure 1-à-N (accélère le lookup ; n'altère pas les données)
    for coll in ("documents", "comptes_annuels", "nbb_financials_raw"):
        if coll in existing:
            db[coll].create_index("enterprise")

    # Reconstruit proprement notre collection cible (pas une source)
    db[TARGET].drop()

    pipeline = [
        # clé normalisée (sans points) à partir de l'_id de la base
        {"$set": {"_num": _nodots("$_id")}},
    ]

    # Jointures 1-à-1 (rich, kbopub, ejustice) sur la clé normalisée
    for coll, field in [("enterprises_rich", "rich"),
                        ("kbopub", "kbopub"),
                        ("ejustice", "ejustice")]:
        if coll in existing and coll != BASE:
            tmp = f"_{field}"
            pipeline += [
                {"$lookup": {
                    "from": coll, "let": {"n": "$_num"},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$_id", "$$n"]}}}],
                    "as": tmp}},
                {"$set": {field: {"$first": f"${tmp}"}}},
                {"$unset": tmp},
            ]

    # Jointure 1-à-N : documents (PDF NBB/notaire)
    if "documents" in existing:
        pipeline.append({"$lookup": {
            "from": "documents", "let": {"n": "$_num"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$enterprise", "$$n"]}}}],
            "as": "documents"}})

    # Jointure 1-à-N : comptes financiers = comptes_annuels + nbb_financials_raw
    # (le renommage a laissé d'anciens CSV dans nbb_financials_raw → on combine).
    fin = [c for c in ("comptes_annuels", "nbb_financials_raw") if c in existing]
    for i, c in enumerate(fin):
        pipeline.append({"$lookup": {
            "from": c, "let": {"n": "$_num"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$enterprise", "$$n"]}}}],
            "as": f"_fin{i}"}})
    if fin:
        pipeline.append({"$set": {
            "comptes_annuels": {"$concatArrays": [f"$_fin{i}" for i in range(len(fin))]}}})
        pipeline.append({"$unset": [f"_fin{i}" for i in range(len(fin))]})

    # Dédup des activités : doublon = même NaceCode ET même Classification
    # (codes différents conservés ; MAIN/SECO conservés). Ordre préservé.
    def _dedup(array_ref, code_field, class_field):
        key = {"$concat": [
            {"$toString": {"$ifNull": [f"$$this.{code_field}", ""]}}, "|",
            {"$toString": {"$ifNull": [f"$$this.{class_field}", ""]}}]}
        reduce = {"$reduce": {
            "input": {"$ifNull": [array_ref, []]},
            "initialValue": {"seen": [], "out": []},
            "in": {"$let": {"vars": {"k": key}, "in": {"$cond": [
                {"$in": ["$$k", "$$value.seen"]}, "$$value",
                {"seen": {"$concatArrays": ["$$value.seen", ["$$k"]]},
                 "out": {"$concatArrays": ["$$value.out", ["$$this"]]}}]}}}}}
        return {"$let": {"vars": {"r": reduce}, "in": "$$r.out"}}

    # Adresses : ne garder que le siège social enregistré (REGO / \"Siège\")
    def _keep_rego(array_ref, field, values):
        return {"$filter": {"input": {"$ifNull": [array_ref, []]},
                            "as": "a", "cond": {"$in": [f"$$a.{field}", values]}}}

    pipeline.append({"$set": {"adresses": _keep_rego("$adresses", "type", ["Siège", "REGO"])}})
    pipeline.append({"$set": {"rich": {"$cond": [
        {"$eq": [{"$type": "$rich"}, "object"]},
        {"$mergeObjects": ["$rich", {"adresses": _keep_rego("$rich.adresses", "TypeOfAddress", ["REGO"])}]},
        "$rich"]}}})

    pipeline.append({"$set": {"activites": _dedup("$activites", "nace_code", "classification")}})
    pipeline.append({"$set": {"rich": {"$cond": [
        {"$eq": [{"$type": "$rich"}, "object"]},
        {"$mergeObjects": ["$rich", {"activites": _dedup("$rich.activites", "NaceCode", "Classification")}]},
        "$rich"]}}})

    # Convertit date_creation (StartDate \"DD-MM-YYYY\") en chaîne \"YYYY-MM-DD\"
    # (propre à l'affichage, sans heure, et comparable/triable directement).
    def _ymd(field):
        ref = f"${field}"
        return {"$let": {
            "vars": {"d": {"$dateFromString": {"dateString": ref, "format": "%d-%m-%Y",
                                               "onError": None, "onNull": None}}},
            "in": {"$cond": [{"$eq": ["$$d", None]}, ref,
                             {"$dateToString": {"format": "%Y-%m-%d", "date": "$$d"}}]}}}

    pipeline.append({"$set": {"date_creation": _ymd("date_creation")}})
    # idem pour rich.StartDate quand rich est présent
    pipeline.append({"$set": {"rich": {"$cond": [
        {"$eq": [{"$type": "$rich"}, "object"]},
        {"$mergeObjects": ["$rich", {"StartDate": _ymd("rich.StartDate")}]},
        "$rich"]}}})

    # _id du silver = numéro sans points, puis écriture
    pipeline += [
        {"$set": {"_id": "$_num"}},
        {"$unset": "_num"},
        {"$merge": {"into": TARGET, "on": "_id",
                    "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]

    print(f"Construction de {DB}.{TARGET} depuis '{BASE}' (quelques minutes)…", flush=True)
    db[BASE].aggregate(pipeline, allowDiskUse=True)

    n = db[TARGET].count_documents({})
    print(f"Terminé : {n} documents dans {TARGET}.", flush=True)
    return n


if __name__ == "__main__":
    build()
