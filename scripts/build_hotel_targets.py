"""
StateDB du scraping financier NBB — secteur HÔTELLERIE.

On filtre `enterprise_silver` pour extraire les entreprises dont AU MOINS une
activité (principale MAIN *ou* secondaire SECO) porte un code NACE d'hébergement
(55xxx), puis on les charge dans la collection d'état `hotel_targets` (kbo_db).
Cette collection pilote ensuite le scraping des dépôts NBB (reprise via `status`).

Codes NACE retenus (Nace2008 + Nace2025) :
  55100  Hôtels et hébergement similaire
  55201  Auberges de jeunesse
  55202  Centres et villages de vacances
  55203  Gîtes de vacances, appartements et meublés de vacances
  55204  Chambres d'hôtes
  55209  Autres hébergements de courte durée n.c.a.
  55300  Terrains de camping et parcs pour caravanes
  55400  Intermédiation pour l'hébergement (Nace2025, type Airbnb/Booking)
  55900  Autres hébergements

Idempotent : un ré-run met à jour la liste sans écraser le `status` déjà
atteint par le scraping ($setOnInsert sur status/created_at). Ne modifie
QUE `hotel_targets` (lecture seule sur enterprise_silver).

    docker compose exec airflow-scheduler python /opt/airflow/scripts/build_hotel_targets.py
"""
import datetime as dt
import os

from pymongo import MongoClient, UpdateOne

URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
SILVER = os.getenv("SILVER_TARGET", "enterprise_silver")
STATE = os.getenv("HOTEL_STATE", "hotel_targets")

# NaceCode est stocké en chaîne ("55100") dans enterprise_silver.
HOTEL_CODES = ["55100", "55201", "55202", "55203", "55204",
               "55209", "55300", "55400", "55900"]

# Formes juridiques exclues (entités publiques) — codes rich.JuridicalForm.
EXCLUDED_FORMS = [
    "110", "114", "116", "117",                       # entités publiques
    "301", "302", "303",                              # services fédéraux
    "310", "320", "330", "340", "350",                # autorités régionales
    "400", "411", "412", "413", "414", "415",         # communes, CPAS,
    "416", "417", "418", "419", "420",                # intercommunales
]

CODE_LABELS = {
    "55100": "Hôtels et hébergement similaire",
    "55201": "Auberges de jeunesse",
    "55202": "Centres et villages de vacances",
    "55203": "Gîtes de vacances, appartements et meublés de vacances",
    "55204": "Chambres d'hôtes",
    "55209": "Autres hébergements de courte durée n.c.a.",
    "55300": "Terrains de camping et parcs pour caravanes",
    "55400": "Intermédiation pour l'hébergement (Nace2025)",
    "55900": "Autres hébergements",
}


def _now():
    return dt.datetime.now(dt.timezone.utc)


def _name(doc):
    """Récupère une dénomination lisible, quel que soit le champ présent."""
    for k in ("denomination", "denominations", "nom", "noms", "name"):
        v = doc.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, list) and v:
            first = v[0]
            if isinstance(first, str):
                return first
            if isinstance(first, dict):
                for kk in ("Denomination", "denomination", "value", "nom"):
                    if first.get(kk):
                        return str(first[kk])
    rich = doc.get("rich") or {}
    for k in ("denominations", "Denominations", "noms"):
        v = rich.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            for kk in ("Denomination", "denomination", "value"):
                if v[0].get(kk):
                    return str(v[0][kk])
    return None


def _matched_codes(doc):
    """Codes hôteliers portés par l'activité PRINCIPALE (MAIN)."""
    found = set()
    for a in (doc.get("activites") or []):
        if str(a.get("classification", "")).strip() != "MAIN":
            continue
        c = str(a.get("nace_code", "")).strip()
        if c in CODE_LABELS:
            found.add(c)
    return sorted(found)


def build():
    db = MongoClient(URI)[DB]
    silver = db[SILVER]
    state = db[STATE]
    state.create_index("status")
    run_ts = _now()  # horodatage de ce build → sert à purger les cibles obsolètes

    # Filtres (cf. cahier des charges) :
    #   Status AC + TypeOfEnterprise 2 (personne morale privée)
    #   activité PRINCIPALE (MAIN) portant un code NACE hôtelier (même élément)
    #   hors formes juridiques publiques (JuridicalForm exclus)
    query = {
        "rich.Status": "AC",
        "rich.TypeOfEnterprise": "2",
        "rich.JuridicalForm": {"$nin": EXCLUDED_FORMS},
        "activites": {"$elemMatch": {"classification": "MAIN",
                                     "nace_code": {"$in": HOTEL_CODES}}},
    }
    proj = {"activites": 1, "denominations": 1, "rich.JuridicalForm": 1,
            "rich.denominations": 1}

    total = silver.count_documents(query)
    print(f"Entreprises hôtelières (filtres stricts) : {total}", flush=True)
    if total == 0:
        print("⚠ 0 match — vérifier les champs de filtre (aucune écriture).", flush=True)
        return 0

    ops, upserts, per_code = [], 0, {c: 0 for c in CODE_LABELS}
    cur = silver.find(query, proj).batch_size(500)
    for doc in cur:
        num = str(doc["_id"])
        codes = _matched_codes(doc)
        for c in codes:
            per_code[c] += 1
        ops.append(UpdateOne(
            {"_id": num},
            {"$setOnInsert": {"status": "pending", "created_at": run_ts},
             "$set": {"nace_codes": codes,
                      "juridical_form": (doc.get("rich") or {}).get("JuridicalForm"),
                      "denomination": _name(doc), "updated_at": run_ts}},
            upsert=True))
        if len(ops) >= 1000:
            upserts += state.bulk_write(ops, ordered=False).upserted_count
            ops = []
    if ops:
        upserts += state.bulk_write(ops, ordered=False).upserted_count

    # Purge : cibles de l'ancien scope, non revues par ce build et pas encore
    # scrapées (on ne supprime jamais une cible déjà 'done').
    pruned = state.delete_many(
        {"updated_at": {"$lt": run_ts}, "status": {"$ne": "done"}}).deleted_count

    print(f"\n{STATE} : {state.count_documents({})} cibles "
          f"({upserts} nouvelles, {pruned} obsolètes purgées).", flush=True)
    print("Répartition par code NACE (activité principale) :", flush=True)
    for c in HOTEL_CODES:
        print(f"  {c}  {CODE_LABELS[c]:<52} {per_code[c]}", flush=True)
    print("\nStatuts :", flush=True)
    for st in ("pending", "done", "error"):
        print(f"  {st:<8} {state.count_documents({'status': st})}", flush=True)
    return state.count_documents({})


if __name__ == "__main__":
    build()
