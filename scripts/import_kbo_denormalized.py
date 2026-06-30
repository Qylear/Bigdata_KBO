"""
Dénormalisation KBO via SQLite (jointures sur disque) → MongoDB.

REPRENABLE : si Mongo/Docker plante, il suffit de relancer — les entreprises
déjà écrites dans la collection sont sautées (vérif par lot sur les _id), et le
SQLite de staging est réutilisé s'il existe (pas de rechargement inutile).

Étapes :
  1. code.csv en mémoire (libellés FR/NL) ;
  2. les 7 CSV KBO dans un SQLite de staging indexé (réutilisé si présent) ;
  3. lots de 1000 entreprises : on saute celles déjà faites, on enrichit les
     activités (NACE/ActivityGroup/Classification FR+NL), upsert en masse.

Env utiles : MONGO_DB (déf. kbo_db), MONGO_COLLECTION (déf. enterprises_rich),
KBO_DIR, SQLITE_DB, BATCH_SIZE, RESUME (déf. true).
"""
import csv
import os
import sqlite3
from collections import defaultdict

from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB_NAME = os.getenv("MONGO_DB", "kbo_db")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "enterprises_rich")
DATA_DIR = os.getenv("KBO_DIR", "/data/KBO")
SQLITE_DB = os.getenv("SQLITE_DB", "/tmp/staging_kbo.db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
RESUME = os.getenv("RESUME", "true").lower() in {"1", "true", "yes", "on"}

FILES_MAPPING = {
    "enterprise": {"file": "enterprise.csv", "id_col": "EnterpriseNumber"},
    "activity": {"file": "activity.csv", "id_col": "EntityNumber"},
    "address": {"file": "address.csv", "id_col": "EntityNumber"},
    "branch": {"file": "branch.csv", "id_col": "EnterpriseNumber"},
    "contact": {"file": "contact.csv", "id_col": "EntityNumber"},
    "establishment": {"file": "establishment.csv", "id_col": "EnterpriseNumber"},
    "denomination": {"file": "denomination.csv", "id_col": "EntityNumber"},
}


def load_codes_mapping():
    filepath = os.path.join(DATA_DIR, "code.csv")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Fichier dictionnaire introuvable : {filepath}")
    codes = defaultdict(lambda: defaultdict(dict))
    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            codes[row["Category"]][row["Code"]][row["Language"]] = row["Description"]
    print("Dictionnaire de codes chargé en mémoire.", flush=True)
    return codes


def setup_sqlite(reuse=False):
    # Réutilisation du staging existant (reprise après crash)
    if reuse and os.path.exists(SQLITE_DB):
        try:
            conn = sqlite3.connect(SQLITE_DB)
            conn.execute("SELECT count(*) FROM enterprise").fetchone()
            print(f"SQLite de staging réutilisé : {SQLITE_DB}", flush=True)
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass

    if os.path.exists(SQLITE_DB):
        os.remove(SQLITE_DB)
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    for table, meta in FILES_MAPPING.items():
        filepath = os.path.join(DATA_DIR, meta["file"])
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Fichier introuvable : {filepath}")
        with open(filepath, mode="r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader)
            cols = ", ".join([f'"{h}" TEXT' for h in headers])
            cursor.execute(f"CREATE TABLE {table} ({cols})")
            placeholders = ", ".join(["?"] * len(headers))
            insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= 100000:
                    cursor.executemany(insert_query, batch)
                    batch.clear()
            if batch:
                cursor.executemany(insert_query, batch)
        cursor.execute(f'CREATE INDEX idx_{table}_id ON {table}("{meta["id_col"]}")')
        print(f"Table SQLite '{table}' chargée et indexée.", flush=True)
    conn.commit()
    return conn


def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _enrich_activity(act, codes_dict):
    grp = act.get("ActivityGroup")
    if grp and grp in codes_dict.get("ActivityGroup", {}):
        act["ActivityGroup_Desc_FR"] = codes_dict["ActivityGroup"][grp].get("FR")
        act["ActivityGroup_Desc_NL"] = codes_dict["ActivityGroup"][grp].get("NL")
    clas = act.get("Classification")
    if clas and clas in codes_dict.get("Classification", {}):
        act["Classification_Desc_FR"] = codes_dict["Classification"][clas].get("FR")
        act["Classification_Desc_NL"] = codes_dict["Classification"][clas].get("NL")
    nace_ver = act.get("NaceVersion", "")
    nace_code = act.get("NaceCode")
    nace_cat = f"Nace{nace_ver}" if nace_ver else "Nace2008"
    if nace_code and nace_code in codes_dict.get(nace_cat, {}):
        act["NaceCode_Desc_FR"] = codes_dict[nace_cat][nace_code].get("FR")
        act["NaceCode_Desc_NL"] = codes_dict[nace_cat][nace_code].get("NL")
    return act


def process_and_load_mongo(sqlite_conn, codes_dict, resume=True):
    collection = MongoClient(MONGO_URI)[DB_NAME][COLLECTION_NAME]

    sqlite_conn.row_factory = dict_factory
    cursor = sqlite_conn.cursor()
    cursor.execute('SELECT "EnterpriseNumber" FROM enterprise')
    all_enterprises = [row["EnterpriseNumber"] for row in cursor.fetchall()]
    total = len(all_enterprises)
    print(f"\nMongoDB → {DB_NAME}.{COLLECTION_NAME} ({total} entreprises, resume={resume})...",
          flush=True)

    def children(table, key_col, ids, placeholders):
        cursor.execute(f'SELECT * FROM {table} WHERE "{key_col}" IN ({placeholders})', ids)
        out = defaultdict(list)
        for row in cursor.fetchall():
            out[row[key_col]].append(row)
        return out

    written = skipped = 0
    for i in range(0, total, BATCH_SIZE):
        batch_ids = all_enterprises[i:i + BATCH_SIZE]

        # Reprise : ignorer les entreprises déjà présentes dans la collection
        if resume:
            keys = [b.replace(".", "") for b in batch_ids]
            existing = {d["_id"] for d in
                        collection.find({"_id": {"$in": keys}}, {"_id": 1})}
            if existing:
                batch_ids = [b for b in batch_ids if b.replace(".", "") not in existing]
                skipped += len(existing)
            if not batch_ids:
                if (i + BATCH_SIZE) % 50000 < BATCH_SIZE:
                    print(f"Progression : {i + BATCH_SIZE}/{total} "
                          f"(écrites={written}, déjà faites={skipped})", flush=True)
                continue

        ph = ",".join(["?"] * len(batch_ids))
        cursor.execute(f'SELECT * FROM enterprise WHERE "EnterpriseNumber" IN ({ph})', batch_ids)
        enterprises_data = {row["EnterpriseNumber"]: row for row in cursor.fetchall()}

        activities = children("activity", "EntityNumber", batch_ids, ph)
        addresses = children("address", "EntityNumber", batch_ids, ph)
        branches = children("branch", "EnterpriseNumber", batch_ids, ph)
        contacts = children("contact", "EntityNumber", batch_ids, ph)
        establishments = children("establishment", "EnterpriseNumber", batch_ids, ph)
        denominations = children("denomination", "EntityNumber", batch_ids, ph)

        ops = []
        for ent_id, ent_doc in enterprises_data.items():
            pk = ent_id.replace(".", "")
            ent_doc["_id"] = pk
            ent_doc["activites"] = [
                _enrich_activity(a, codes_dict) for a in activities.get(ent_id, [])]
            ent_doc["adresses"] = addresses.get(ent_id, [])
            ent_doc["succursales"] = branches.get(ent_id, [])
            ent_doc["contacts"] = contacts.get(ent_id, [])
            ent_doc["etablissements"] = establishments.get(ent_id, [])
            ent_doc["denominations"] = denominations.get(ent_id, [])
            ops.append(UpdateOne({"_id": pk}, {"$set": ent_doc}, upsert=True))

        if ops:
            collection.bulk_write(ops, ordered=False)
            written += len(ops)

        if (i + BATCH_SIZE) % 50000 < BATCH_SIZE:
            print(f"Progression : {i + BATCH_SIZE}/{total} "
                  f"(écrites={written}, déjà faites={skipped})", flush=True)

    print(f"Terminé : {written} écrites, {skipped} déjà présentes "
          f"({written + skipped}/{total}).", flush=True)
    return written + skipped


def run():
    codes_dict = load_codes_mapping()
    conn = setup_sqlite(reuse=RESUME)
    try:
        n = process_and_load_mongo(conn, codes_dict, resume=RESUME)
    finally:
        conn.close()
    # Nettoyage du staging uniquement après un parcours complet sans crash
    if os.path.exists(SQLITE_DB):
        os.remove(SQLITE_DB)
    return n


if __name__ == "__main__":
    run()
