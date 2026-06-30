"""
Rapport de validation de l'avancée — état réel des données ingérées.

À lancer dans le conteneur airflow :
    docker compose exec airflow-scheduler python /opt/airflow/scripts/verify_progress.py

Interroge HDFS + MongoDB et affiche, section par section, ce qui est présent
(✓) ou manquant (✗), avec les volumes.
"""
import os

OK, KO, WARN = "✓", "✗", "•"


def _mongo():
    from pymongo import MongoClient
    return MongoClient(
        os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin"),
        serverSelectionTimeoutMS=8000)


def _hdfs():
    from hdfs import InsecureClient
    return InsecureClient(os.getenv("HDFS_URL", "http://namenode:9870"),
                          user=os.getenv("HDFS_USER", "root"))


def _count(db, coll):
    try:
        return db[coll].count_documents({})
    except Exception:
        return 0


def header(title):
    print(f"\n{'='*64}\n  {title}\n{'='*64}")


def main():
    client = _mongo()
    dbs = client.list_database_names()

    # ------------------------------------------------------------------ #
    header("1. Connectivité")
    try:
        client.admin.command("ping")
        print(f"  {OK} MongoDB joignable — bases : {', '.join(d for d in dbs if d not in ('admin','local','config'))}")
    except Exception as e:
        print(f"  {KO} MongoDB : {e}")
    try:
        h = _hdfs()
        root = h.list("/")
        print(f"  {OK} HDFS joignable — racine : {root}")
    except Exception as e:
        h = None
        print(f"  {KO} HDFS : {e}")

    # ------------------------------------------------------------------ #
    header("2. KBO brut (CSV → Mongo)")
    found_kbo_db = None
    for dbname in ("kbo_db", "ingestion", "bronze"):
        if dbname not in dbs:
            continue
        db = client[dbname]
        kbo_colls = sorted(c for c in db.list_collection_names() if c.startswith("kbo_"))
        if kbo_colls:
            found_kbo_db = dbname
            print(f"  {OK} base '{dbname}' — {len(kbo_colls)} collections KBO :")
            for c in kbo_colls:
                print(f"      {c:20s} {_count(db, c):>12,}")
            break
    if not found_kbo_db:
        print(f"  {KO} aucune collection kbo_* trouvée (lancer ingestion_kbo.py)")

    # ------------------------------------------------------------------ #
    header("3. Entités consolidées (jointures + traduction)")
    candidates = [("kbo_db", "enterprises_rich")]
    any_entity = False
    for dbname, coll in candidates:
        if dbname in dbs and coll in client[dbname].list_collection_names():
            n = _count(client[dbname], coll)
            if n:
                any_entity = True
                print(f"  {OK} {dbname}.{coll} : {n:,} fiches")
                sample = client[dbname][coll].find_one({"_id": "0203430576"}) \
                    or client[dbname][coll].find_one()
                if sample:
                    keys = [k for k in sample.keys() if not k.startswith("_")]
                    print(f"      champs : {', '.join(keys[:12])}{'…' if len(keys)>12 else ''}")
                    for arr in ("activites", "adresses", "denominations", "contacts",
                                "etablissements", "succursales"):
                        if arr in sample:
                            print(f"      {arr:14s} : {len(sample[arr])} éléments")
    if not any_entity:
        print(f"  {KO} pas de fiches consolidées "
              f"(lancer transform_entities.py ou import_kbo_denormalized.py)")

    # ------------------------------------------------------------------ #
    header("4. Documents (PDF → HDFS, méta → Mongo)")
    ing = client["kbo_db"] if "kbo_db" in dbs else None
    if ing is not None and "documents" in ing.list_collection_names():
        tot = _count(ing, "documents")
        nbb = ing["documents"].count_documents({"source": "nbb"})
        notaire = ing["documents"].count_documents({"source": "notaire"})
        csv = _count(ing, "comptes_annuels")
        print(f"  {OK} catalogue documents : {tot} (nbb={nbb}, notaire={notaire})")
        print(f"  {OK if csv else KO} comptes_annuels (CSV financiers) : {csv}")
        # Répartition par entreprise
        for ent in ing["documents"].distinct("enterprise"):
            c = ing["documents"].count_documents({"enterprise": ent})
            print(f"      {ent} : {c} documents")
    else:
        print(f"  {KO} pas de catalogue documents (lancer ingestion_documents.py)")

    # HDFS : arborescence /documents
    if h is not None:
        try:
            info = h.content("/documents", strict=False)
            if info:
                mo = info.get("length", 0) / (1024*1024)
                print(f"  {OK} HDFS /documents : {info.get('fileCount',0)} fichiers, "
                      f"{mo:.1f} Mo, {info.get('directoryCount',0)} dossiers")
            else:
                print(f"  {KO} HDFS /documents absent")
        except Exception as e:
            print(f"  {WARN} HDFS /documents : {e}")

    print("\nRapport terminé.\n")


if __name__ == "__main__":
    main()
