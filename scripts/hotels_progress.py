"""Avancement du scraping financier hôtelier (StateDB hotel_targets + données)."""
import os

from pymongo import MongoClient

db = MongoClient(os.environ["MONGO_URI"])["kbo_db"]
state = db["hotel_targets"]

total = state.count_documents({})
done = state.count_documents({"status": "done"})
pending = state.count_documents({"status": "pending"})
error = state.count_documents({"status": "error"})
pct = (100 * (done + error) / total) if total else 0

bar = "#" * int(pct / 2) + "-" * (50 - int(pct / 2))
print(f"\nAVANCEMENT  [{bar}] {pct:5.1f}%")
print(f"  cibles   : {total}")
print(f"  done     : {done}")
print(f"  pending  : {pending}")
print(f"  error    : {error}")

# Total de dépôts financiers effectivement récupérés (filings_count cumulé)
agg = list(state.aggregate([
    {"$match": {"status": "done"}},
    {"$group": {"_id": None, "filings": {"$sum": "$filings_count"}}}]))
filings = agg[0]["filings"] if agg else 0
print(f"\n  dépôts scrapés (filings_count cumulé) : {filings}")
print(f"  CSV dans comptes_annuels             : {db['comptes_annuels'].count_documents({})}")
print(f"  PDF dans documents (source=nbb)      : {db['documents'].count_documents({'source': 'nbb'})}")

# Cibles avec 0 dépôt (aucun compte déposé depuis 2021) et dernières erreurs
zero = state.count_documents({"status": "done", "filings_count": 0})
print(f"\n  done mais 0 dépôt (pas de compte >=2021) : {zero}")

errs = list(state.find({"status": "error"}, {"error": 1}).limit(5))
if errs:
    print("\n  dernières erreurs :")
    for e in errs:
        print(f"    {e['_id']} : {str(e.get('error'))[:90]}")
