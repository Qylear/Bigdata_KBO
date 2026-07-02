"""Inspecte enterprise_silver : où vivent Status / TypeOfEnterprise / JuridicalForm
et la classification des activités (codes bruts vs libellés traduits)."""
import json
import os

from pymongo import MongoClient

c = MongoClient(os.environ["MONGO_URI"])["kbo_db"]["enterprise_silver"]

# un doc hôtelier au hasard (au moins un code 55xxx)
d = c.find_one({"$or": [
    {"activites.nace_code": {"$in": ["55100", "55203", 55100]}},
    {"rich.activites.NaceCode": {"$in": ["55100", "55203"]}}]})

print("=== TOP-LEVEL KEYS ===")
print(sorted(d.keys()))
print("\n=== RICH KEYS ===")
print(sorted((d.get("rich") or {}).keys()) if isinstance(d.get("rich"), dict) else "pas de rich")

def show(path, obj, keys):
    print(f"\n=== {path} ===")
    for k in keys:
        if k in obj:
            print(f"  {k} = {obj[k]!r}")

show("top", d, ["Status", "status", "statut", "TypeOfEnterprise", "type_entreprise",
                "JuridicalForm", "forme_juridique", "JuridicalSituation"])
rich = d.get("rich") or {}
if isinstance(rich, dict):
    show("rich", rich, ["Status", "TypeOfEnterprise", "JuridicalForm",
                        "JuridicalSituation", "TypeOfEnterpriseFR", "JuridicalFormFR"])

print("\n=== activites[0] (top) ===")
print(json.dumps((d.get("activites") or [{}])[0], ensure_ascii=False, default=str))
print("=== rich.activites[0] ===")
print(json.dumps((rich.get("activites") or [{}])[0], ensure_ascii=False, default=str))

# valeurs distinctes utiles pour construire les filtres
print("\n=== distinct TypeOfEnterprise (rich) ===", c.distinct("rich.TypeOfEnterprise")[:20])
print("=== distinct JuridicalForm (rich) [échantillon] ===", c.distinct("rich.JuridicalForm")[:30])
print("=== distinct classification (activites) ===", c.distinct("activites.classification")[:20])
print("=== distinct Classification (rich.activites) ===", c.distinct("rich.activites.Classification")[:20])
print("=== distinct Status (top) ===", c.distinct("Status")[:10], "| statut:", c.distinct("statut")[:10])
