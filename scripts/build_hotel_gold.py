"""
Couche GOLD — kbo_db.hotel_gold (un document par entreprise hôtelière).

Part 1 (Spark + MongoDB) : Spark lit en parallèle les CSV PCMN bruts depuis HDFS
sous la structure {bce}/hbb/{ref}.csv (format "code_pcmn;valeur"), calcule les
ratios financiers de chaque exercice, et consolide tout en UN document par
entreprise (upsert sur enterprise_number).

Mapping PCMN → Gold :
  70=chiffre_affaires  60=achats  71=variation_stocks  9901=ebit  9904=resultat_net
  54+55=tresorerie  17+43=dettes_financieres  10/15=fonds_propres  100=capital_souscrit
Ratios :
  marge_brute = CA - Achats + Variation stocks
  marge_nette (%) = Résultat net / CA * 100      roe (%) = Résultat net / Fonds propres * 100
  ratio_liquidite = Trésorerie / Dettes fin.     taux_endettement (%) = Dettes fin. / Fonds propres * 100

Source : GOLD_SOURCE=hdfs (défaut, Spark sur {bce}/hbb/) ; fallback Mongo
comptes_annuels si HDFS vide. Lecture seule sur les sources ; écrit hotel_gold.

    docker compose exec airflow-scheduler python /opt/airflow/scripts/build_hotel_gold.py
    docker compose exec airflow-scheduler python /opt/airflow/scripts/build_hotel_gold.py --inspect
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys
from collections import defaultdict

from pymongo import MongoClient, UpdateOne

MONGO_URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")
STATE = os.getenv("HOTEL_STATE", "hotel_targets")
TARGET = os.getenv("GOLD_TARGET", "hotel_gold")
SOURCE = os.getenv("GOLD_SOURCE", "hdfs").lower()
HDFS_RPC = os.getenv("HDFS_RPC", "hdfs://namenode:9000")
HBB_ROOT = os.getenv("HBB_ROOT", "")

DATE_KEY = "Accounting period end date"  # ligne du CSV donnant l'année d'exercice

SINGLE = {
    "ca": ["70"],
    "achats": ["60"],
    "variation_stocks": ["71"],
    "ebit": ["9901", "9900"],
    "resultat_avant_impots": ["9903"],
    "resultat_net": ["9904"],
    "fonds_propres": ["10/15"],
    "capital_souscrit": ["100", "10"],
}
# Agrégats : détaillé si présent (schéma complet), sinon agrégat (abrégé/micro).
AGG = {
    "tresorerie":         {"granular": ["54", "55"], "agg": ["54/58", "50/53"]},
    "dettes_financieres": {"granular": ["17", "43"], "agg": ["42/48"]},
}
NEEDED_CODES = sorted(
    {c for v in SINGLE.values() for c in v}
    | {c for spec in AGG.values() for c in spec["granular"] + spec["agg"]}
    | {DATE_KEY})


def _now():
    return dt.datetime.now(dt.timezone.utc)


def to_float(s):
    if s is None:
        return None
    t = str(s).strip().replace(" ", "").replace("\xa0", "")
    if not t or t in {"-", "."}:
        return None
    neg = t.startswith("-") or (t.startswith("(") and t.endswith(")"))
    t = t.lstrip("-").strip("()")
    if "." in t and "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        v = float(t)
        return -v if neg else v
    except ValueError:
        return None


def first(codes, variants):
    for c in variants:
        if c in codes:
            v = to_float(codes[c])
            if v is not None:
                return v
    return None


def agg_value(codes, spec):
    vals = [to_float(codes[c]) for c in spec["granular"] if c in codes]
    vals = [v for v in vals if v is not None]
    if vals:
        return sum(vals)
    return first(codes, spec["agg"])


def _ratio(num, den, mult=1):
    if num is None or den in (None, 0):
        return None
    return round(num / den * mult, 4)


def year_from(codes, reference):
    """Année d'exercice = année de 'Accounting period end date', sinon préfixe ref."""
    m = re.search(r"(19|20)\d{2}", str(codes.get(DATE_KEY, "")))
    if m:
        return m.group(0)
    head = str(reference or "").split("-")[0]
    return head if re.fullmatch(r"(19|20)\d{2}", head) else "?"


def compute_year(year, reference, codes):
    ca = first(codes, SINGLE["ca"])
    achats = first(codes, SINGLE["achats"])
    var_stocks = first(codes, SINGLE["variation_stocks"])
    ebit = first(codes, SINGLE["ebit"])
    net = first(codes, SINGLE["resultat_net"])
    fp = first(codes, SINGLE["fonds_propres"])
    capital = first(codes, SINGLE["capital_souscrit"])
    treso = agg_value(codes, AGG["tresorerie"])
    dettes = agg_value(codes, AGG["dettes_financieres"])
    rai = first(codes, SINGLE["resultat_avant_impots"])   # résultat avant impôts (9903)
    impots = (rai - net) if (rai is not None and net is not None) else None
    charges_expl = (ca - ebit) if (ca is not None and ebit is not None) else None
    marge_brute = (ca - (achats or 0) + (var_stocks or 0)) if ca is not None else None
    # ROE et taux d'endettement n'ont de sens que si les fonds propres sont > 0.
    fp_ok = fp is not None and fp > 0

    return {
        "year": str(year), "reference": reference,
        "ca": ca, "achats": achats, "variation_stocks": var_stocks,
        "marge_brute": marge_brute, "charges_exploitation": charges_expl,
        "ebit": ebit, "resultat_avant_impots": rai, "impots": impots, "resultat_net": net,
        "tresorerie": treso, "dettes_financieres": dettes,
        "fonds_propres": fp, "capital_souscrit": capital,
        "fonds_propres_negatifs": (fp is not None and fp <= 0),
        "ratios": {
            "marge_nette": _ratio(net, ca, 100),
            "roe": _ratio(net, fp, 100) if fp_ok else None,
            "ratio_liquidite": _ratio(treso, dettes),
            "taux_endettement": _ratio(dettes, fp, 100) if fp_ok else None,
        },
    }


def infer_schema_type(years):
    if any(y.get("ca") is not None for y in years):
        return "full"
    if any(y.get("fonds_propres") is not None for y in years):
        return "abrege"
    return "micro"


# --------------------------------------------------------------------------- #
def load_from_hdfs(hotel_nums) -> dict:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[*]").appName("hotel-gold")
        .config("spark.hadoop.fs.defaultFS", HDFS_RPC)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate())
    try:
        pattern = f"{HDFS_RPC}{HBB_ROOT}/*/hbb/*.csv"
        raw = spark.read.text(pattern).withColumn("path", F.input_file_name())
        # Séparateur tolérant : ; OU , (le CSV NBB réel est en virgule malgré la
        # spec). code = texte avant le 1er délimiteur ; val = le reste.
        pat = r'^\s*"?([^";,]+)"?\s*[;,]\s*"?(.*?)"?\s*$'
        raw = (raw
               .withColumn("enterprise", F.regexp_extract("path", r"/([^/]+)/hbb/", 1))
               .withColumn("reference", F.regexp_extract("path", r"/hbb/(.+)\.csv$", 1))
               .withColumn("code", F.trim(F.regexp_extract("value", pat, 1)))
               .withColumn("val", F.trim(F.regexp_extract("value", pat, 2))))
        raw = raw.filter(F.col("code").isin(NEEDED_CODES) & (F.col("val") != ""))
        hotels_df = spark.createDataFrame([(n,) for n in hotel_nums], ["enterprise"])
        raw = raw.join(F.broadcast(hotels_df), "enterprise", "inner")
        rows = raw.select("enterprise", "reference", "code", "val").collect()
    finally:
        spark.stop()

    # regroupe par fichier (entreprise, référence), puis déduit l'année
    by_file = defaultdict(dict)
    for r in rows:
        by_file[(r["enterprise"], r["reference"])][r["code"]] = r["val"]
    out = {}
    for (ent, ref), codes in by_file.items():
        out[(ent, year_from(codes, ref))] = {"ref": ref, "codes": codes}
    return out


def load_from_mongo(db, hotel_set) -> dict:
    out = {}
    cur = db["comptes_annuels"].find(
        {"enterprise": {"$in": list(hotel_set)}},
        {"enterprise": 1, "year": 1, "reference": 1, "codes": 1}).batch_size(500)
    for d in cur:
        codes = {str(k): v for k, v in (d.get("codes") or {}).items()}
        year = str(d.get("year") or year_from(codes, d.get("reference")))
        out[(d["enterprise"], year)] = {"ref": d.get("reference"), "codes": codes}
    return out


def inspect(db, hotel_set):
    freq = defaultdict(int)
    n = 0
    for d in db["comptes_annuels"].find({"enterprise": {"$in": list(hotel_set)}},
                                        {"codes": 1}).limit(3000):
        n += 1
        for k in (d.get("codes") or {}):
            freq[str(k)] += 1
    print(f"Échantillon : {n} dépôts hôteliers\n\nCodes attendus — présence :")
    reporting = list(SINGLE.items()) + [(k, v["granular"] + v["agg"]) for k, v in AGG.items()]
    for name, variants in reporting:
        best = max((freq.get(c, 0) for c in variants), default=0)
        print(f"  {name:<20} {variants} : {best}/{n} ({100*best/n if n else 0:4.1f}%)")


# --------------------------------------------------------------------------- #
def build():
    db = MongoClient(MONGO_URI)[DB]
    hotel_docs = {d["_id"]: d for d in db[STATE].find({}, {"denomination": 1, "nace_codes": 1})}
    hotel_set = set(hotel_docs)
    if not hotel_set:
        print("⚠ hotel_targets vide — lancer d'abord run_hotels.py --build.")
        return 0
    if "--inspect" in sys.argv:
        inspect(db, hotel_set)
        return 0

    exercises = {}
    if SOURCE == "hdfs":
        try:
            exercises = load_from_hdfs(sorted(hotel_set))
        except Exception as exc:  # noqa: BLE001
            print(f"HDFS/Spark indisponible ({exc}) → fallback Mongo.", flush=True)
    if not exercises:
        print("Lecture depuis Mongo comptes_annuels…", flush=True)
        exercises = load_from_mongo(db, hotel_set)
    if not exercises:
        print("⚠ Aucun exercice trouvé (CSV pas encore scrapés/exportés ?).")
        return 0

    per_ent = defaultdict(list)
    for (ent, year), payload in exercises.items():
        per_ent[ent].append(compute_year(year, payload["ref"], payload["codes"]))

    ops = []
    for ent, years in per_ent.items():
        years.sort(key=lambda e: e["year"])
        prev = None
        for y in years:
            ca = y["ca"]
            y["ratios"]["croissance_ca"] = (
                round((ca - prev) / prev, 4) if (prev not in (None, 0) and ca is not None) else None)
            if ca is not None:
                prev = ca
        meta = hotel_docs.get(ent, {})
        ops.append(UpdateOne(
            {"_id": ent},
            {"$set": {"enterprise_number": ent, "denomination": meta.get("denomination"),
                      "nace_codes": meta.get("nace_codes"),
                      "schema_type": infer_schema_type(years),
                      "nb_exercices": len(years), "years": years, "last_updated": _now()}},
            upsert=True))
        if len(ops) >= 1000:
            db[TARGET].bulk_write(ops, ordered=False)
            ops = []
    if ops:
        db[TARGET].bulk_write(ops, ordered=False)

    n = db[TARGET].count_documents({})
    with_ca = db[TARGET].count_documents({"years.ca": {"$ne": None}})
    print(f"{TARGET} : {n} entreprises consolidées ({with_ca} avec un CA).", flush=True)
    return n


if __name__ == "__main__":
    build()
