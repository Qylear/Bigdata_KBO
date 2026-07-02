"""
API FastAPI — Part 2 : expose les couches Silver + Gold au frontend.

Endpoints :
  GET /api/stats                  compteurs globaux
  GET /api/hotels                 recherche / liste (nom ou n° BCE)
  GET /api/hotels/{bce}           fiche : Silver + ratios Gold + dirigeants kbopub
  GET /api/hotels/{bce}/sankey    Sankey compte de résultats (CA → Marge brute → Résultat net)
  GET /api/stream/notaire         SSE : statuts notaire streamés au fil du scraping

Le frontend statique (Part 3) est servi sur "/" s'il est présent (api/static/).
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time

log = logging.getLogger("uvicorn.error")

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from hdfs import InsecureClient
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
DB = os.getenv("INGESTION_DB", "kbo_db")

db = MongoClient(MONGO_URI)[DB]
gold = db["hotel_gold"]
silver = db["enterprise_silver"]
kbopub = db["kbopub"]
documents = db["documents"]
targets = db["hotel_targets"]

hdfs = InsecureClient(os.getenv("HDFS_URL", "http://namenode:9870"),
                      user=os.getenv("HDFS_USER", "root"))
ALLOWED_ROOTS = ("/documents/", "/kbopub/", "/ejustice/")

app = FastAPI(title="Hôtellerie — Gold API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# --------------------------------------------------------------------------- #
def _latest(years):
    return max(years, key=lambda e: e.get("year", "")) if years else {}


def _denom(sv):
    dens = (sv.get("denominations") or []) if sv else []

    def _val(d):
        return d.get("valeur") or d.get("Denomination") or d.get("denomination") or d.get("value")

    # 1) dénomination officielle (type_code 001), 2) sinon n'importe laquelle
    for d in dens:
        if d.get("type_code") == "001" and _val(d):
            return _val(d)
    for d in dens:
        if _val(d):
            return _val(d)
    return None


def _adresse(sv):
    a = (sv.get("adresses") or [None])[0] if sv else None
    if not a:
        return None
    rue = " ".join(str(x) for x in [a.get("rue"), a.get("numero")] if x)
    ville = " ".join(str(x) for x in [a.get("code_postal"), a.get("commune")] if x)
    return ", ".join(x for x in [rue, ville] if x) or None


def _activites(sv):
    out = []
    for a in (sv.get("activites") or []) if sv else []:
        out.append({"nace_code": a.get("nace_code"), "libelle": a.get("nace_libelle"),
                    "classification": a.get("classification")})
    return out


def _bce_like(nom):
    """Si `nom` est un n° d'entreprise (personne morale dirigeante), renvoie le
    numéro dédotté à 10 chiffres, sinon None (personne physique)."""
    d = re.sub(r"\D", "", str(nom or ""))
    if len(d) == 9:
        d = "0" + d
    return d if len(d) == 10 else None


def _enrich_dirigeants(fonctions):
    """Résout le nom des dirigeants personnes morales (n° BCE → dénomination)."""
    refs = {n for f in fonctions if (n := _bce_like(f.get("nom")))}
    names = {}
    if refs:
        for d in silver.find({"_id": {"$in": list(refs)}}, {"denominations": 1}):
            names[d["_id"]] = _denom(d)
    out = []
    for f in fonctions:
        num = _bce_like(f.get("nom"))
        if num:
            out.append({**f, "nom": names.get(num) or f.get("nom"),
                        "ref_bce": num, "is_company": True})
        else:
            out.append(f)
    return out


# --------------------------------------------------------------------------- #
@app.get("/api/stats")
def stats():
    return {
        "entreprises": silver.estimated_document_count(),
        "hotels_gold": gold.estimated_document_count(),
        "cibles": targets.estimated_document_count(),
        "cibles_done": targets.count_documents({"status": "done"}),
        "statuts_notaire": documents.count_documents({"source": "notaire"}),
        "comptes_csv": db["comptes_annuels"].estimated_document_count(),
    }


@app.get("/api/enterprises")
def list_enterprises(q: str = "", limit: int = Query(50, le=200), skip: int = 0):
    """Couche SILVER : recherche dans TOUTES les entreprises (enterprise_silver),
    par n° BCE ou nom. `has_gold` = données financières (hôtellerie) disponibles."""
    proj = {"denominations": 1, "forme_juridique": 1, "statut": 1}
    if not q:
        docs = list(silver.find({}, proj).skip(skip).limit(limit))
    else:
        qc = q.replace(".", "").replace(" ", "")
        if qc.isdigit():                       # n° BCE (préfixe, indexé → rapide)
            docs = list(silver.find({"_id": {"$regex": "^" + re.escape(qc)}}, proj).limit(limit))
        else:
            try:                               # recherche plein-texte (index texte → rapide)
                docs = list(silver.find({"$text": {"$search": q}}, proj).limit(limit))
            except Exception:                  # repli si l'index texte n'existe pas encore (lent)
                docs = list(silver.find(
                    {"denominations.valeur": {"$regex": re.escape(q), "$options": "i"}},
                    proj).limit(limit))
    ids = [d["_id"] for d in docs]
    gold_ids = set(gold.distinct("_id", {"_id": {"$in": ids}})) if ids else set()
    items = [{"bce": d["_id"], "denomination": _denom(d),
              "forme_juridique": d.get("forme_juridique"), "statut": d.get("statut"),
              "has_gold": d["_id"] in gold_ids} for d in docs]
    return {"items": items}


@app.get("/api/hotels")
def list_hotels(q: str = "", limit: int = Query(50, le=200), skip: int = 0):
    """Liste TOUT le périmètre hôtelier (hotel_targets, ~4900), pas seulement ceux
    qui ont des comptes. Les données financières (hotel_gold) sont rattachées
    quand elles existent ; `has_gold` l'indique."""
    query = {}
    if q:
        qc = q.replace(".", "").replace(" ", "")
        if qc.isdigit():                       # n° BCE (préfixe)
            query = {"_id": {"$regex": "^" + re.escape(qc)}}
        else:                                  # nom : via le silver (index texte), intersecté hôtels
            try:
                sids = [s["_id"] for s in silver.find({"$text": {"$search": q}}, {"_id": 1}).limit(500)]
            except Exception:                  # repli si pas d'index texte
                sids = [s["_id"] for s in silver.find(
                    {"denominations.valeur": {"$regex": re.escape(q), "$options": "i"}}, {"_id": 1}).limit(500)]
            query = {"_id": {"$in": sids}}
    docs = list(targets.find(query, {"denomination": 1, "nace_codes": 1, "status": 1}
                             ).skip(skip).limit(limit))
    ids = [d["_id"] for d in docs]
    golds = {g["_id"]: g for g in gold.find(
        {"_id": {"$in": ids}}, {"nb_exercices": 1, "schema_type": 1, "years": 1, "denomination": 1})}
    svs = {s["_id"]: s for s in silver.find({"_id": {"$in": ids}}, {"denominations": 1})}
    items = []
    for d in docs:
        g = golds.get(d["_id"])
        last = _latest(g.get("years", [])) if g else {}
        name = _denom(svs.get(d["_id"])) or d.get("denomination") or (g or {}).get("denomination")
        items.append({
            "bce": d["_id"], "denomination": name, "nace_codes": d.get("nace_codes"),
            "schema_type": (g or {}).get("schema_type"), "nb_exercices": (g or {}).get("nb_exercices", 0),
            "dernier_exercice": last.get("year"), "ca": last.get("ca"),
            "resultat_net": last.get("resultat_net"), "has_gold": bool(g),
            "status": d.get("status"),
        })
    return {"total": targets.count_documents(query), "items": items}


@app.get("/api/hotels/{bce}")
def hotel(bce: str):
    g = gold.find_one({"_id": bce})
    sv = silver.find_one({"_id": bce}, {"denominations": 1, "forme_juridique": 1,
                                        "statut": 1, "adresses": 1, "activites": 1,
                                        "type_entreprise": 1})
    kp = kbopub.find_one({"_id": bce}, {"fonctions": 1, "donnees_financieres": 1,
                                        "liens_entites": 1, "contact": 1})
    if not g and not sv:
        raise HTTPException(404, "entreprise inconnue")
    return {
        "bce": bce,
        "silver": {
            "denomination": _denom(sv) or (g or {}).get("denomination"),
            "forme_juridique": (sv or {}).get("forme_juridique"),
            "statut": (sv or {}).get("statut"),
            "type_entreprise": (sv or {}).get("type_entreprise"),
            "adresse": _adresse(sv),
            "activites": _activites(sv),
        },
        "gold": {"schema_type": (g or {}).get("schema_type"),
                 "years": (g or {}).get("years", [])} if g else None,
        "dirigeants": _enrich_dirigeants((kp or {}).get("fonctions", [])),
        "donnees_financieres": (kp or {}).get("donnees_financieres"),
        "liens_entites": (kp or {}).get("liens_entites", []),
        "contact": (kp or {}).get("contact"),
    }


@app.post("/api/hotels/{bce}/dirigeants")
def scrape_dirigeants(bce: str, force: bool = False):
    """Scrape À LA DEMANDE la fiche kbopub (dirigeants + données financières +
    liens entre entités + contact). Mise en cache après."""
    import kbopub as kbopub_mod
    kbopub_mod.scrape_one(bce, force=force)
    kp = db["kbopub"].find_one({"_id": bce}) or {}
    return {
        "dirigeants": _enrich_dirigeants(kp.get("fonctions", [])),
        "donnees_financieres": kp.get("donnees_financieres"),
        "liens_entites": kp.get("liens_entites", []),
        "contact": kp.get("contact"),
    }


@app.post("/api/hotels/{bce}/ejustice")
def scrape_ejustice(bce: str, force: bool = False):
    """Scrape À LA DEMANDE les publications eJustice (+ PDF → HDFS). Cache après."""
    import ejustice
    pubs = ejustice.scrape_one(bce, force=force)
    return {"publications": pubs}


@app.get("/api/hotels/{bce}/sankey")
def sankey(bce: str, year: str = ""):
    g = gold.find_one({"_id": bce})
    if not g:
        raise HTTPException(404, "entreprise absente de la couche Gold")
    years = g.get("years", [])
    y = next((e for e in years if e.get("year") == year), _latest(years))
    if not y:
        return {"year": None, "nodes": [], "links": []}
    def num(k):
        v = y.get(k)
        return float(v) if isinstance(v, (int, float)) else None

    ca = num("ca"); ebit = num("ebit"); rai = num("resultat_avant_impots")
    net = num("resultat_net"); impots = num("impots")
    deficit = net is not None and net < 0

    if ca is not None and ca > 0 and deficit:
        # Exercice déficitaire : les charges (CA + perte) sont couvertes par le CA
        # et par la perte de l'exercice. Le CA garde sa VRAIE valeur.
        raw = [("Chiffre d'affaires", "Charges", ca),
               ("Perte de l'exercice", "Charges", -net)]
        order = ["Chiffre d'affaires", "Perte de l'exercice", "Charges"]
    else:
        # Waterfall bénéficiaire (comme le notebook)
        def pos(v):
            return v if (isinstance(v, (int, float)) and v > 0) else 0.0
        charges = (ca - ebit) if (ca is not None and ebit is not None) else None
        raw = [("Chiffre d'affaires", "Charges exploit.", pos(charges)),
               ("Chiffre d'affaires", "Résultat exploit.", pos(ebit)),
               ("Résultat exploit.", "Résultat avant impôts", pos(rai)),
               ("Résultat avant impôts", "Résultat net", pos(net)),
               ("Résultat avant impôts", "Impôts", pos(impots))]
        order = ["Chiffre d'affaires", "Charges exploit.", "Résultat exploit.",
                 "Résultat avant impôts", "Résultat net", "Impôts"]
    # on ne garde que les nœuds effectivement reliés
    used = set()
    for s, d, v in raw:
        if v > 0:
            used.add(s)
            used.add(d)
    node_order = [n for n in order if n in used]
    idx = {n: i for i, n in enumerate(node_order)}
    links = [{"source": idx[s], "target": idx[d], "value": round(v, 2)} for s, d, v in raw if v > 0]
    return {"year": y.get("year"), "deficit": deficit,
            "nodes": [{"name": n} for n in node_order], "links": links}


@app.get("/api/hotels/{bce}/documents")
def documents_list(bce: str):
    """Catalogue des documents (PDF/CSV/HTML) d'une entreprise, avec chemin HDFS."""
    out, seen = [], set()
    for d in documents.find({"enterprise": bce}):
        seen.add(d.get("hdfs_path"))
        out.append({
            "type": d.get("type"), "source": d.get("source"), "year": d.get("year"),
            "filename": d.get("filename"), "hdfs_path": d.get("hdfs_path"),
            "label": d.get("title") or d.get("reference") or d.get("filename"),
        })
    # CSV financiers présents sur HDFS ({bce}/hbb/) mais pas dans le catalogue
    for c in db["comptes_annuels"].find({"enterprise": bce}, {"reference": 1, "year": 1}):
        ref = c.get("reference")
        if not ref:
            continue
        p = f"/{bce}/hbb/{ref}.csv"
        if p in seen:
            continue
        seen.add(p)
        out.append({"type": "comptes_annuels_csv", "source": "nbb",
                    "year": str(c.get("year") or ""), "filename": f"{ref}.csv",
                    "hdfs_path": p, "label": ref})
    out.sort(key=lambda x: (x.get("source") or "", str(x.get("year") or "")))
    return {"documents": out}


def _download_allowed(path):
    if not path.startswith("/") or ".." in path:
        return False
    if any(path.startswith(r) for r in ALLOWED_ROOTS):
        return True
    return bool(re.match(r"^/\d+/hbb/[^/]+\.csv$", path))   # CSV financiers {bce}/hbb/


@app.get("/api/download")
def download(path: str):
    """Télécharge un fichier depuis HDFS (dossiers de documents uniquement)."""
    if not _download_allowed(path):
        raise HTTPException(400, "chemin non autorisé")
    fname = path.rsplit("/", 1)[-1]
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
    media = {"pdf": "application/pdf", "csv": "text/csv", "html": "text/html"}.get(
        ext, "application/octet-stream")

    def stream():
        with hdfs.read(path) as r:
            while True:
                chunk = r.read(65536)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(stream(), media_type=media,
                             headers={"Content-Disposition": f'attachment; filename="{fname}"'})


_notaire_running = set()
_notaire_lock = threading.Lock()


def _launch_notaire(bce):
    """Lance (une seule fois) le scrape notaire d'une entreprise en tâche de fond."""
    with _notaire_lock:
        if bce in _notaire_running:
            log.info("notaire: %s déjà en cours → skip", bce)
            return
        _notaire_running.add(bce)
    log.info("notaire: thread démarré pour %s", bce)

    def _work():
        lg = log
        try:
            lg.info("notaire: lancement du scrape %s", bce)
            import ingestion_documents
            res = ingestion_documents.scrape_notaire_one(bce)
            lg.info("notaire: %s terminé → %s", bce, res)
        except Exception as exc:  # noqa: BLE001
            lg.warning("notaire: %s ÉCHEC → %r", bce, exc)
        finally:
            with _notaire_lock:
                _notaire_running.discard(bce)

    threading.Thread(target=_work, daemon=True).start()


def _notaire_busy(bce):
    with _notaire_lock:
        return bce in _notaire_running


@app.get("/api/stream/notaire")
def stream_notaire(bce: str = "", scrape: bool = True, force: bool = False):
    """SSE : lance le scrape notaire à la demande (si pas déjà en cache, ou si
    force=true) et émet les statuts au fil de l'eau. `event: done` à la fin."""
    def gen():
        q = {"source": "notaire"}
        if bce:
            q["enterprise"] = bce
        launched = False
        if bce and scrape:
            cp = db["documents_checkpoints"].find_one({"_id": bce})
            done_cp = bool(cp and cp.get("notaire") == "done")
            log.info("notaire SSE bce=%s checkpoint_done=%s force=%s", bce, done_cp, force)
            if force or not done_cp:
                _launch_notaire(bce)
                launched = True
        last_ts = None
        init = list(documents.find(q).sort("downloaded_at", -1).limit(20))
        for d in reversed(init):
            last_ts = d.get("downloaded_at") or last_ts
            yield _sse(d)
        yield f"event: ready\ndata: {json.dumps({'count': len(init), 'scraping': launched})}\n\n"
        idle = 0
        while True:
            nq = dict(q)
            if last_ts is not None:
                nq["downloaded_at"] = {"$gt": last_ts}
            got = False
            for d in documents.find(nq).sort("downloaded_at", 1).limit(50):
                last_ts = d.get("downloaded_at") or last_ts
                yield _sse(d)
                got = True
            idle = 0 if got else idle + 1
            # Fin quand : requête ciblée (bce), scrape terminé, et plus rien de neuf
            if bce and not _notaire_busy(bce) and idle >= 2:
                yield "event: done\ndata: {}\n\n"
                break
            yield ": keep-alive\n\n"
            time.sleep(2)
    return StreamingResponse(gen(), media_type="text/event-stream")


def _sse(doc):
    payload = {"enterprise": doc.get("enterprise"), "year": doc.get("year"),
               "title": doc.get("title") or doc.get("filename"),
               "hdfs_path": doc.get("hdfs_path"), "deed_date": doc.get("deed_date"),
               "at": str(doc.get("downloaded_at"))}
    return f"event: statut\ndata: {json.dumps(payload, default=str)}\n\n"


_static = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static):
    app.mount("/", StaticFiles(directory=_static, html=True), name="static")
