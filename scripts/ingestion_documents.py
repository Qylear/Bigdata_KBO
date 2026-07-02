"""
Ingestion — récupération des documents (PDF) → HDFS, métadonnées → MongoDB.

Sources :
  - NBB / CBSO (comptes annuels) : API HTTP, via le POOL Tor (haproxy).
      PDF            → HDFS  /documents/{num}/{annee}/comptes_{ref}.pdf
      CSV financier  → Mongo collection `nbb_financials_raw` (brut, sans KPI)
  - notaire.be (statuts) : challenge F5 → Playwright + Tor DÉDIÉ (IP stable).
      PDF            → HDFS  /documents/{num}/{annee}/statut_{docId}.pdf

Catalogue de provenance : collection `documents` (source, année, URL, chemin
HDFS, taille, sha256, date).

Résilience : chaque requête est réessayée ; comme la session force
"Connection: close", chaque tentative repasse par le round-robin HAProxy →
un autre exit Tor. Une entreprise en échec n'interrompt pas les autres.
"""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import io
import json
import logging
import os
import time

import requests
from urllib.parse import urlencode

log = logging.getLogger(__name__)

ENTREPRISES = {
    "Google Belgium": "0878065378",
    "Apple Retail Belgium": "0836157420",
    "SNCB": "0203430576",
}

NBB_BASE = "https://consult.cbso.nbb.be/api"
NOTAIRE_BASE = "https://statuts.notaire.be/stapor_v1"
NOTAIRE_SEED = "0836157420"


# --------------------------------------------------------------------------- #
def _cfg():
    return {
        "hdfs_url": os.getenv("HDFS_URL", "http://namenode:9870"),
        "hdfs_user": os.getenv("HDFS_USER", "root"),
        "docs_root": os.getenv("HDFS_DOCS_ROOT", "/documents"),
        "mongo_uri": os.getenv(
            "MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin"),
        "db": os.getenv("INGESTION_DB", "kbo_db"),
        "tor_pool": os.getenv("TOR_SOCKS", "socks5h://haproxy:9050"),
        "tor_notaire": os.getenv("NOTAIRE_TOR_SOCKS", "socks5h://tor-notaire:9050"),
        "delay": float(os.getenv("REQUEST_DELAY", "0.5")),
        # Parcours de TOUTES les entreprises (depuis kbo_enterprise)
        "src_db": os.getenv("KBO_SRC_DB", os.getenv("INGESTION_DB", "kbo_db")),
        "only_active": os.getenv("ONLY_ACTIVE", "true").lower() in {"1", "true", "yes", "on"},
        "max_enterprises": int(os.getenv("MAX_ENTERPRISES", "0")),
        "batch_size": int(os.getenv("DOC_BATCH_SIZE", "1000")),
        "batch_pause": float(os.getenv("DOC_BATCH_PAUSE", "0")),
        # Années à scraper (dépôt). Vide = toutes. Déf. 2025 et 2026.
        "years": {y.strip() for y in os.getenv("DOC_YEARS", "2025,2026").split(",") if y.strip()},
    }


def _now():
    return dt.datetime.now(dt.timezone.utc)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# --------------------------------------------------------------------------- #
class Store:
    def __init__(self, cfg):
        from hdfs import InsecureClient
        from pymongo import MongoClient

        self.cfg = cfg
        self.hdfs = InsecureClient(cfg["hdfs_url"], user=cfg["hdfs_user"])
        self.db = MongoClient(cfg["mongo_uri"])[cfg["db"]]
        self.documents = self.db["documents"]
        self.comptes = self.db["comptes_annuels"]
        self.documents.create_index("enterprise")
        self.documents.create_index([("source", 1), ("enterprise", 1), ("year", 1)])
        self.checkpoints = self.db["documents_checkpoints"]
        self.checkpoints.create_index("nbb")
        self.checkpoints.create_index("notaire")
        # Base contenant kbo_enterprise (peut différer de la base d'écriture)
        self.src = self.db.client[cfg["src_db"]]

    def hdfs_path(self, enterprise, year, filename):
        return f"{self.cfg['docs_root']}/{enterprise}/{year}/{filename}"

    def put_pdf(self, enterprise, year, filename, data, source, doc_type, url, extra=None):
        path = self.hdfs_path(enterprise, year, filename)
        self.hdfs.write(path, data=io.BytesIO(data), overwrite=True)
        doc = {
            "_id": f"{source}:{enterprise}:{filename}",
            "source": source, "type": doc_type, "enterprise": enterprise,
            "year": str(year), "filename": filename, "url": url, "hdfs_path": path,
            "size_bytes": len(data), "sha256": _sha256(data), "downloaded_at": _now(),
        }
        if extra:
            doc.update(extra)
        self.documents.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        log.info("HDFS ← %s (%d Ko)", path, len(data) // 1024)
        return path

    def put_nbb_csv(self, enterprise, year, reference, deposit_id, codes, raw_text):
        self.comptes.replace_one(
            {"_id": deposit_id},
            {"_id": deposit_id, "enterprise": enterprise, "year": str(year),
             "reference": reference, "codes": codes, "raw_csv": raw_text,
             "ingested_at": _now()},
            upsert=True)

    # --- Reprise (checkpoints) ---
    def is_done(self, enterprise, source):
        cp = self.checkpoints.find_one({"_id": enterprise})
        return bool(cp and cp.get(source) == "done")

    def mark(self, enterprise, source, status, info=None):
        upd = {source: status}
        if info is not None:
            upd[f"info_{source}"] = info
        self.checkpoints.update_one({"_id": enterprise}, {"$set": upd}, upsert=True)


# --------------------------------------------------------------------------- #
def _tor_session(socks: str) -> requests.Session:
    s = requests.Session()
    s.proxies = {"http": socks, "https": socks}
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/149.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fr-BE,fr;q=0.9,en;q=0.8",
        # Nouvelle connexion à chaque requête → nouvel exit Tor via le round-robin
        "Connection": "close",
    })
    return s


# Codes signalant une limite/blocage côté serveur → on change d'IP (exit Tor)
RATE_LIMIT_CODES = {403, 429, 503}


def _http_get(sess, url, attempts=8, connect_to=20, read_to=90, **kw):
    """GET avec retries. Chaque tentative ouvre une nouvelle connexion
    (Connection: close) → un autre exit Tor via le round-robin HAProxy.
    Sur 403/429/503 (IP limitée), on réessaie donc sur une autre IP."""
    last_exc = None
    resp = None
    for i in range(attempts):
        try:
            resp = sess.get(url, timeout=(connect_to, read_to), **kw)
        except requests.RequestException as exc:
            last_exc = exc
            log.warning("GET échec (%d/%d) %s : %s", i + 1, attempts, url, exc)
            time.sleep(2)
            continue
        if resp.status_code in RATE_LIMIT_CODES:
            log.warning("HTTP %s (limite IP) sur %s — rotation d'exit (%d/%d)",
                        resp.status_code, url, i + 1, attempts)
            time.sleep(2 + i)
            continue
        return resp
    if resp is not None:
        return resp
    raise last_exc


# --------------------------------------------------------------------------- #
# NBB / CBSO — via le pool Tor
# --------------------------------------------------------------------------- #
def _nbb_parse_csv(text: str) -> dict:
    import pandas as pd
    from io import StringIO

    df = pd.read_csv(StringIO(text), header=None, skiprows=1)
    codes = {}
    for _, row in df.iterrows():
        key = str(row[0]).strip()
        try:
            codes[key] = float(row[1])
        except (ValueError, TypeError):
            codes[key] = row[1]
    return codes


def _filing_year(dep) -> str:
    """Année de dépôt : préfixe de la référence (ex. \"2026-00149705\"), sinon
    année de depositDate, sinon periodEndDateYear."""
    head = str(dep.get("reference") or "").split("-")[0]
    if len(head) == 4 and head.isdigit():
        return head
    dd = str(dep.get("depositDate") or "")
    return dd[:4] if dd[:4].isdigit() else str(dep.get("periodEndDateYear") or "")


def ingest_nbb(store: Store, enterprise: str) -> dict:
    cfg = store.cfg
    sess = _tor_session(cfg["tor_pool"])
    page = f"https://consult.cbso.nbb.be/consult-enterprise/{enterprise}"
    sess.headers["Referer"] = page
    _http_get(sess, page)  # amorce les cookies de session

    url = (f"{NBB_BASE}/rs-consult/published-deposits"
           f"?page=0&size=50&enterpriseNumber={enterprise}"
           f"&sort=periodEndDate,desc&sort=depositDate,desc")
    deposits = _http_get(sess, url).json().get("content", [])
    log.info("[%s] NBB : %d dépôts", enterprise, len(deposits))

    pdf_ok = csv_ok = kept = 0
    for dep in deposits:
        dep_id, year, ref = dep["id"], dep.get("periodEndDateYear"), dep.get("reference")
        if cfg.get("min_year"):
            # Mode « depuis N » : on filtre sur l'ANNÉE D'EXERCICE
            # (accountingYearEndDate ≈ periodEndDateYear), pas l'année de dépôt.
            ey = str(year or "")
            if not (ey.isdigit() and int(ey) >= cfg["min_year"]):
                continue
        elif cfg.get("years") and _filing_year(dep) not in cfg["years"]:
            continue
        kept += 1
        try:
            r = _http_get(sess, f"{NBB_BASE}/external/broker/public/deposits/pdf/{dep_id}",
                          read_to=120)
            if r.status_code == 200 and len(r.content) > 1000:
                store.put_pdf(enterprise, year, f"comptes_{ref}.pdf", r.content,
                              source="nbb", doc_type="comptes_annuels", url=r.url,
                              extra={"deposit_id": dep_id, "reference": ref,
                                     "deposit_date": dep.get("depositDate"),
                                     "migration": bool(dep.get("migration"))})
                pdf_ok += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] PDF %s abandonné : %s", enterprise, year, exc)
        time.sleep(cfg["delay"])

        if not dep.get("migration"):
            try:
                r = _http_get(
                    sess, f"{NBB_BASE}/external/broker/public/deposits/consult/csv/{dep_id}")
                if r.status_code == 200 and r.text.strip():
                    codes = _nbb_parse_csv(r.text)
                    # 1) parsé (codes PCMN) → Mongo comptes_annuels
                    store.put_nbb_csv(enterprise, year, ref, dep_id, codes, r.text)
                    # 2) fichier CSV brut → HDFS (à côté du PDF) + catalogue documents
                    store.put_pdf(enterprise, year, f"comptes_{ref}.csv", r.content,
                                  source="nbb", doc_type="comptes_annuels_csv", url=r.url,
                                  extra={"deposit_id": dep_id, "reference": ref})
                    csv_ok += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("[%s] CSV %s abandonné : %s", enterprise, year, exc)
            time.sleep(cfg["delay"])

    return {"enterprise": enterprise, "deposits": len(deposits),
            "filings": kept, "pdf": pdf_ok, "csv": csv_ok}


def run_nbb() -> list[dict]:
    logging.basicConfig(level=logging.INFO)
    store = Store(_cfg())
    results = []
    for num in ENTREPRISES.values():
        try:
            results.append(ingest_nbb(store, num))
        except Exception as exc:  # noqa: BLE001
            log.error("[%s] NBB échec global : %s", num, exc)
            results.append({"enterprise": num, "error": str(exc)})
    return results


# --------------------------------------------------------------------------- #
# notaire.be — Playwright (F5) + Tor dédié (IP stable)
# --------------------------------------------------------------------------- #
def _notaire_user_agent():
    return ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36")


class NotaireClient:
    """Tout passe par une PAGE Chromium réelle : le challenge F5 est résolu une
    fois, puis les appels API se font via fetch() DANS la page (page.evaluate).
    C'est indispensable : un client HTTP séparé (context.request) a une autre
    empreinte TLS et se fait rejeter par F5 malgré les cookies."""

    def __init__(self, socks: str):
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        # Tor optionnel pour notaire : F5 bloque souvent les exits Tor.
        # NOTAIRE_USE_TOR=false → navigateur en direct (IP de la machine).
        # F5 bloque les exits Tor → notaire en DIRECT par défaut.
        use_tor = (os.getenv("NOTAIRE_USE_TOR", "false").lower()
                   in {"1", "true", "yes", "on"}) and bool(socks)
        proxy = {"server": socks.replace("socks5h", "socks5")} if use_tor else None
        log.info("notaire.be : navigateur %s", "via Tor" if use_tor else "EN DIRECT")
        self.browser = self._pw.chromium.launch(
            headless=True, proxy=proxy, args=["--no-sandbox"])
        self.ctx = self.browser.new_context(
            locale="fr-BE", user_agent=_notaire_user_agent())
        self.page = self.ctx.new_page()
        self.page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        self._solve_f5()

    def _solve_f5(self):
        seed = (f"{NOTAIRE_BASE}/enterprise/{NOTAIRE_SEED}/statutes"
                f"?enterpriseNumber={NOTAIRE_SEED}&statuteStart=0&statuteCount=5")
        self.page.goto("https://statuts.notaire.be/", wait_until="load", timeout=60_000)
        self.page.wait_for_timeout(2000)
        self.page.goto(seed, wait_until="load", timeout=90_000)
        for _ in range(80):
            names = {c["name"] for c in self.ctx.cookies()}
            if "OClmoOot" in names and "Lyp1CWKh" in names:
                break
            self.page.wait_for_timeout(500)
        log.info("notaire.be : challenge F5 résolu")

    # --- Appels API via fetch() exécuté DANS la page (passe F5) ------------- #
    def _fetch_json(self, url):
        return self.page.evaluate(
            """async (url) => {
                const r = await fetch(url, {headers: {'Accept': 'application/json'}});
                return {status: r.status,
                        ct: r.headers.get('content-type') || '',
                        body: await r.text()};
            }""", url)

    def _fetch_pdf_b64(self, url):
        return self.page.evaluate(
            """async (url) => {
                const r = await fetch(url);
                if (r.status !== 200) return {status: r.status, ct: '', b64: null};
                const buf = await r.arrayBuffer();
                const bytes = new Uint8Array(buf);
                let bin = ''; const CH = 0x8000;
                for (let i = 0; i < bytes.length; i += CH)
                    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + CH));
                return {status: r.status,
                        ct: r.headers.get('content-type') || '',
                        b64: btoa(bin)};
            }""", url)

    def statutes(self, enterprise: str) -> list[dict]:
        all_st, offset, retried = [], 0, False
        while True:
            qs = urlencode({"deedDate": "", "offset": offset, "limit": 20})
            url = f"{NOTAIRE_BASE}/api/enterprises/{enterprise}/statutes?{qs}"
            res = self._fetch_json(url)
            if "application/json" not in res.get("ct", ""):
                if retried:
                    break
                retried = True
                self._solve_f5()
                continue
            data = json.loads(res["body"])
            batch = data.get("statutes", [])
            all_st.extend(batch)
            if not batch or len(all_st) >= data.get("totalItems", 0):
                break
            offset += 20
        return [s for s in all_st if s.get("documentStatus") == "DONE"]

    def pdf(self, enterprise, doc_id):
        url = (f"{NOTAIRE_BASE}/api/enterprises/{enterprise}"
               f"/statutes/non-certified/{doc_id}")
        res = self._fetch_pdf_b64(url)
        if res.get("status") != 200 or "pdf" not in res.get("ct", "") or not res.get("b64"):
            return None
        return base64.b64decode(res["b64"])

    def close(self):
        try:
            self.browser.close()
        finally:
            self._pw.stop()


def ingest_notaire(store: Store, client: NotaireClient, enterprise: str) -> dict:
    statutes = client.statutes(enterprise)
    log.info("[%s] notaire : %d statuts", enterprise, len(statutes))
    ok = 0
    for st in statutes:
        deed = st.get("deedDate") or ""
        year = deed[:4] if deed else "unknown"
        doc_id = st["documentId"]
        try:
            data = client.pdf(enterprise, doc_id)
            if data:
                store.put_pdf(
                    enterprise, year, f"statut_{doc_id}.pdf", data,
                    source="notaire", doc_type="statut",
                    url=f"{NOTAIRE_BASE}/api/enterprises/{enterprise}/statutes/non-certified/{doc_id}",
                    extra={"document_id": doc_id, "deed_date": st.get("deedDate"),
                           "title": st.get("documentTitle")})
                ok += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] statut %s abandonné : %s", enterprise, doc_id, exc)
        time.sleep(store.cfg["delay"])
    return {"enterprise": enterprise, "statuts": len(statutes), "pdf": ok}


def run_notaire() -> list[dict]:
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    client = NotaireClient(cfg["tor_notaire"])
    results = []
    try:
        for num in ENTREPRISES.values():
            try:
                results.append(ingest_notaire(store, client, num))
            except Exception as exc:  # noqa: BLE001
                log.error("[%s] notaire échec global : %s", num, exc)
                results.append({"enterprise": num, "error": str(exc)})
    finally:
        client.close()
    return results


# --------------------------------------------------------------------------- #
# Parcours de TOUTES les entreprises (lots + reprise par checkpoints)
# --------------------------------------------------------------------------- #
def _clean(num):
    return str(num).replace(".", "").replace(" ", "").strip()


def _batched(iterable, n):
    """Découpe un itérable en lots (listes) de taille n."""
    import itertools
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def iter_enterprise_numbers(store, only_active=True, limit=0):
    """Curseur Mongo sur kbo_enterprise → numéros (sans points). RAM bornée."""
    q = {"Status": "AC"} if only_active else {}
    cur = store.src["kbo_enterprise"].find(q, {"EnterpriseNumber": 1}).batch_size(1000)
    n = 0
    for doc in cur:
        num = _clean(doc.get("EnterpriseNumber"))
        if not num:
            continue
        yield num
        n += 1
        if limit and n >= limit:
            break


def run_nbb_all() -> dict:
    """NBB sur TOUTES les entreprises, avec reprise (skip celles déjà 'done')."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    bs, pause = cfg["batch_size"], cfg["batch_pause"]
    seen = done = errs = skip = 0
    enterprises = iter_enterprise_numbers(store, cfg["only_active"], cfg["max_enterprises"])
    for lot, chunk in enumerate(_batched(enterprises, bs), 1):
        for num in chunk:
            seen += 1
            if store.is_done(num, "nbb"):
                skip += 1
                continue
            try:
                res = ingest_nbb(store, num)
                store.mark(num, "nbb", "done", res)
                done += 1
            except Exception as exc:  # noqa: BLE001
                store.mark(num, "nbb", "error", {"msg": str(exc)})
                errs += 1
        log.info("Lot %d (%d entreprises) — vues=%d, traitées=%d, déjà=%d, erreurs=%d",
                 lot, len(chunk), seen, done, skip, errs)
        if pause:
            time.sleep(pause)
    summary = {"seen": seen, "processed": done, "skipped": skip, "errors": errs}
    log.info("NBB (toutes entreprises) terminé : %s", summary)
    return summary


def run_notaire_all() -> dict:
    """notaire sur TOUTES les entreprises, avec reprise. NotaireClient réutilisé."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    client = NotaireClient(cfg["tor_notaire"])
    bs, pause = cfg["batch_size"], cfg["batch_pause"]
    seen = done = errs = skip = 0
    enterprises = iter_enterprise_numbers(store, cfg["only_active"], cfg["max_enterprises"])
    try:
        for lot, chunk in enumerate(_batched(enterprises, bs), 1):
            for num in chunk:
                seen += 1
                if store.is_done(num, "notaire"):
                    skip += 1
                    continue
                try:
                    res = ingest_notaire(store, client, num)
                    store.mark(num, "notaire", "done", res)
                    done += 1
                except Exception as exc:  # noqa: BLE001
                    store.mark(num, "notaire", "error", {"msg": str(exc)})
                    errs += 1
            log.info("Lot %d (%d entreprises) — vues=%d, traitées=%d, déjà=%d, erreurs=%d",
                     lot, len(chunk), seen, done, skip, errs)
            if pause:
                time.sleep(pause)
    finally:
        client.close()
    summary = {"seen": seen, "processed": done, "skipped": skip, "errors": errs}
    log.info("notaire (toutes entreprises) terminé : %s", summary)
    return summary


# --------------------------------------------------------------------------- #
# NBB ciblé HÔTELLERIE — piloté par la StateDB `hotel_targets`, dépôts >= 2021
# --------------------------------------------------------------------------- #
def iter_hotel_targets(store, limit=0):
    """Numéros à scraper depuis hotel_targets (on saute ceux déjà 'done')."""
    coll = store.db[os.getenv("HOTEL_STATE", "hotel_targets")]
    cur = coll.find({"status": {"$ne": "done"}}, {"_id": 1}).batch_size(500)
    n = 0
    for doc in cur:
        yield str(doc["_id"])
        n += 1
        if limit and n >= limit:
            break


def run_nbb_hotels() -> dict:
    """Scrape les dépôts NBB des entreprises hôtelières (StateDB hotel_targets),
    depuis DOC_MIN_YEAR (défaut 2021). Reprise : `status` done/error dans la
    collection d'état. Lance d'abord build_hotel_targets.py pour peupler la liste."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    cfg["years"] = None                                    # désactive le filtre par set d'années
    cfg["min_year"] = int(os.getenv("DOC_MIN_YEAR", "2021"))  # on remonte jusqu'à 2021
    store = Store(cfg)
    state = store.db[os.getenv("HOTEL_STATE", "hotel_targets")]

    total = state.count_documents({})
    todo = state.count_documents({"status": {"$ne": "done"}})
    log.info("Hôtellerie : %d cibles, %d à traiter (min_year=%d)",
             total, todo, cfg["min_year"])

    seen = done = errs = 0
    for num in iter_hotel_targets(store, cfg["max_enterprises"]):
        seen += 1
        try:
            res = ingest_nbb(store, num)
            state.update_one({"_id": num},
                             {"$set": {"status": "done", "scraped_at": _now(),
                                       "filings_count": res.get("filings", 0), "result": res}})
            done += 1
        except Exception as exc:  # noqa: BLE001
            state.update_one({"_id": num},
                             {"$set": {"status": "error", "scraped_at": _now(), "error": str(exc)}})
            errs += 1
        if seen % 50 == 0:
            log.info("… %d vues, %d ok, %d erreurs", seen, done, errs)
        if cfg["batch_pause"] and seen % cfg["batch_size"] == 0:
            time.sleep(cfg["batch_pause"])

    summary = {"targets": total, "processed": done, "errors": errs}
    log.info("NBB hôtellerie terminé : %s", summary)
    return summary


# --------------------------------------------------------------------------- #
def verify() -> dict:
    store = Store(_cfg())
    counts = {
        "documents": store.documents.count_documents({}),
        "documents_nbb": store.documents.count_documents({"source": "nbb"}),
        "documents_notaire": store.documents.count_documents({"source": "notaire"}),
        "comptes_annuels": store.comptes.count_documents({}),
    }
    log.info("Vérification documents : %s", counts)
    return counts


if __name__ == "__main__":
    # Test rapide sur les 3 entreprises du notebook (NBB + notaire).
    print(json.dumps(run_nbb(), indent=2))
    print(json.dumps(run_notaire(), indent=2))
    print(json.dumps(verify(), indent=2))
