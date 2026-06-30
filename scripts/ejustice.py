"""
Scraper eJustice — publications au Moniteur belge (section 2.9 du notebook).

Source : https://www.ejustice.just.fgov.be/cgi_tsv/list.pl?language=fr&btw={num}&page=N
Recherche par numéro d'entreprise (paramètre 'btw'), avec pagination.

Pour chaque entreprise :
  - récupère toutes les pages de résultats ;
  - archive le HTML brut sur HDFS (/ejustice/{num}/p{page}.html) — rien perdu ;
  - parse les publications (date, NUMAC, type, lien) → Mongo kbo_db.ejustice.

Reprenable : checkpoint par entreprise (kbo_db.documents_checkpoints, clé 'ejustice').
Parser best-effort à affiner sur la sortie réelle (le HTML brut reste sur HDFS).
"""
from __future__ import annotations

import datetime as dt
import io
import logging
import os
import re
import time

import requests

log = logging.getLogger(__name__)

BASE = "https://www.ejustice.just.fgov.be/cgi_tsv/list.pl"
RATE_LIMIT_CODES = {403, 429, 503}
MAX_PAGES = int(os.getenv("EJUSTICE_MAX_PAGES", "50"))
_WS = re.compile(r"\s+")


def _cfg():
    return {
        "hdfs_url": os.getenv("HDFS_URL", "http://namenode:9870"),
        "hdfs_user": os.getenv("HDFS_USER", "root"),
        "mongo_uri": os.getenv(
            "MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin"),
        "db": os.getenv("INGESTION_DB", "kbo_db"),
        "src_db": os.getenv("KBO_SRC_DB", os.getenv("INGESTION_DB", "kbo_db")),
        "tor": os.getenv("TOR_SOCKS", "socks5h://haproxy:9050"),
        "use_tor": os.getenv("EJUSTICE_USE_TOR", "true").lower() in {"1", "true", "yes", "on"},
        "hdfs_root": os.getenv("EJUSTICE_HDFS_ROOT", "/ejustice"),
        "only_active": os.getenv("ONLY_ACTIVE", "true").lower() in {"1", "true", "yes", "on"},
        "max_enterprises": int(os.getenv("MAX_ENTERPRISES", "0")),
        "batch_size": int(os.getenv("DOC_BATCH_SIZE", "1000")),
        "batch_pause": float(os.getenv("DOC_BATCH_PAUSE", "0")),
        "delay": float(os.getenv("REQUEST_DELAY", "0.4")),
    }


def _now():
    return dt.datetime.now(dt.timezone.utc)


def _clean(num):
    return str(num).replace(".", "").replace(" ", "").strip()


def _txt(el):
    return _WS.sub(" ", el.get_text(" ", strip=True)) if el else ""


# --------------------------------------------------------------------------- #
def make_session(cfg) -> requests.Session:
    s = requests.Session()
    if cfg["use_tor"]:
        s.proxies = {"http": cfg["tor"], "https": cfg["tor"]}
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/149.0.0.0 Safari/537.36"),
        "Accept-Language": "fr-BE,fr;q=0.9",
        "Connection": "close",
    })
    return s


def fetch_list(sess, numero, page, attempts=6) -> str | None:
    url = f"{BASE}?language=fr&btw={_clean(numero)}&page={page}"
    last = None
    for i in range(attempts):
        try:
            r = sess.get(url, timeout=(20, 60))
        except requests.RequestException as exc:
            last = exc
            time.sleep(2)
            continue
        if r.status_code in RATE_LIMIT_CODES:
            time.sleep(2 + i)
            continue
        if r.status_code == 200:
            return r.text
        return None
    if last:
        log.warning("[%s] eJustice injoignable : %s", numero, last)
    return None


# --------------------------------------------------------------------------- #
def parse_publications(html: str) -> list[dict]:
    """Best-effort : extrait les publications (NUMAC, date, type, lien).

    Heuristique : chaque publication a un lien dont l'URL contient 'numac='.
    On récupère le NUMAC, le texte visible et l'URL. À affiner sur le HTML réel.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    pubs = []
    seen = set()
    # Chaque publication = un lien PDF /tsv_pdf/AAAA/MM/JJ/NUMAC.pdf
    for a in soup.find_all("a", href=re.compile(r"/tsv_pdf/\d{4}/\d{2}/\d{2}/")):
        href = a["href"]
        m = re.search(r"/tsv_pdf/(\d{4})/(\d{2})/(\d{2})/([^/.]+)\.pdf", href)
        if not m:
            continue
        y, mo, d, numac = m.groups()
        if numac in seen:
            continue
        seen.add(numac)
        bloc = a.find_parent(["li", "tr", "div", "p"]) or a
        titre = _txt(bloc)
        pubs.append({
            "numac": numac,
            "date": f"{y}-{mo}-{d}",
            "titre": titre[:200] or None,
            "pdf_url": href if href.startswith("http")
                       else f"https://www.ejustice.just.fgov.be{href}",
        })
    return pubs


def _last_page(html: str) -> int:
    """Détecte le numéro de dernière page (liens page=N)."""
    pages = [int(m) for m in re.findall(r"[?&]page=(\d+)", html)]
    return max(pages) if pages else 1


# --------------------------------------------------------------------------- #
class Store:
    def __init__(self, cfg):
        from hdfs import InsecureClient
        from pymongo import MongoClient

        self.cfg = cfg
        self.hdfs = InsecureClient(cfg["hdfs_url"], user=cfg["hdfs_user"])
        client = MongoClient(cfg["mongo_uri"])
        self.db = client[cfg["db"]]
        self.src = client[cfg["src_db"]]
        self.ejustice = self.db["ejustice"]
        self.checkpoints = self.db["documents_checkpoints"]

    def archive_html(self, numero, page, html):
        path = f"{self.cfg['hdfs_root']}/{_clean(numero)}/p{page}.html"
        self.hdfs.write(path, data=io.BytesIO(html.encode("utf-8")), overwrite=True)
        return path

    def save(self, numero, publications, pages):
        self.ejustice.replace_one(
            {"_id": _clean(numero)},
            {"_id": _clean(numero), "nb_publications": len(publications),
             "pages": pages, "publications": publications, "scraped_at": _now()},
            upsert=True)

    def is_done(self, numero):
        cp = self.checkpoints.find_one({"_id": _clean(numero)})
        return bool(cp and cp.get("ejustice") == "done")

    def mark(self, numero, status):
        self.checkpoints.update_one(
            {"_id": _clean(numero)}, {"$set": {"ejustice": status}}, upsert=True)


# --------------------------------------------------------------------------- #
def iter_enterprise_numbers(store, only_active=True, limit=0):
    q = {"Status": "AC"} if only_active else {}
    cur = store.src["kbo_enterprise"].find(q, {"EnterpriseNumber": 1}).batch_size(1000)
    n = 0
    for doc in cur:
        num = _clean(doc.get("EnterpriseNumber"))
        if num:
            yield num
            n += 1
            if limit and n >= limit:
                break


def ingest_one(store, sess, numero) -> int:
    html = fetch_list(sess, numero, 1)
    if html is None:
        return -1
    last = min(_last_page(html), MAX_PAGES)
    all_pubs = parse_publications(html)
    store.archive_html(numero, 1, html)
    for page in range(2, last + 1):
        h = fetch_list(sess, numero, page)
        if not h:
            break
        store.archive_html(numero, page, h)
        all_pubs.extend(parse_publications(h))
        time.sleep(store.cfg["delay"])
    # dédup par numac
    uniq = {p["numac"]: p for p in all_pubs}
    store.save(numero, list(uniq.values()), last)
    return len(uniq)


def run_ejustice(numeros=None) -> dict:
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    sess = make_session(cfg)
    log.info("eJustice : %s", "via Tor" if cfg["use_tor"] else "EN DIRECT")

    explicit = numeros is not None  # liste fournie = on force le re-scraping
    if numeros is None:
        numeros = iter_enterprise_numbers(store, cfg["only_active"], cfg["max_enterprises"])
    else:
        numeros = [_clean(n) for n in numeros]

    seen = done = errs = skip = 0
    for num in numeros:
        seen += 1
        if not explicit and store.is_done(num):
            skip += 1
            continue
        try:
            n = ingest_one(store, sess, num)
            store.mark(num, "done" if n >= 0 else "error")
            done += (1 if n >= 0 else 0)
            errs += (0 if n >= 0 else 1)
        except Exception as exc:  # noqa: BLE001
            store.mark(num, "error")
            errs += 1
            log.warning("[%s] eJustice échec : %s", num, exc)
        if seen % cfg["batch_size"] == 0:
            log.info("… %d vues (ok=%d, déjà=%d, ko=%d)", seen, done, skip, errs)
            if cfg["batch_pause"]:
                time.sleep(cfg["batch_pause"])
        time.sleep(cfg["delay"])

    summary = {"seen": seen, "scraped": done, "skipped": skip, "errors": errs}
    log.info("eJustice terminé : %s", summary)
    return summary


def verify() -> dict:
    cfg = _cfg()
    store = Store(cfg)
    return {"ejustice_docs": store.ejustice.count_documents({})}


if __name__ == "__main__":
    import json
    import sys
    nums = sys.argv[1:] or None
    print(json.dumps(run_ejustice(nums), indent=2, default=str))
    print(json.dumps(verify(), indent=2))
