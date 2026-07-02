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
        self.documents = self.db["documents"]
        self.checkpoints = self.db["documents_checkpoints"]

    def archive_html(self, numero, page, html):
        path = f"{self.cfg['hdfs_root']}/{_clean(numero)}/p{page}.html"
        self.hdfs.write(path, data=io.BytesIO(html.encode("utf-8")), overwrite=True)
        return path

    def put_publication_pdf(self, numero, pub, data):
        """PDF d'une publication du Moniteur → HDFS + catalogue `documents`."""
        num = _clean(numero)
        numac = pub.get("numac")
        path = f"{self.cfg['hdfs_root']}/{num}/pdf/{numac}.pdf"
        self.hdfs.write(path, data=io.BytesIO(data), overwrite=True)
        self.documents.replace_one(
            {"_id": f"ejustice:{num}:{numac}"},
            {"_id": f"ejustice:{num}:{numac}", "source": "ejustice", "type": "publication",
             "enterprise": num, "numac": numac, "year": (pub.get("date") or "")[:4],
             "title": pub.get("titre"), "filename": f"{numac}.pdf", "hdfs_path": path,
             "date": pub.get("date"), "downloaded_at": _now()},
            upsert=True)
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
    """Parallélisé (SCRAPE_WORKERS) + shardable (SHARD_INDEX/COUNT). Reprise via
    checkpoint 'ejustice'. Chaque thread a sa propre session Tor + son Store."""
    logging.basicConfig(level=logging.INFO)
    import threading

    import parallel
    cfg = _cfg()
    store = Store(cfg)
    workers = parallel.get_workers()
    log.info("eJustice : %s (workers=%d)", "via Tor" if cfg["use_tor"] else "EN DIRECT", workers)

    explicit = numeros is not None  # liste fournie = on force le re-scraping
    if numeros is None:
        numeros = [n for n in iter_enterprise_numbers(store, cfg["only_active"], cfg["max_enterprises"])
                   if parallel.shard_ok(n)]
    else:
        numeros = [_clean(n) for n in numeros if parallel.shard_ok(_clean(n))]

    c = {"seen": 0, "done": 0, "errs": 0, "skip": 0}
    lock = threading.Lock()

    def handle(ctx, num):
        st, sess = ctx
        if not explicit and st.is_done(num):
            with lock:
                c["seen"] += 1
                c["skip"] += 1
            return
        try:
            n = ingest_one(st, sess, num)
            st.mark(num, "done" if n >= 0 else "error")
            ok = n >= 0
        except Exception as exc:  # noqa: BLE001
            st.mark(num, "error")
            ok = False
            log.warning("[%s] eJustice échec : %s", num, exc)
        with lock:
            c["seen"] += 1
            c["done"] += int(ok)
            c["errs"] += int(not ok)
            if c["seen"] % cfg["batch_size"] == 0:
                log.info("… %d vues (ok=%d, déjà=%d, ko=%d)", c["seen"], c["done"], c["skip"], c["errs"])
        time.sleep(cfg["delay"])

    parallel.run_pool(numeros, lambda: (Store(cfg), make_session(cfg)), handle, workers)
    summary = {"seen": c["seen"], "scraped": c["done"], "skipped": c["skip"], "errors": c["errs"]}
    log.info("eJustice terminé : %s", summary)
    return summary


def scrape_one(numero, force=False) -> list:
    """eJustice À LA DEMANDE pour UNE entreprise : récupère les publications du
    Moniteur, télécharge leurs PDF sur HDFS (+ catalogue `documents`) et renvoie
    la liste. Mise en cache via checkpoint 'ejustice'."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    num = _clean(numero)
    if not force and store.is_done(num):
        doc = store.ejustice.find_one({"_id": num})
        return (doc or {}).get("publications", [])
    sess = make_session(cfg)
    html = fetch_list(sess, num, 1)
    if html is None:
        store.mark(num, "error")
        return []
    last = min(_last_page(html), MAX_PAGES)
    all_pubs = parse_publications(html)
    store.archive_html(num, 1, html)
    for page in range(2, last + 1):
        h = fetch_list(sess, num, page)
        if not h:
            break
        store.archive_html(num, page, h)
        all_pubs.extend(parse_publications(h))
        time.sleep(cfg["delay"])
    pubs = list({p["numac"]: p for p in all_pubs}.values())
    for p in pubs:
        try:
            r = sess.get(p["pdf_url"], timeout=(20, 60))
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                p["hdfs_path"] = store.put_publication_pdf(num, p, r.content)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] PDF eJustice %s abandonné : %s", num, p.get("numac"), exc)
        time.sleep(cfg["delay"])
    store.save(num, pubs, last)
    store.mark(num, "done")
    return pubs


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
