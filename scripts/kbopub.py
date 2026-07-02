"""
Scraper de la fiche publique KBO (kbopub.economie.fgov.be) — sections 2.1 à 2.5,
2.8 et 2.10 du notebook (champs ABSENTS du dump CSV) :

  2.1 Généralités        : statut, situation juridique, date début, adresse siège…
  2.2 Juridique          : type d'entité, forme légale, n° entreprise
  2.3 Activités          : NACE TVA/ONSS (versions 2025/2008/2003)
  2.4 Dirigeants         : "Fonctions" (administrateurs, gérants…)
  2.5 Liens entre entités
  2.8 Établissements     : nombre d'UE (+ lien)
  2.10 Contact           : téléphone, fax, e-mail, site web
  + Qualités, Données financières (capital, AG, fin d'exercice), liens externes

Stratégie : la page est en HTML server-rendu (pas de JS). On l'archive BRUTE sur
HDFS (/kbopub/{num}.html) pour ne rien perdre, et on parse les champs dans Mongo
(kbo_db.kbopub). Tout passe par le pool Tor (rotation + retry sur limite).

Reprenable : checkpoint par entreprise dans kbo_db.documents_checkpoints (clé 'kbopub').
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

BASE = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
RATE_LIMIT_CODES = {403, 429, 503}


def _cfg():
    return {
        "hdfs_url": os.getenv("HDFS_URL", "http://namenode:9870"),
        "hdfs_user": os.getenv("HDFS_USER", "root"),
        "mongo_uri": os.getenv(
            "MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin"),
        "db": os.getenv("INGESTION_DB", "kbo_db"),
        "src_db": os.getenv("KBO_SRC_DB", os.getenv("INGESTION_DB", "kbo_db")),
        "tor": os.getenv("TOR_SOCKS", "socks5h://haproxy:9050"),
        "use_tor": os.getenv("KBOPUB_USE_TOR", "true").lower() in {"1", "true", "yes", "on"},
        "hdfs_root": os.getenv("KBOPUB_HDFS_ROOT", "/kbopub"),
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


# --------------------------------------------------------------------------- #
# Session HTTP (Tor optionnel)
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


def fetch_page(sess, numero, attempts=8) -> str | None:
    url = f"{BASE}?lang=fr&ondernemingsnummer={_clean(numero)}"
    last = None
    for i in range(attempts):
        try:
            r = sess.get(url, timeout=(20, 60))
        except requests.RequestException as exc:
            last = exc
            time.sleep(2)
            continue
        if r.status_code in RATE_LIMIT_CODES:
            log.warning("HTTP %s (limite) %s — rotation (%d/%d)",
                        r.status_code, numero, i + 1, attempts)
            time.sleep(2 + i)
            continue
        if r.status_code == 200:
            return r.text
        return None
    if last:
        log.warning("[%s] kbopub injoignable : %s", numero, last)
    return None


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
_WS = re.compile(r"\s+")


def _txt(el):
    return _WS.sub(" ", el.get_text(" ", strip=True)) if el else ""


def parse_kbopub(html: str) -> dict:
    """Parse la fiche en label→valeur (robuste) + sections en listes.

    La page est un grand tableau : chaque ligne a une cellule 'label' (classe QL)
    et une cellule 'valeur'. Les sections (Fonctions, Qualités, Activités…) sont
    des lignes d'en-tête. On reste tolérant : on capture aussi le texte brut.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    out = {
        "generalites": {}, "juridique": {}, "contact": {},
        "donnees_financieres": {},
        "fonctions": [], "qualites": [], "autorisations": [],
        "activites": [], "liens_entites": [], "liens_externes": {},
        "nb_etablissements": None, "ue_url": None,
    }

    # 1) Couples label/valeur — exhaustif.
    #    a) cellules de label (classe QL) → cellule suivante ;
    #    b) complément : TOUTES les lignes à >=2 cellules (label=1re, valeur=reste).
    #    On garde la valeur non vide la plus longue.
    kv = {}

    def _put(label, val):
        label = (label or "").rstrip(":").strip()
        val = (val or "").strip()
        if label and val and (label not in kv or len(val) > len(kv[label])):
            kv[label] = val

    for lab in soup.select("td.QL"):
        sib = lab.find_next_sibling("td")
        _put(_txt(lab), _txt(sib) if sib is not None else "")
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            _put(_txt(tds[0]), " ".join(_txt(t) for t in tds[1:]))

    def g(*labels):
        for l in labels:
            if l in kv and "Pas de données" not in kv[l]:
                return kv[l]
        return None

    # --- 2.1 Généralités / 2.2 Juridique ---
    out["generalites"] = {
        "numero": g("Numéro d'entreprise"),
        "statut": g("Statut"),
        "situation_juridique": g("Situation juridique"),
        "date_debut": g("Date de début"),
        "denomination": g("Dénomination", "Dénomination de la personne morale"),
        "adresse_siege": g("Adresse du siège", "Adresse"),
    }
    out["juridique"] = {
        "type_entite": g("Type d'entité"),
        "forme_legale": g("Forme légale"),
    }
    # --- 2.10 Contact ---
    out["contact"] = {
        "telephone": g("Numéro de téléphone"),
        "fax": g("Numéro de fax"),
        "email": g("E-mail"),
        "site_web": g("Adresse web"),
    }
    # --- Données financières ---
    out["donnees_financieres"] = {
        "capital": g("Capital"),
        "assemblee_generale": g("Assemblée générale"),
        "fin_exercice": g("Date de fin de l'année comptable"),
    }
    # --- 2.8 Établissements (nombre d'UE + lien) ---
    ue = g("Nombre d'unités d'établissement (UE)", "Nombre d'unités d'établissement")
    if ue:
        m = re.search(r"\d+", ue)
        out["nb_etablissements"] = int(m.group()) if m else None
    link_ue = soup.find("a", href=re.compile(r"toonvestigingps"))
    if link_ue:
        out["ue_url"] = link_ue.get("href")

    # --- 2.4 Dirigeants : on parse la SECTION 'Fonctions' en entier (tous
    # libellés, publics compris : Bourgmestre, Secrétaire…). Les titres de
    # section sont des cellules de classe 'I' ; une fonction = ligne à
    # [fonction, nom, 'Depuis le …']. Les qualités/activités qui suivent sont
    # en une seule cellule → écartées par le garde >= 2 cellules + nom valide.
    current = None
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        fclass = cells[0].get("class") or []
        if "I" in fclass:                      # en-tête de section
            current = _txt(cells[0])
            continue
        if current in ("Fonctions", "Functies") and len(cells) >= 2:
            fonction = _txt(cells[0])
            nom = _txt(cells[1])
            depuis = _txt(cells[2]) if len(cells) > 2 else None
            if fonction and nom and not nom.startswith("Depuis") and "Depuis" not in fonction:
                out["fonctions"].append({"fonction": fonction, "nom": nom, "depuis": depuis})
        elif current in ("Liens entre entités", "Onderlinge betrekkingen"):
            t = " ".join(_txt(c) for c in cells).strip()
            if t and "Pas de données" not in t and "Geen gegevens" not in t:
                mb = re.search(r"(\d{4}\.\d{3}\.\d{3})", t)
                mn = re.search(r"\(([^)]+)\)", t)
                out["liens_entites"].append({
                    "bce": mb.group(1).replace(".", "") if mb else None,
                    "nom": mn.group(1).strip() if mn else None,
                    "texte": t})

    # --- 2.3 Activités (NACE TVA/ONSS) : liens naceToelichting ---
    for a in soup.find_all("a", href=re.compile(r"naceToelichting")):
        href = a.get("href", "")
        mver = re.search(r"nace\.version=(\d+)", href)
        mcode = re.search(r"nace\.code=(\d+)", href)
        # libellé = texte après le lien dans la même ligne
        parent = a.find_parent("td") or a.parent
        ligne = _txt(parent.find_parent("tr")) if parent and parent.find_parent("tr") else _txt(parent)
        out["activites"].append({
            "nace_version": mver.group(1) if mver else None,
            "nace_code": mcode.group(1) if mcode else _txt(a),
            "ligne": ligne,
        })

    # --- Liens externes (ejustice / NBB / notaire) ---
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "ejustice" in href:
            out["liens_externes"]["ejustice"] = href
        elif "consult.cbso.nbb.be" in href:
            out["liens_externes"]["nbb"] = href
        elif "statuts.notaire.be" in href:
            out["liens_externes"]["notaire"] = href

    return out


# --------------------------------------------------------------------------- #
# Stockage
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
        self.kbopub = self.db["kbopub"]
        self.kbopub.create_index("_id")
        self.checkpoints = self.db["documents_checkpoints"]

    def archive_html(self, numero, html):
        path = f"{self.cfg['hdfs_root']}/{_clean(numero)}.html"
        self.hdfs.write(path, data=io.BytesIO(html.encode("utf-8")), overwrite=True)
        return path

    def save(self, numero, parsed, hdfs_path):
        self.kbopub.replace_one(
            {"_id": _clean(numero)},
            {"_id": _clean(numero), **parsed,
             "hdfs_html": hdfs_path, "scraped_at": _now()},
            upsert=True)

    def is_done(self, numero):
        cp = self.checkpoints.find_one({"_id": _clean(numero)})
        return bool(cp and cp.get("kbopub") == "done")

    def mark(self, numero, status):
        self.checkpoints.update_one(
            {"_id": _clean(numero)}, {"$set": {"kbopub": status}}, upsert=True)


# --------------------------------------------------------------------------- #
# Orchestration
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


def ingest_one(store, sess, numero) -> bool:
    html = fetch_page(sess, numero)
    if not html:
        return False
    parsed = parse_kbopub(html)
    path = store.archive_html(numero, html)
    store.save(numero, parsed, path)
    return True


def run_kbopub(numeros=None) -> dict:
    """Parallélisé (SCRAPE_WORKERS) + shardable (SHARD_INDEX/COUNT). Reprise via
    checkpoint 'kbopub'. Chaque thread a sa propre session Tor + son propre Store."""
    logging.basicConfig(level=logging.INFO)
    import threading

    import parallel
    cfg = _cfg()
    store = Store(cfg)
    workers = parallel.get_workers()
    log.info("kbopub : %s (workers=%d)", "via Tor" if cfg["use_tor"] else "EN DIRECT", workers)

    if numeros is None:
        numeros = [n for n in iter_enterprise_numbers(store, cfg["only_active"], cfg["max_enterprises"])
                   if parallel.shard_ok(n)]
    else:
        numeros = [_clean(n) for n in numeros if parallel.shard_ok(_clean(n))]

    c = {"seen": 0, "done": 0, "errs": 0, "skip": 0}
    lock = threading.Lock()

    def handle(ctx, num):
        st, sess = ctx
        if st.is_done(num):
            with lock:
                c["seen"] += 1
                c["skip"] += 1
            return
        try:
            ok = bool(ingest_one(st, sess, num))
            st.mark(num, "done" if ok else "empty")
        except Exception as exc:  # noqa: BLE001
            st.mark(num, "error")
            ok = False
            log.warning("[%s] kbopub échec : %s", num, exc)
        with lock:
            c["seen"] += 1
            c["done"] += int(ok)
            c["errs"] += int(not ok)
            if c["seen"] % cfg["batch_size"] == 0:
                log.info("… %d vues (ok=%d, déjà=%d, ko=%d)", c["seen"], c["done"], c["skip"], c["errs"])
        time.sleep(cfg["delay"])

    parallel.run_pool(numeros, lambda: (Store(cfg), make_session(cfg)), handle, workers)
    summary = {"seen": c["seen"], "scraped": c["done"], "skipped": c["skip"], "errors": c["errs"]}
    log.info("kbopub terminé : %s", summary)
    return summary


def run_kbopub_hotels(limit=0) -> dict:
    """kbopub ciblé sur les entreprises de hotel_targets (dirigeants, contacts,
    capital…). Reprise via checkpoint 'kbopub' (les déjà 'done' sont sautées)."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    state = store.db[os.getenv("HOTEL_STATE", "hotel_targets")]
    nums = [str(d["_id"]) for d in state.find({}, {"_id": 1})]
    limit = limit or cfg["max_enterprises"]
    if limit:
        nums = nums[:limit]
    log.info("kbopub hôtellerie : %d cibles", len(nums))
    return run_kbopub(nums)


def scrape_one(numero, force=False) -> list:
    """Scrape À LA DEMANDE la fiche kbopub d'UNE entreprise → dirigeants.
    Met en cache (checkpoint 'kbopub') : si déjà fait et pas force, on relit
    simplement la base. Renvoie la liste `fonctions` (dirigeants/représentants)."""
    logging.basicConfig(level=logging.INFO)
    cfg = _cfg()
    store = Store(cfg)
    num = _clean(numero)
    if not force and store.is_done(num):
        doc = store.kbopub.find_one({"_id": num})
        return (doc or {}).get("fonctions", [])
    sess = make_session(cfg)
    try:
        ok = bool(ingest_one(store, sess, num))
        store.mark(num, "done" if ok else "empty")
    except Exception as exc:  # noqa: BLE001
        store.mark(num, "error")
        log.warning("[%s] kbopub on-demand échec : %s", num, exc)
    doc = store.kbopub.find_one({"_id": num})
    return (doc or {}).get("fonctions", [])


def verify() -> dict:
    cfg = _cfg()
    store = Store(cfg)
    return {"kbopub_docs": store.kbopub.count_documents({})}


if __name__ == "__main__":
    import json
    import sys
    nums = sys.argv[1:] or None
    print(json.dumps(run_kbopub(nums), indent=2, default=str))
    print(json.dumps(verify(), indent=2))
