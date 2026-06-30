"""
Fonctions de smoke test — vérifient que chaque brique répond.

Utilisées à la fois par le script standalone (scripts/smoke_test.py) et par le
DAG Airflow (dags/smoke_test.py). Chaque fonction lève une exception si la
brique est KO, et renvoie une courte chaîne de diagnostic si OK.
"""
from __future__ import annotations

import os
import time
import uuid


# --------------------------------------------------------------------------- #
def check_hdfs() -> str:
    """Écrit puis relit un petit fichier sur HDFS via WebHDFS."""
    from hdfs import InsecureClient

    url = os.getenv("HDFS_URL", "http://namenode:9870")
    user = os.getenv("HDFS_USER", "root")
    client = InsecureClient(url, user=user)

    path = f"/smoke/test_{uuid.uuid4().hex}.txt"
    payload = "hdfs ok"
    client.write(path, data=payload.encode(), overwrite=True)
    with client.read(path) as r:
        content = r.read().decode()
    client.delete(path)
    assert content == payload, "contenu HDFS relu incohérent"
    return f"HDFS OK ({url}) — écriture/lecture/suppression réussies"


# --------------------------------------------------------------------------- #
def check_mongo() -> str:
    """Insère puis relit un document dans Mongo."""
    from pymongo import MongoClient

    uri = os.getenv("MONGO_URI", "mongodb://kbo:kbo_secret@mongo:27017/?authSource=admin")
    client = MongoClient(uri, serverSelectionTimeoutMS=10000)
    coll = client["smoke"]["test"]
    doc_id = coll.insert_one({"ping": "pong", "ts": time.time()}).inserted_id
    found = coll.find_one({"_id": doc_id})
    coll.delete_one({"_id": doc_id})
    assert found and found["ping"] == "pong", "document Mongo introuvable"
    return f"Mongo OK ({uri.split('@')[-1]}) — insert/find/delete réussis"


# --------------------------------------------------------------------------- #
def check_tor() -> str:
    """Vérifie le pool Tor via HAProxy et mesure la diversité d'IP.

    Plusieurs requêtes successives passent par HAProxy → round-robin sur les
    instances Tor → on s'attend à voir plusieurs IP de sortie distinctes.
    Les instances mettent 30-90 s à bootstrapper : on patiente avant d'échouer.
    """
    import requests

    socks = os.getenv("TOR_SOCKS", "socks5h://haproxy:9050")
    proxies = {"http": socks, "https": socks}
    # Connection: close → nouvelle connexion à chaque appel = nouveau backend
    headers = {"Connection": "close"}

    def exit_ip() -> str:
        return requests.get(
            "https://api.ipify.org", proxies=proxies, headers=headers, timeout=30
        ).text.strip()

    ips: list[str] = []
    last_err = None
    deadline = time.time() + 150  # ~2,5 min pour le bootstrap du pool
    while time.time() < deadline and len(ips) < 8:
        try:
            ips.append(exit_ip())
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(5)
    if not ips:
        raise RuntimeError(f"Pool Tor injoignable via HAProxy : {last_err}")

    distinct = sorted(set(ips))
    return (
        f"Tor pool OK — {len(ips)} requêtes, {len(distinct)} IP distinctes : "
        f"{distinct}"
    )


# --------------------------------------------------------------------------- #
def check_spark() -> str:
    """Crée une SparkSession locale et exécute un petit job."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("smoke-test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame([(i, i * i) for i in range(1000)], ["n", "carre"])
        total = df.count()
        somme = df.groupBy().sum("carre").collect()[0][0]
        version = spark.version
    finally:
        spark.stop()
    assert total == 1000, "count Spark inattendu"
    return f"Spark OK (v{version}) — {total} lignes, somme des carrés={somme}"


CHECKS = {
    "hdfs": check_hdfs,
    "mongo": check_mongo,
    "tor": check_tor,
    "spark": check_spark,
}
