"""Dump la structure HTML de la section 'Fonctions' d'une fiche kbopub archivée."""
import os

from bs4 import BeautifulSoup
from hdfs import InsecureClient

NUM = os.getenv("NUM", "0207491215")
c = InsecureClient(os.getenv("HDFS_URL", "http://namenode:9870"), user="root")
with c.read(f"/kbopub/{NUM}.html") as r:
    html = r.read().decode("utf-8", "ignore")

soup = BeautifulSoup(html, "lxml")
print(f"=== Lignes du tableau contenant Fonctions/Bourgmestre/Depuis (num={NUM}) ===")
keys = ["Fonctions", "Functies", "Bourgmestre", "Burgemeester", "Secrétaire",
        "Secretaris", "Rosseel", "Vanhooren", "Depuis le", "Sinds"]
for tr in soup.find_all("tr"):
    t = tr.get_text(" ", strip=True)
    if any(k in t for k in keys):
        cells = [x.get_text(" ", strip=True) for x in tr.find_all(["td", "th"])]
        classes = tr.get("class")
        first = tr.find(["td", "th"])
        fclass = first.get("class") if first else None
        print(f"\nROW class={classes} first_cell_class={fclass}")
        print("  cells:", cells)
