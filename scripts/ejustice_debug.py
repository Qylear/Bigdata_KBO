"""Diagnostic : lit le HTML eJustice archivé sur HDFS et montre la structure."""
import os
import re
import sys

from bs4 import BeautifulSoup
from hdfs import InsecureClient

num = sys.argv[1] if len(sys.argv) > 1 else "0203430576"
page = sys.argv[2] if len(sys.argv) > 2 else "1"

c = InsecureClient(os.getenv("HDFS_URL", "http://namenode:9870"), user="root")
with c.read(f"/ejustice/{num}/p{page}.html") as r:
    html = r.read().decode("utf-8", "ignore")

soup = BeautifulSoup(html, "lxml")
links = [a.get("href", "") for a in soup.find_all("a", href=True)]
print(f"Taille HTML: {len(html)} | nb liens: {len(links)}\n")

print("=== Liens candidats (article/numac/pdf/tsv/caller/std) ===")
seen = set()
for h in links:
    if any(k in h.lower() for k in ("article", "numac", "pdf", "tsv", "caller", "std", "image")):
        if h not in seen:
            seen.add(h)
            print(" ", h)
        if len(seen) >= 15:
            break

print("\n=== Échantillon de TOUS les liens (15 premiers) ===")
for h in links[:15]:
    print(" ", h)

# Cherche des dates dans le texte pour localiser les blocs publications
print("\n=== Lignes contenant une date (8 premières) ===")
txt = soup.get_text("\n")
dates = [l.strip() for l in txt.splitlines()
         if re.search(r"\d{2}[-/]\d{2}[-/]\d{4}|\d{4}-\d{2}-\d{2}", l) and l.strip()]
for d in dates[:8]:
    print(" ", d[:160])
