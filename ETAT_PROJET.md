# État du projet — repère

Ce fichier dit, à tout moment, **où on en est** et **dans quel ordre lancer les étapes**.

## Idée générale

Le notebook `BCE-Copy1.ipynb` est un **prototype** pour 3 entreprises sur une seule
machine. Le projet, lui, **industrialise** ce prototype pour **toutes** les
entreprises du registre KBO, avec une vraie stack Big Data.

## Architecture (où va quoi)

- **HDFS** → les fichiers **PDF** : `/documents/{NumEntreprise}/{Année}/`
- **MongoDB — une seule base : `kbo_db`** :
  - `kbo_enterprise`, `kbo_activity`, `kbo_address`, … → les 8 CSV KBO **bruts**
  - `enterprises_rich` → la **fiche consolidée** (jointures + codes traduits)
  - `comptes_annuels` → les **CSV financiers** NBB (bruts)
  - `documents` → le **catalogue** des PDF (source, année, chemin HDFS, sha256)
  - `documents_checkpoints` → la **reprise** du scraping
- **Tor** (pool + HAProxy, et un Tor dédié pour notaire) → pour le scraping
- **Airflow** orchestre, **Spark** pour les jointures lourdes

## Correspondance notebook → projet

| Notebook | Composant | État |
|---|---|---|
| §1 Entité KBO (CSV + traduction) | `kbo_*` bruts + `enterprises_rich` | ✅ fait |
| §2.7 Comptes annuels NBB | PDF→HDFS, CSV→`comptes_annuels` | ✅ fait |
| §2.6 Statuts notaire | PDF→HDFS via Playwright (en DIRECT, F5 bloque Tor) | ✅ fait |
| §2.1–2.5, 2.8, 2.10 (scraping kbopub) | HTML→HDFS + champs→`kbo_db.kbopub` | ✅ fait |
| §2.9 Publications eJustice | HTML→HDFS + publications→`kbo_db.ejustice` | ✅ fait |
| §3 KPI financiers | données brutes stockées, pas de calcul | ❌ étape suivante |

## Le pipeline, dans l'ordre

Tout tourne dans le conteneur `airflow-scheduler`. Base unique : `kbo_db`.

### 0. Démarrer + vérifier l'infra
```
docker compose up -d --build
docker compose run --rm airflow-scheduler python /opt/airflow/scripts/smoke_test.py
```

### 1. Charger les CSV KBO bruts → kbo_db.kbo_*
```
docker compose exec airflow-scheduler python /opt/airflow/scripts/ingestion_kbo.py
```

### 2. Consolider (jointures + traduction) → kbo_db.enterprises_rich
```
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler python -u /opt/airflow/scripts/import_kbo_denormalized.py
```
(Approche SQLite, sans OOM. Le job Spark équivalent a été supprimé pour éviter
les doublons.)

### 3. Récupérer les documents (PDF→HDFS, CSV→Mongo) — années 2025/2026
```
# test sur 3 entreprises
docker compose exec airflow-scheduler python /opt/airflow/scripts/run_documents.py --scope sample --source nbb
# toutes les entreprises (reprenable : relancer la même commande)
docker compose exec airflow-scheduler python /opt/airflow/scripts/run_documents.py --scope all --source nbb
```
Filtre d'années réglable : `-e DOC_YEARS=2026` ou `-e DOC_YEARS=2024,2025,2026`.

### 4. Valider l'avancée (rapport complet)
```
docker compose exec airflow-scheduler python /opt/airflow/scripts/verify_progress.py
```

## Scripts (rôle de chacun)

| Script | Rôle |
|---|---|
| `scripts/ingestion_kbo.py` | CSV KBO bruts → `kbo_db.kbo_*` (Spark) |
| `scripts/import_kbo_denormalized.py` | jointures + traduction → `kbo_db.enterprises_rich` (SQLite) |
| `scripts/ingestion_documents.py` | briques NBB + notaire (téléchargement, HDFS, catalogue) |
| `scripts/run_documents.py` | lanceur documents (`--scope`, `--source`) |
| `scripts/verify_progress.py` | rapport de validation (Mongo + HDFS) |
| `scripts/smoke_test.py` / `checks.py` | tests d'infra (HDFS, Mongo, Tor, Spark) |

## DAGs Airflow (UI : http://localhost:8080)

Tous en pause à la création — active-les dans l'UI pour qu'ils tournent.

| DAG | Rôle | Planif |
|---|---|---|
| `pipeline_kbo` | **maître** : enchaîne load → consolidation → documents → verify | `@monthly` |
| `ingestion_kbo_load` | CSV KBO bruts → `kbo_*` | manuel |
| `import_kbo_denormalized` | consolidation → `enterprises_rich` (reprenable) | manuel |
| `ingestion_documents` | comptes annuels NBB, toutes entreprises (reprenable) | manuel |
| `ingestion_notaire` | statuts notaire (isolé car fragile) | manuel |
| `smoke_test` | tests d'infra | manuel |

Pour automatiser tout le pipeline d'un coup : active **`pipeline_kbo`**.
Pour lancer/planifier en CLI :
```
docker compose exec airflow-scheduler airflow dags unpause pipeline_kbo
docker compose exec airflow-scheduler airflow dags trigger pipeline_kbo
```

## Reste à faire

1. *(optionnel, pour coller au notebook)* **scraping kbopub** : dirigeants,
   liens entre entités, capital social, n° TVA… (champs absents du dump CSV).
3. *(optionnel)* **publications eJustice**.
4. **Couche suivante — transformation** : calcul des **KPI financiers**
   (chiffre d'affaires, marge, EBITDA, EBIT, résultat net, croissance,
   autonomie financière) à partir de `comptes_annuels`.

## Note de migration

La base a été unifiée vers `kbo_db`. Si d'anciennes données traînent dans les
bases `bronze` ou `ingestion`, elles sont orphelines — tu peux les supprimer :
```
docker compose exec mongo mongosh -u kbo -p kbo_secret --authenticationDatabase admin --eval "db.getSiblingDB('bronze').dropDatabase(); db.getSiblingDB('ingestion').dropDatabase()"
```
Comme la base par défaut est maintenant `kbo_db`, relance l'étape 1
(`ingestion_kbo.py`) pour avoir `kbo_enterprise` dans `kbo_db` (nécessaire au
parcours « toutes entreprises » des documents).
