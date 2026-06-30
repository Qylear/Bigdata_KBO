# Stack Big Data — infrastructure

Premier jet : monter l'infrastructure et vérifier que tout démarre et
communique. Pas encore de logique métier.

| Brique     | Rôle                                   | Accès                         |
|------------|----------------------------------------|-------------------------------|
| **HDFS**   | Stockage des fichiers PDF              | UI http://localhost:9870      |
| **MongoDB**| Stockage des CSV                       | `mongodb://localhost:27017`   |
| **Tor**    | Rotation d'IP (scraping)               | SOCKS5 `localhost:9050`       |
| **Airflow**| Orchestration (LocalExecutor)         | UI http://localhost:8080      |
| **Spark**  | Traitement (PySpark en `local[*]`)    | dans le worker Airflow        |
| Postgres   | Métadonnées Airflow (interne)         | —                             |

Spark n'est pas un cluster séparé : PySpark s'exécute en local dans le conteneur
Airflow. Postgres sert uniquement de base de métadonnées à Airflow.

## Démarrage

```bash
cp .env.example .env
docker compose up -d --build
```

Premier lancement : compter quelques minutes (build des images Airflow/Tor,
téléchargement de Chromium non requis ici, initialisation HDFS).

Vérifier l'état :

```bash
docker compose ps
```

## Smoke tests

Deux façons de vérifier que les 5 briques répondent.

**1. En ligne de commande** (le plus rapide) :

```bash
docker compose run --rm airflow-scheduler python /opt/airflow/scripts/smoke_test.py
```

Sortie attendue :

```
[OK]   hdfs   — HDFS OK ... écriture/lecture/suppression réussies
[OK]   mongo  — Mongo OK ... insert/find/delete réussis
[OK]   tor    — Tor OK — IP avant=x.x.x.x / après NEWNYM=y.y.y.y
[OK]   spark  — Spark OK (v3.5.1) — 1000 lignes, somme des carrés=...
Toutes les briques répondent.
```

**2. Depuis Airflow** : ouvrir http://localhost:8080 (admin/admin par défaut),
activer puis déclencher le DAG **`smoke_test`**. Une tâche par brique
(`hdfs`, `mongo`, `tor`, `spark`) — tout en vert = stack opérationnelle.

## Structure

```
docker-compose.yml      les 6 services + volumes
hadoop.env              config HDFS (WebHDFS activé)
.env.example            secrets / identifiants (copier en .env)
tor/                    image Tor (SOCKS + contrôle NEWNYM)
airflow/                image Airflow + JDK + PySpark + clients
dags/smoke_test.py      DAG de vérification
scripts/checks.py       fonctions de test (HDFS, Mongo, Tor, Spark)
scripts/smoke_test.py   runner standalone
```

## Détails utiles

- **HDFS** : WebHDFS exposé sur `namenode:9870`, réplication = 1 (un seul
  datanode). Client Python : `hdfs.InsecureClient`.
- **Tor** : port de contrôle `9051` protégé par mot de passe (`.env`). La
  rotation se fait par signal `NEWNYM` via `stem`.
- **Airflow** : `LocalExecutor`, identifiants admin définis dans `.env`. Les
  DAGs sont en pause à la création — pensez à activer `smoke_test`.
- **Connexions** : les services se joignent par leur nom (`namenode`, `mongo`,
  `tor`) sur le réseau Docker interne ; les ports `localhost` ci-dessus sont
  pour l'accès depuis l'hôte.

## Arrêt

```bash
docker compose down          # arrête tout
docker compose down -v       # + supprime les données (HDFS, Mongo, Postgres)
```
