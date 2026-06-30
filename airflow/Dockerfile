FROM apache/airflow:2.9.3-python3.11

# --- Dépendances système : JDK (PySpark) + libs Chromium (Playwright/notaire) ---
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jdk procps curl \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
        libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
        libxrandr2 libgbm1 libasound2 libpango-1.0-0 libcairo2 libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Cache Ivy partagé pour le connecteur Spark-Mongo (rempli au build ci-dessous)
RUN mkdir -p /opt/spark-ivy && chown -R airflow: /opt/spark-ivy

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Navigateur Chromium pour Playwright (challenge F5 notaire.be)
RUN python -m playwright install chromium

# Pré-résolution du connecteur mongo-spark dans le cache Ivy (aucun
# téléchargement Maven au runtime).
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').appName('warm-ivy') \
.config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:10.3.0') \
.config('spark.jars.ivy','/opt/spark-ivy').getOrCreate().stop()"
