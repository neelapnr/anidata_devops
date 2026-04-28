# =============================================================================
# Image Docker custom AniData Airflow
# -----------------------------------------------------------------------------
# Étend l'image officielle Apache Airflow 2.x avec :
#   - le code du scraper (package anidata_scraper)
#   - les DAGs (airflow/dags/)
#   - les dépendances Python supplémentaires (elasticsearch, pandas, ...)
#
# Cette image sera buildée et publiée automatiquement sur GHCR
# par le workflow .github/workflows/ci.yml à chaque merge sur main.
# =============================================================================

# Image de base officielle Airflow, version pinnée pour la reproductibilité.
# On garde la même version que dans le docker-compose.yml.
FROM apache/airflow:2.8.1-python3.11

# Métadonnées (visibles sur la page GHCR de l'image)
LABEL org.opencontainers.image.title="AniData Airflow"
LABEL org.opencontainers.image.description="Airflow custom AniData Lab — scraper + DAGs"
LABEL org.opencontainers.image.source="https://github.com/neelapnr/anidata_devops"

# IMPORTANT : on reste en utilisateur airflow (jamais root pour pip)
USER airflow

# --- Dépendances Python supplémentaires ---------------------------------------
# On copie d'abord uniquement le requirements pour profiter du cache Docker :
# tant que ce fichier ne change pas, cette couche n'est pas reconstruite.
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# --- Installation du scraper en tant que package Python -----------------------
# On copie le pyproject.toml et le code du scraper, puis on l'installe.
# Comme ça, depuis n'importe quel DAG on peut faire :
#   from anidata_scraper import scrape_to_file
COPY --chown=airflow:root anidata-scraper/pyproject.toml /opt/airflow/scraper/pyproject.toml
COPY --chown=airflow:root anidata-scraper/anidata_scraper/ /opt/airflow/scraper/anidata_scraper/
RUN pip install --no-cache-dir /opt/airflow/scraper/

# --- Copie des DAGs -----------------------------------------------------------
# Airflow scanne automatiquement /opt/airflow/dags pour découvrir les DAGs.
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/

# --- Dossier de données -------------------------------------------------------
# Création du dossier où le scraper écrira ses fichiers JSON.
# Sera monté en volume dans docker-compose pour la persistance.
RUN mkdir -p /opt/airflow/data/raw

# Vérification finale : le package s'importe correctement
RUN python -c "import anidata_scraper; print('AniData Scraper OK')"
