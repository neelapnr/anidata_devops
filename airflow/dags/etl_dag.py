"""
AniData Lab - DAG ETL (séance 4)

Lit le JSON produit par scraper_dag, transforme les animes scrapés et
les indexe dans Elasticsearch (index `anidex_animes`, alimenté en continu
par le scraper, distinct de l'index `anime` chargé depuis le CSV en sem 1).

Déclenché automatiquement par scraper_dag via TriggerDagRunOperator,
mais aussi déclenchable manuellement (avec ou sans conf).
"""

from datetime import datetime, timedelta
from pathlib import Path
import json
import time

from airflow import DAG
from airflow.operators.python import PythonOperator


RAW_DIR = "/opt/airflow/data/raw"
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "anidex_animes"

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================
# TASK 1 : RESOLVE_INPUT
# ============================================

def resolve_input(**kwargs):
    """Détermine le fichier JSON à indexer.

    Priorité :
    1. dag_run.conf['json_path'] (passé par scraper_dag)
    2. Le fichier le plus récent dans /opt/airflow/data/raw/
    """
    ti = kwargs["ti"]
    conf = kwargs.get("dag_run").conf or {}

    json_path = conf.get("json_path")

    if not json_path:
        files = sorted(Path(RAW_DIR).glob("anime_*.json"))
        if not files:
            raise FileNotFoundError(f"Aucun fichier anime_*.json dans {RAW_DIR}")
        json_path = str(files[-1])
        print(f"Pas de json_path en conf, utilisation du plus récent : {json_path}")
    else:
        print(f"Fichier reçu de scraper_dag : {json_path}")

    ti.xcom_push(key="json_path", value=json_path)


# ============================================
# TASK 2 : TRANSFORM
# ============================================

def transform(**kwargs):
    """Aplatit la structure scrapée en documents prêts pour l'indexation.

    Le JSON produit par scrape_to_file a la structure :
        {"scraped_at": ..., "stats": {...}, "animes": [...], "news": [...]}

    On garde la liste des animes, on enrichit avec scraped_at, et on filtre
    les champs vides (None) pour avoir un index propre.
    """
    ti = kwargs["ti"]
    json_path = ti.xcom_pull(task_ids="resolve_input", key="json_path")

    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    scraped_at = payload.get("scraped_at")
    animes = payload.get("animes", [])

    docs = []
    for anime in animes:
        doc = {k: v for k, v in anime.items() if v is not None and v != ""}
        doc["scraped_at"] = scraped_at
        docs.append(doc)

    print(f"Stats source : {payload.get('stats')}")
    print(f"{len(docs)} documents prêts à indexer")

    ti.xcom_push(key="docs_count", value=len(docs))
    ti.xcom_push(key="docs", value=docs)


# ============================================
# TASK 3 : LOAD ES
# ============================================

def load_es(**kwargs):
    """Bulk-indexe les documents dans Elasticsearch."""
    from elasticsearch import Elasticsearch, helpers

    ti = kwargs["ti"]
    docs = ti.xcom_pull(task_ids="transform", key="docs")

    es = Elasticsearch(ES_HOST, request_timeout=60)

    # Attente que ES soit prêt (cluster.health)
    for attempt in range(15):
        try:
            health = es.cluster.health()
            print(f"ES cluster '{health['cluster_name']}' status={health['status']}")
            break
        except Exception:
            if attempt < 14:
                print(f"ES pas prêt ({attempt + 1}/15)...")
                time.sleep(5)
            else:
                raise ConnectionError("Impossible de se connecter à Elasticsearch")

    # Création idempotente de l'index
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX)
        print(f"Index '{ES_INDEX}' créé")

    # Bulk indexation : on UPSERT par id (= l'id de l'anime sur le mock-site)
    # comme ça les runs successifs mettent à jour les mêmes docs sans dupliquer.
    def gen_actions():
        for doc in docs:
            action = {"_op_type": "index", "_index": ES_INDEX, "_source": doc}
            doc_id = doc.get("id")
            if doc_id is not None:
                action["_id"] = str(doc_id)
            yield action

    start = time.time()
    success, errors = 0, 0
    for ok_flag, _ in helpers.streaming_bulk(
        es,
        gen_actions(),
        chunk_size=500,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        success += 1 if ok_flag else 0
        errors += 0 if ok_flag else 1

    elapsed = time.time() - start
    es.indices.refresh(index=ES_INDEX)

    print(f"Indexation : {success} OK, {errors} erreurs en {elapsed:.1f}s")
    ti.xcom_push(key="indexed", value=success)
    ti.xcom_push(key="errors", value=errors)


# ============================================
# TASK 4 : VERIFY
# ============================================

def verify(**kwargs):
    """Vérifie le contenu de l'index après chargement."""
    from elasticsearch import Elasticsearch

    es = Elasticsearch(ES_HOST, request_timeout=30)
    count = es.count(index=ES_INDEX)["count"]

    print("=" * 50)
    print(f"ETL TERMINÉ — index '{ES_INDEX}'")
    print(f"  Documents indexés : {count}")
    print(f"  Grafana           : http://localhost:3000")
    print("=" * 50)


# ============================================
# DAG
# ============================================

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description="Lit le JSON scrapé et l'indexe dans Elasticsearch (anidex_animes)",
    schedule_interval=None,  # déclenché par scraper_dag, jamais auto
    start_date=datetime(2026, 4, 28),
    catchup=False,
    tags=["anidata", "etl", "elasticsearch"],
) as dag:

    t_resolve = PythonOperator(task_id="resolve_input", python_callable=resolve_input)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load = PythonOperator(task_id="load_es", python_callable=load_es)
    t_verify = PythonOperator(task_id="verify", python_callable=verify)

    t_resolve >> t_transform >> t_load >> t_verify
