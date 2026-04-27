"""
AniData Lab - DAG 1 : Pipeline ETL Complet
Extract (CSV) -> Nettoyage -> Validation -> Feature Engineering -> Export Gold -> Indexation ES

Ce DAG peut etre declenche manuellement ou par le DAG 2 (data_injector)
qui lui transmet des donnees JSON/XML via XCom (conf).
"""

from datetime import datetime, timedelta
from airflow import DAG
try:
    # Airflow 2+
    from airflow.operators.python import PythonOperator
except ImportError:
    # Airflow <2
    from airflow.operators.python_operator import PythonOperator  # type: ignore
from anidata_utils import (
    load_csv, validate_schema, clean_dataframe, add_features,
    save_staging, export_gold, index_to_elasticsearch, verify_elasticsearch,
    DATA_DIR,
)

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ============================================
# TASK 1 : EXTRACTION
# ============================================

def extract(**kwargs):
    """Charge le CSV brut. Si le DAG est declenche par le DAG 2,
    recupere les donnees injectees via conf (XCom inter-DAG)."""
    import pandas as pd
    import json as json_mod

    conf = kwargs.get("dag_run").conf or {}
    ti = kwargs["ti"]

    # Si le DAG 2 a envoye des donnees JSON/XML via conf
    if conf.get("json_records"):
        print(f"Donnees recues du DAG 2 : {len(conf['json_records'])} records JSON")
        ti.xcom_push(key="injected_json", value=len(conf["json_records"]))

    if conf.get("xml_records"):
        print(f"Donnees recues du DAG 2 : {len(conf['xml_records'])} records XML")
        ti.xcom_push(key="injected_xml", value=len(conf["xml_records"]))

    # Charger le CSV brut
    df = load_csv("anime.csv")
    validate_schema(df, "anime")
    path = save_staging(df, "anime_raw_staging.csv")

    ti.xcom_push(key="staging_path", value=path)
    ti.xcom_push(key="n_rows", value=len(df))


# ============================================
# TASK 2 : NETTOYAGE
# ============================================

def clean(**kwargs):
    """Applique le nettoyage complet sur les donnees brutes."""
    import pandas as pd

    ti = kwargs["ti"]
    staging_path = ti.xcom_pull(task_ids="extract", key="staging_path")

    df = pd.read_csv(staging_path)
    df = clean_dataframe(df)

    path = save_staging(df, "anime_cleaned.csv")
    ti.xcom_push(key="cleaned_path", value=path)


# ============================================
# TASK 3 : VALIDATION
# ============================================

def validate(**kwargs):
    """Verifie la qualite des donnees nettoyees."""
    import pandas as pd

    ti = kwargs["ti"]
    cleaned_path = ti.xcom_pull(task_ids="clean", key="cleaned_path")
    df = pd.read_csv(cleaned_path)

    n_rows = len(df)
    n_cols = df.shape[1]
    nan_pct = (df.isna().sum().sum() / (n_rows * n_cols) * 100)

    print(f"Lignes       : {n_rows:,}")
    print(f"Colonnes     : {n_cols}")
    print(f"NaN restants : {nan_pct:.1f}%")

    if n_rows < 10000:
        raise ValueError(f"Trop peu de lignes apres nettoyage : {n_rows}")
    if "score" not in df.columns:
        raise ValueError("Colonne 'score' manquante apres nettoyage")

    print("Validation OK")
    ti.xcom_push(key="validated_path", value=cleaned_path)


# ============================================
# TASK 4 : FEATURE ENGINEERING
# ============================================

def features(**kwargs):
    """Cree les features metier (weighted_score, drop_ratio, etc.)."""
    import pandas as pd

    ti = kwargs["ti"]
    validated_path = ti.xcom_pull(task_ids="validate", key="validated_path")

    df = pd.read_csv(validated_path)
    df = add_features(df)

    path = save_staging(df, "anime_gold.csv")
    ti.xcom_push(key="gold_staging_path", value=path)


# ============================================
# TASK 5 : EXPORT GOLD (CSV + JSON versionne)
# ============================================

def export(**kwargs):
    """Exporte le dataset gold en CSV versionne + JSON NDJSON."""
    import pandas as pd

    ti = kwargs["ti"]
    gold_staging = ti.xcom_pull(task_ids="features", key="gold_staging_path")

    df = pd.read_csv(gold_staging)
    version, json_path, n_docs = export_gold(df)

    ti.xcom_push(key="json_path", value=json_path)
    ti.xcom_push(key="version", value=version)
    ti.xcom_push(key="n_docs", value=n_docs)


# ============================================
# TASK 6 : INDEXATION ELASTICSEARCH
# ============================================

def load_es(**kwargs):
    """Indexe le dataset gold dans Elasticsearch."""
    ti = kwargs["ti"]
    json_path = ti.xcom_pull(task_ids="export", key="json_path")

    success, errors = index_to_elasticsearch(json_path)
    ti.xcom_push(key="indexed", value=success)
    ti.xcom_push(key="errors", value=errors)


# ============================================
# TASK 7 : VERIFICATION
# ============================================

def verify(**kwargs):
    """Verifie que les donnees sont bien dans Elasticsearch."""
    ti = kwargs["ti"]
    count, size_mb = verify_elasticsearch()

    version = ti.xcom_pull(task_ids="export", key="version")
    n_docs = ti.xcom_pull(task_ids="export", key="n_docs")
    injected_json = ti.xcom_pull(task_ids="extract", key="injected_json")
    injected_xml = ti.xcom_pull(task_ids="extract", key="injected_xml")

    print("=" * 50)
    print("PIPELINE ETL TERMINE")
    print(f"  Gold version  : v{version}")
    print(f"  Documents     : {n_docs:,}")
    print(f"  ES index      : {count:,} docs, {size_mb:.1f} MB")
    if injected_json:
        print(f"  Injection JSON: {injected_json} records (depuis DAG 2)")
    if injected_xml:
        print(f"  Injection XML : {injected_xml} records (depuis DAG 2)")
    print(f"  Grafana       : http://localhost:3000")
    print("=" * 50)


# ============================================
# DAG
# ============================================

with DAG(
    dag_id="dag1_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet : Extract > Clean > Validate > Features > Export > Load ES",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "etl", "pipeline"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_clean = PythonOperator(task_id="clean", python_callable=clean)
    t_validate = PythonOperator(task_id="validate", python_callable=validate)
    t_features = PythonOperator(task_id="features", python_callable=features)
    t_export = PythonOperator(task_id="export", python_callable=export)
    t_load = PythonOperator(task_id="load_es", python_callable=load_es)
    t_verify = PythonOperator(task_id="verify", python_callable=verify)

    t_extract >> t_clean >> t_validate >> t_features >> t_export >> t_load >> t_verify
