"""
AniData Lab - DAG Scraper (séance 4)

Scrape le mock-site AniDex avec BeautifulSoup (lib `anidata_scraper`),
écrit le résultat en JSON dans /opt/airflow/data/raw/, puis déclenche
etl_dag pour l'indexation dans Elasticsearch.

Chaîne complète :
    scraper_dag → (TriggerDagRunOperator) → etl_dag → Elasticsearch → Grafana
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Importé depuis le package installé dans l'image Docker custom
# (cf. Dockerfile racine : pip install /opt/airflow/scraper/)
from anidata_scraper import scrape_to_file


RAW_DIR = "/opt/airflow/data/raw"
MOCK_SITE_URL = "http://mock-site"

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_scraper(**kwargs):
    """Lance le scraper et pousse le chemin du JSON produit dans XCom."""
    ti = kwargs["ti"]

    json_path = scrape_to_file(
        output_dir=RAW_DIR,
        base_url=MOCK_SITE_URL,
        enrich=True,
    )

    print(f"Fichier JSON produit : {json_path}")
    ti.xcom_push(key="json_path", value=json_path)
    return json_path


with DAG(
    dag_id="scraper_dag",
    default_args=default_args,
    description="Scrape le mock-site AniDex (BS4) et déclenche l'ETL",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    tags=["anidata", "scraper", "bs4"],
) as dag:

    t_scrape = PythonOperator(
        task_id="scrape_anidex",
        python_callable=run_scraper,
    )

    # Déclenche etl_dag en lui passant le chemin du JSON via conf.
    # etl_dag pourra le récupérer via dag_run.conf['json_path'].
    t_trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_dag",
        trigger_dag_id="etl_dag",
        conf={"json_path": "{{ ti.xcom_pull(task_ids='scrape_anidex', key='json_path') }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t_scrape >> t_trigger_etl

# Demo CI/CD
