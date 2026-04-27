"""
AniData Lab - DAG 2 : Injecteur de donnees JSON/XML
Lit les fichiers anime_gold.json et anime_gold.xml,
les pousse via XCom, puis declenche le DAG 1 (etl_pipeline)
en lui transmettant les donnees via conf.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from anidata_utils import DATA_DIR

import os
import json

default_args = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================
# TASK 1 : LIRE LE FICHIER JSON
# ============================================

def read_json(**kwargs):
    """Lit anime_gold.json (NDJSON) et pousse les records via XCom."""
    json_path = os.path.join(DATA_DIR, "anime_gold.json")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Fichier introuvable : {json_path}")

    records = []
    with open(json_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    print(f"JSON charge : {len(records):,} records depuis {json_path}")
    print(f"Colonnes    : {list(records[0].keys()) if records else 'vide'}")

    # Pousser un resume via XCom (pas tous les records, trop lourd)
    summary = {
        "source": "anime_gold.json",
        "n_records": len(records),
        "columns": list(records[0].keys()) if records else [],
        "sample": records[:3],
    }
    kwargs["ti"].xcom_push(key="json_summary", value=summary)
    kwargs["ti"].xcom_push(key="json_count", value=len(records))


# ============================================
# TASK 2 : LIRE LE FICHIER XML
# ============================================

def read_xml(**kwargs):
    """Lit anime_gold.xml et pousse les records via XCom."""
    import xml.etree.ElementTree as ET

    xml_path = os.path.join(DATA_DIR, "anime_gold.xml")

    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"Fichier introuvable : {xml_path}")

    tree = ET.parse(xml_path)
    root = tree.getroot()

    records = []
    for anime in root.findall("anime"):
        record = {}
        for child in anime:
            record[child.tag] = child.text
        records.append(record)

    print(f"XML charge : {len(records):,} records depuis {xml_path}")
    print(f"Colonnes   : {list(records[0].keys()) if records else 'vide'}")

    summary = {
        "source": "anime_gold.xml",
        "n_records": len(records),
        "columns": list(records[0].keys()) if records else [],
        "sample": records[:3],
    }
    kwargs["ti"].xcom_push(key="xml_summary", value=summary)
    kwargs["ti"].xcom_push(key="xml_count", value=len(records))


# ============================================
# TASK 3 : VALIDER LES DONNEES
# ============================================

def validate_injected(**kwargs):
    """Verifie que les fichiers JSON et XML sont coherents."""
    ti = kwargs["ti"]
    json_summary = ti.xcom_pull(task_ids="read_json", key="json_summary")
    xml_summary = ti.xcom_pull(task_ids="read_xml", key="xml_summary")

    print("=" * 50)
    print("VALIDATION DES DONNEES INJECTEES")
    print(f"  JSON : {json_summary['n_records']:,} records, {len(json_summary['columns'])} colonnes")
    print(f"  XML  : {xml_summary['n_records']:,} records, {len(xml_summary['columns'])} colonnes")

    # Verifier coherence
    if json_summary["n_records"] == 0:
        raise ValueError("Fichier JSON vide")
    if xml_summary["n_records"] == 0:
        raise ValueError("Fichier XML vide")

    diff = abs(json_summary["n_records"] - xml_summary["n_records"])
    if diff > 0:
        print(f"  Difference : {diff} records (normal si les sources different)")

    print("  Validation OK")
    print("=" * 50)

    # Preparer les donnees a transmettre au DAG 1 via conf
    conf = {
        "json_records": json_summary["sample"],
        "xml_records": xml_summary["sample"],
        "json_count": json_summary["n_records"],
        "xml_count": xml_summary["n_records"],
        "triggered_by": "dag2_data_injector",
    }
    ti.xcom_push(key="trigger_conf", value=conf)


# ============================================
# DAG
# ============================================

with DAG(
    dag_id="dag2_data_injector",
    default_args=default_args,
    description="Lit JSON + XML, valide, puis declenche le pipeline ETL (DAG 1)",
    schedule_interval=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["anidata", "injector", "xcom"],
) as dag:

    t_json = PythonOperator(task_id="read_json", python_callable=read_json)
    t_xml = PythonOperator(task_id="read_xml", python_callable=read_xml)
    t_validate = PythonOperator(task_id="validate_injected", python_callable=validate_injected)

    t_trigger = TriggerDagRunOperator(
        task_id="trigger_etl_pipeline",
        trigger_dag_id="dag1_etl_pipeline",
        conf={
            "triggered_by": "dag2_data_injector",
            "json_records": "{{ ti.xcom_pull(task_ids='validate_injected', key='trigger_conf')['json_records'] }}",
            "xml_records": "{{ ti.xcom_pull(task_ids='validate_injected', key='trigger_conf')['xml_records'] }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # JSON et XML en parallele, puis validation, puis trigger DAG 1
    [t_json, t_xml] >> t_validate >> t_trigger
