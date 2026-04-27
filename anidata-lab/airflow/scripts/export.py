"""
AniData Lab - Script d'export
Sauvegarde staging + export gold (CSV versionne + JSON NDJSON).
"""

import os
import json
import pandas as pd

DATA_DIR = "/opt/airflow/data"
STAGING_DIR = os.path.join(DATA_DIR, "staging")
GOLD_DIR = os.path.join(DATA_DIR, "gold")


def save_staging(df, name="anime_cleaned.csv"):
    """Sauvegarde en staging."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    path = os.path.join(STAGING_DIR, name)
    df.to_csv(path, index=False)
    print(f"Staging : {path}")
    return path


def export_gold(df):
    """Exporte le dataset gold en CSV versionne + JSON NDJSON."""
    import shutil

    os.makedirs(GOLD_DIR, exist_ok=True)

    # Numero de version
    existing = [f for f in os.listdir(GOLD_DIR) if f.startswith("anime_gold_v")]
    versions = []
    for f in existing:
        try:
            versions.append(int(f.split("_v")[1].split(".")[0]))
        except (IndexError, ValueError):
            pass
    version = max(versions) + 1 if versions else 1

    # Sauvegarder CSV versionne
    gold_csv = os.path.join(STAGING_DIR, "anime_gold.csv")
    df.to_csv(gold_csv, index=False)
    shutil.copyfile(gold_csv, os.path.join(GOLD_DIR, f"anime_gold_v{version}.csv"))
    shutil.copyfile(gold_csv, os.path.join(DATA_DIR, "anime_gold.csv"))

    # Export JSON NDJSON
    json_path = os.path.join(DATA_DIR, "anime_gold.json")

    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    for col in df.select_dtypes(include=["Int64"]).columns:
        df[col] = df[col].astype("float64")

    records = df.where(df.notna(), None).to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        for rec in records:
            clean = {k: v for k, v in rec.items() if v is not None}
            f.write(json.dumps(clean, ensure_ascii=False) + "\n")

    print(f"Gold v{version} : {len(records):,} docs | CSV + JSON")
    return version, json_path, len(records)
