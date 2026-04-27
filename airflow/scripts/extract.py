"""
AniData Lab - Script d'extraction
Charge les fichiers CSV bruts et valide leur schema.
"""

import os
import pandas as pd

DATA_DIR = "/opt/airflow/data"

REQUIRED_COLS = {
    "anime": ["MAL_ID", "Name", "Score", "Genres", "Type", "Episodes", "Studios"],
    "ratings": ["user_id", "anime_id", "rating"],
    "synopsis": ["MAL_ID", "Name"],
}


def load_csv(filename, nrows=None):
    """Charge un CSV depuis DATA_DIR."""
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    df = pd.read_csv(path, nrows=nrows)
    print(f"{filename} : {len(df):,} lignes x {df.shape[1]} colonnes")
    return df


def count_lines(filename):
    """Estime les lignes d'un CSV via la taille du fichier."""
    path = os.path.join(DATADIR, filename)
    size = os.path.getsize(path)
    with open(path, encoding="utf-8") as f:
        sample = [f.readline() for  in range(1001)]
    avg_line = sum(len(l) for l in sample[1:]) / len(sample[1:])
    estimated = int(size / avg_line)
    print(f"{filename} : ~{estimated:,} lignes estimees ({size / 1024 / 1024:.0f} MB)")
    return estimated


def validate_schema(df, file_key):
    """Verifie que les colonnes obligatoires sont presentes."""
    required = REQUIRED_COLS.get(file_key, [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{file_key} : colonnes manquantes {missing}")
    print(f"Schema {file_key} OK : {list(df.columns)}")
