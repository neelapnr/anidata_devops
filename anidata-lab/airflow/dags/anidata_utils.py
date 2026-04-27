"""
AniData Lab - Module utilitaire central
Rassemble toutes les fonctions des scripts individuels.
Les DAGs importent depuis ce fichier unique.

Scripts sources :
  
scripts/extract.py   : chargement CSV, validation schema
scripts/clean.py     : nettoyage (doublons, NaN, types, outliers)
scripts/transform.py : feature engineering (weighted_score, drop_ratio...)
scripts/export.py    : staging, export gold CSV/JSON versionne
scripts/load.py      : indexation Elasticsearch, verification
"""

#--- Extract ---
from scripts.extract import load_csv, count_lines, validate_schema, DATA_DIR

#--- Clean ---
from scripts.clean import clean_dataframe, NAN_VALUES, NUMERIC_COLS

#--- Transform ---
from scripts.transform import add_features

#--- Export ---
from scripts.export import save_staging, export_gold, STAGING_DIR, GOLD_DIR

