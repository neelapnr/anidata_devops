"""
AniData Lab - Script de nettoyage
Suppression doublons, NaN deguises, correction types, trim textes.
"""

import pandas as pd
import numpy as np

NAN_VALUES = ["Unknown", "unknown", "UNKNOWN", "N/A", "n/a", "NA", "None", "none", "-", ".", ""]

NUMERIC_COLS = [
    "score", "episodes", "ranked", "popularity", "members", "favorites",
    "watching", "completed", "on_hold", "dropped", "plan_to_watch",
    "score_10", "score_9", "score_8", "score_7", "score_6",
    "score_5", "score_4", "score_3", "score_2", "score_1",
]


def clean_dataframe(df):
    """Nettoyage complet d'un DataFrame anime."""
    n_before = len(df)

    # Colonnes en snake_case
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

    # Doublons
    df = df.drop_duplicates()
    if "mal_id" in df.columns:
        df = df.drop_duplicates(subset=["mal_id"], keep="first")

    # NaN deguises
    for col in df.select_dtypes(include=["object"]).columns:
        df.loc[df[col].isin(NAN_VALUES), col] = np.nan

    # Types numeriques
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Score 0 => NaN
    if "score" in df.columns:
        df.loc[df["score"] == 0, "score"] = np.nan

    # Trim textes
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True)

    # Multi-valuees (genres, studios, etc.)
    for col in ["genres", "producers", "licensors", "studios"]:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: ", ".join(i.strip() for i in str(v).split(",") if i.strip()) if pd.notna(v) else np.nan
            )

    # Dates
    if "aired" in df.columns:
        df["aired_start"] = pd.to_datetime(df["aired"].str.split(" to ").str[0].str.strip(), errors="coerce")
        df["aired_end"] = pd.to_datetime(df["aired"].str.split(" to ").str[1].str.strip(), errors="coerce")

    # Outliers (marques, pas supprimes)
    df["is_outlier"] = False
    if "score" in df.columns:
        df.loc[df["score"].notna() & ((df["score"] < 1) | (df["score"] > 10)), "is_outlier"] = True
    if "episodes" in df.columns:
        df.loc[df["episodes"].notna() & (df["episodes"] > 5000), "is_outlier"] = True

    print(f"Nettoyage : {n_before:,} => {len(df):,} lignes | Outliers : {df['is_outlier'].sum()}")
    return df
