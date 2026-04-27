"""
AniData Lab - Script de feature engineering
Cree les metriques metier : weighted_score, drop_ratio, studio_tier, etc.
"""

import pandas as pd
import numpy as np


def add_features(df):
    """Ajoute toutes les features metier au DataFrame."""
    n_before = df.shape[1]

    # weighted_score = score * log(members)
    if "score" in df.columns and "members" in df.columns:
        df["weighted_score"] = (df["score"] * np.log10(df["members"].fillna(0) + 1)).round(2)
        df.loc[df["score"].isna(), "weighted_score"] = np.nan

    # drop_ratio = dropped / (dropped + completed)
    if "dropped" in df.columns and "completed" in df.columns:
        total = df["dropped"].fillna(0) + df["completed"].fillna(0)
        df["drop_ratio"] = np.where(total > 0, (df["dropped"].fillna(0) / total).round(4), np.nan)

    # score_category : Mauvais / Moyen / Bon / Excellent
    if "score" in df.columns:
        df["score_category"] = pd.cut(
            df["score"], bins=[0, 5, 6.5, 8, 10],
            labels=["Mauvais", "Moyen", "Bon", "Excellent"], include_lowest=True,
        )

    # studio_tier : Top / Mid / Indie
    if "studios" in df.columns:
        df["main_studio"] = df["studios"].str.split(", ").str[0]
        counts = df["main_studio"].value_counts()
        df["studio_tier"] = df["main_studio"].apply(
            lambda s: np.nan if pd.isna(s) else ("Top" if counts.get(s, 0) >= 50 else ("Mid" if counts.get(s, 0) >= 10 else "Indie"))
        )

    # year, decade
    for c in ["aired_start", "aired", "premiered"]:
        if c in df.columns:
            dates = pd.to_datetime(df[c], errors="coerce")
            df["year"] = dates.dt.year
            df["decade"] = (df["year"] // 10 * 10).astype("Int64")
            break

    # genres : nombre + genre principal
    if "genres" in df.columns:
        df["n_genres"] = df["genres"].str.split(", ").str.len()
        df.loc[df["genres"].isna(), "n_genres"] = np.nan
        df["main_genre"] = df["genres"].str.split(", ").str[0]

    # engagement_ratio = favorites / members
    if "favorites" in df.columns and "members" in df.columns:
        df["favorites"] = pd.to_numeric(df["favorites"], errors="coerce")
        df["engagement_ratio"] = np.where(
            df["members"].fillna(0) > 0,
            (df["favorites"].fillna(0) / df["members"]).round(4), np.nan,
        )

    print(f"Features : {df.shape[1] - n_before} nouvelles colonnes")
    return df
