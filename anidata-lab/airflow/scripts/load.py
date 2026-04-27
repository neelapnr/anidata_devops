"""
AniData Lab - Script de chargement Elasticsearch
Indexation bulk + verification.
"""

import os
import json
import time

DATA_DIR = "/opt/airflow/data"

ES_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "anime_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "mal_id": {"type": "integer"},
            "name": {"type": "text", "analyzer": "anime_analyzer",
                     "fields": {"keyword": {"type": "keyword"}}},
            "english_name": {"type": "text", "analyzer": "anime_analyzer"},
            "japanese_name": {"type": "text"},
            "score": {"type": "float"},
            "weighted_score": {"type": "float"},
            "score_category": {"type": "keyword"},
            "type": {"type": "keyword"},
            "source": {"type": "keyword"},
            "rating": {"type": "keyword"},
            "genres": {"type": "keyword"},
            "main_genre": {"type": "keyword"},
            "n_genres": {"type": "integer"},
            "studios": {"type": "keyword"},
            "main_studio": {"type": "keyword"},
            "studio_tier": {"type": "keyword"},
            "episodes": {"type": "integer"},
            "members": {"type": "long"},
            "favorites": {"type": "long"},
            "popularity": {"type": "integer"},
            "ranked": {"type": "float"},
            "drop_ratio": {"type": "float"},
            "engagement_ratio": {"type": "float"},
            "watching": {"type": "long"},
            "completed": {"type": "long"},
            "on_hold": {"type": "long"},
            "dropped": {"type": "long"},
            "plan_to_watch": {"type": "long"},
            "aired": {"type": "text"},
            "aired_start": {"type": "date", "format": "yyyy-MM-dd||epoch_millis", "ignore_malformed": True},
            "aired_end": {"type": "date", "format": "yyyy-MM-dd||epoch_millis", "ignore_malformed": True},
            "premiered": {"type": "keyword"},
            "year": {"type": "integer"},
            "decade": {"type": "integer"},
            "duration": {"type": "keyword"},
            "producers": {"type": "keyword"},
            "licensors": {"type": "keyword"},
            "is_outlier": {"type": "boolean"},
        }
    },
}


def index_to_elasticsearch(json_path, index_name="anime"):
    """Indexe un fichier NDJSON dans Elasticsearch."""
    from elasticsearch import Elasticsearch, helpers

    es = Elasticsearch("http://elasticsearch:9200", request_timeout=60)

    # Attente ES
    for attempt in range(15):
        try:
            health = es.cluster.health()
            print(f"ES cluster '{health['cluster_name']}' status={health['status']}")
            break
        except Exception:
            if attempt < 14:
                print(f"ES pas pret ({attempt+1}/15)...")
                time.sleep(5)
            else:
                raise ConnectionError("Impossible de se connecter a Elasticsearch")

    # Charger les docs
    with open(json_path, "r", encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]

    # Recreer l'index
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    es.indices.create(index=index_name, body=ES_MAPPING)
    print(f"Index '{index_name}' cree ({len(docs):,} docs a indexer)")

    # Bulk indexation
    def gen_actions():
        for doc in docs:
            clean = {k: v for k, v in doc.items() if v is not None and not (isinstance(v, float) and v != v)}
            action = {"_index": index_name, "_source": clean}
            doc_id = doc.get("mal_id")
            if doc_id is not None:
                action["_id"] = str(doc_id)
            yield action

    start = time.time()
    success, errors = 0, 0
    for ok_flag, _ in helpers.streaming_bulk(es, gen_actions(), chunk_size=500, raise_on_error=False, raise_on_exception=False):
        success += 1 if ok_flag else 0
        errors += 0 if ok_flag else 1

    elapsed = time.time() - start
    es.indices.refresh(index=index_name)
    print(f"Indexation : {success:,} OK, {errors} erreurs en {elapsed:.1f}s ({success/elapsed:.0f} docs/s)")
    return success, errors


def verify_elasticsearch(index_name="anime"):
    """Verifie l'index Elasticsearch avec des requetes de test."""
    from elasticsearch import Elasticsearch

    es = Elasticsearch("http://elasticsearch:9200", request_timeout=30)
    count = es.count(index=index_name)["count"]
    stats = es.indices.stats(index=index_name)
    size_mb = stats["indices"][index_name]["total"]["store"]["size_in_bytes"] / (1024 * 1024)

    print(f"Index '{index_name}' : {count:,} docs, {size_mb:.1f} MB")

    for name, query in [("score>9", {"range": {"score": {"gte": 9}}}), ("Action", {"term": {"main_genre": "Action"}})]:
        hits = es.search(index=index_name, body={"query": query, "size": 1})["hits"]["total"]["value"]
        print(f"  {name} : {hits:,} resultats")

    return count, size_mb
