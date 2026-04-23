import json
import time
import re
from typing import List, Dict, Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from config import get_es_client, INDEX_NAME, DATA_FILE


def create_index_with_mapping(es: Elasticsearch, index_name: str = INDEX_NAME) -> None:
    mappings = {
        "properties": {
            "title": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "directors": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "actors": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "genres": {
                "type": "keyword"
            },
            "year": {
                "type": "integer"
            },
            "rating": {
                "type": "float"
            },
            "rank": {
                "type": "integer"
            },
            "release_date": {
                "type": "date"
            },
            "plot": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "plot_tokens": {
                "type": "keyword"
            },
            "running_time_secs": {
                "type": "integer"
            },
            "image_url": {
                "type": "keyword"
            }
        }
    }

    settings = {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Index supprimé : {index_name}")

    es.indices.create(index=index_name, settings=settings, mappings=mappings)
    print(f"Index créé avec mapping : {index_name}")


def load_bulk_actions(filename: str, index_name: str) -> List[Dict[str, Any]]:
        actions = []

        stopwords = {
            "the", "a", "an", "of", "and", "to", "in", "is", "it", "on", "for",
            "with", "as", "at", "by", "from", "this", "that", "his", "her",
            "their", "into", "about", "after", "before", "who", "what", "when",
            "where", "which", "while", "during", "through", "over", "under"
        }

        with open(filename, "r", encoding="utf-8") as f:
            lines = f.readlines()

        total_lines = len(lines)
        print(f"Fichier chargé : {filename}")
        print(f"Nombre total de lignes : {total_lines}")

        for i in range(0, total_lines, 2):
            try:
                meta_line = lines[i].strip()
                if not meta_line:
                    continue

                if i + 1 >= total_lines:
                    print(f"Ligne document manquante après la ligne {i + 1}")
                    break

                doc_line = lines[i + 1].strip()
                if not doc_line:
                    continue

                meta = json.loads(meta_line)
                doc = json.loads(doc_line)

                movie = doc.get("fields", {})
                if not movie:
                    print(f"Document vide ou mal formé à la paire de lignes {i + 1}-{i + 2}")
                    continue

                # Générer plot_tokens à partir de plot
                plot_value = movie.get("plot", "")

                # gérer le cas liste vs string
                if isinstance(plot_value, list):
                    plot_text = plot_value[0] if plot_value else ""
                else:
                    plot_text = plot_value

                tokens = re.findall(r"\b[a-zA-Z]{4,}\b", plot_text.lower())

                stopwords = {"the", "a", "of", "and", "to", "in", "is", "it", "on", "for", "with"}

                tokens = [t for t in tokens if t not in stopwords]

                movie["plot_tokens"] = tokens

                doc_id = None
                if "index" in meta and "_id" in meta["index"]:
                    doc_id = meta["index"]["_id"]

                action = {
                    "_index": index_name,
                    "_source": movie
                }

                if doc_id is not None:
                    action["_id"] = doc_id

                actions.append(action)

                if len(actions) % 500 == 0:
                    print(f"{len(actions)} documents préparés...")

            except json.JSONDecodeError as e:
                print(f"Erreur JSON aux lignes {i + 1}-{i + 2}: {e}")
            except Exception as e:
                print(f"Erreur inattendue aux lignes {i + 1}-{i + 2}: {e}")

        return actions


def index_movies(es: Elasticsearch, filename: str, index_name: str = INDEX_NAME) -> None:
    start = time.time()

    actions = load_bulk_actions(filename, index_name)
    print(f"Total documents prêts à indexer : {len(actions)}")

    if not actions:
        print("Aucun document à indexer.")
        return

    try:
        success, errors = bulk(es, actions, raise_on_error=False)
        print(f"{success} films indexés avec succès")

        if errors:
            print(f"{len(errors)} erreurs rencontrées")
        else:
            print("Aucune erreur d'indexation")
    except Exception as e:
        print(f"Erreur lors de l'indexation bulk : {e}")
        return

    es.indices.refresh(index=index_name)

    elapsed = time.time() - start
    print(f"Temps total : {elapsed:.2f}s")


def verify_index(es: Elasticsearch, index_name: str = INDEX_NAME) -> None:
    print("\n--- Vérification de l'index ---")

    count = es.count(index=index_name)["count"]
    print(f"Nombre de documents indexés : {count}")

    print("\nÉchantillon de 3 documents :")
    result = es.search(index=index_name, size=3)

    for hit in result["hits"]["hits"]:
        source = hit["_source"]
        print(f"- {source.get('title', 'N/A')} | note={source.get('rating', 'N/A')} | année={source.get('year', 'N/A')}")

    mapping = es.indices.get_mapping(index=index_name)
    print("\nMapping récupéré avec succès.")
    print(f"Champs présents : {list(mapping[index_name]['mappings']['properties'].keys())}")


if __name__ == "__main__":
    es = get_es_client()

    info = es.info()
    print(f"Connecté à Elasticsearch {info['version']['number']}")

    create_index_with_mapping(es)
    index_movies(es, str(DATA_FILE))
    verify_index(es)