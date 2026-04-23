from typing import Dict, Any, List

from elasticsearch import Elasticsearch

from config import get_es_client, INDEX_NAME


def global_stats(es: Elasticsearch, index: str = INDEX_NAME) -> Dict[str, Any]:
    """
    Calcule les statistiques globales du dataset.
    """
    aggs = {
        "avg_rating": {"avg": {"field": "rating"}},
        "min_rating": {"min": {"field": "rating"}},
        "max_rating": {"max": {"field": "rating"}}
    }

    result = es.search(index=index, size=0, aggs=aggs)

    total_films = es.count(index=index)["count"]
    avg_rating = result["aggregations"]["avg_rating"]["value"]
    min_rating = result["aggregations"]["min_rating"]["value"]
    max_rating = result["aggregations"]["max_rating"]["value"]

    best_movie = es.search(
        index=index,
        size=1,
        sort=[{"rating": {"order": "desc"}}]
    )["hits"]["hits"][0]["_source"]

    worst_movie = es.search(
        index=index,
        size=1,
        sort=[{"rating": {"order": "asc"}}]
    )["hits"]["hits"][0]["_source"]

    print("\n=== Statistiques globales ===")
    print(f"Nombre total de films : {total_films}")
    print(f"Note moyenne          : {avg_rating:.2f}")
    print(f"Note minimale         : {min_rating}")
    print(f"Note maximale         : {max_rating}")
    print(f"Film le mieux noté    : {best_movie.get('title')} ({best_movie.get('rating')})")
    print(f"Film le moins bien noté : {worst_movie.get('title')} ({worst_movie.get('rating')})")

    return result


def top_genres(es: Elasticsearch, index: str = INDEX_NAME, size: int = 10) -> Dict[str, Any]:
    """
    Top des genres les plus représentés.
    """
    aggs = {
        "genres": {
            "terms": {
                "field": "genres",
                "size": size
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print(f"\n=== Top {size} genres ===")
    for bucket in result["aggregations"]["genres"]["buckets"]:
        print(f"{bucket['key']}: {bucket['doc_count']}")

    return result


def top_directors(es: Elasticsearch, index: str = INDEX_NAME, size: int = 10) -> Dict[str, Any]:
    """
    Top des réalisateurs les plus prolifiques.
    """
    aggs = {
        "directors": {
            "terms": {
                "field": "directors.keyword",
                "size": size
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print(f"\n=== Top {size} réalisateurs ===")
    for bucket in result["aggregations"]["directors"]["buckets"]:
        print(f"{bucket['key']}: {bucket['doc_count']}")

    return result


def top_actors(es: Elasticsearch, index: str = INDEX_NAME, size: int = 10) -> Dict[str, Any]:
    """
    Top des acteurs les plus présents.
    """
    aggs = {
        "actors": {
            "terms": {
                "field": "actors.keyword",
                "size": size
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print(f"\n=== Top {size} acteurs ===")
    for bucket in result["aggregations"]["actors"]["buckets"]:
        print(f"{bucket['key']}: {bucket['doc_count']}")

    return result


def movies_by_decade(es: Elasticsearch, index: str = INDEX_NAME) -> Dict[str, Any]:
    """
    Distribution des films par décennie.
    """
    aggs = {
        "decades": {
            "histogram": {
                "field": "year",
                "interval": 10,
                "min_doc_count": 1
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print("\n=== Films par décennie ===")
    for bucket in result["aggregations"]["decades"]["buckets"]:
        decade = int(bucket["key"])
        print(f"{decade}s: {bucket['doc_count']}")

    return result


def average_rating_by_year(es: Elasticsearch, index: str = INDEX_NAME) -> Dict[str, Any]:
    """
    Calcule la note moyenne des films pour chaque année.
    On groupe par champ 'year', puis on calcule avg(rating).
    """
    aggs = {
        "years": {
            "terms": {
                "field": "year",
                "size": 200,
                "order": {"_key": "asc"}
            },
            "aggs": {
                "avg_rating": {
                    "avg": {
                        "field": "rating"
                    }
                },
                "movie_count": {
                    "value_count": {
                        "field": "year"
                    }
                }
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print("\n=== Évolution de la note moyenne par année ===")
    print("Chaque ligne = moyenne des notes IMDb des films sortis cette année-là.\n")

    for bucket in result["aggregations"]["years"]["buckets"]:
        year = bucket["key"]
        avg_rating = bucket["avg_rating"]["value"]
        count = bucket["doc_count"]

        if avg_rating is not None:
            print(f"{year} : note moyenne = {avg_rating:.2f} | nombre de films = {count}")

    return result


def best_rated_genres(es: Elasticsearch, index: str = INDEX_NAME, size: int = 10) -> Dict[str, Any]:
    """
    Genres les mieux notés en moyenne.
    """
    aggs = {
        "genres": {
            "terms": {
                "field": "genres",
                "size": size,
                "order": {"avg_rating": "desc"}
            },
            "aggs": {
                "avg_rating": {
                    "avg": {
                        "field": "rating"
                    }
                }
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    print(f"\n=== Top {size} genres les mieux notés ===")
    for bucket in result["aggregations"]["genres"]["buckets"]:
        avg_rating = bucket["avg_rating"]["value"]
        print(f"{bucket['key']}: {avg_rating:.2f} ({bucket['doc_count']} films)")

    return result


def best_rated_directors(
    es: Elasticsearch,
    min_films: int = 3,
    index: str = INDEX_NAME
) -> Dict[str, Any]:
    """
    Réalisateurs avec la meilleure note moyenne,
    en ne gardant que ceux qui ont au moins min_films films.
    """
    if min_films < 1:
        min_films = 3

    aggs = {
        "directors": {
            "terms": {
                "field": "directors.keyword",
                "size": 300
            },
            "aggs": {
                "avg_rating": {
                    "avg": {
                        "field": "rating"
                    }
                },
                "keep_only_directors_with_enough_movies": {
                    "bucket_selector": {
                        "buckets_path": {
                            "docCount": "_count"
                        },
                        "script": f"params.docCount >= {min_films}"
                    }
                }
            }
        }
    }

    result = es.search(index=index, size=0, aggs=aggs)

    buckets = result["aggregations"]["directors"]["buckets"]

    if not buckets:
        print(f"\nAucun réalisateur trouvé avec au moins {min_films} films.")
        print("Essaie une valeur plus petite, par exemple 3, 5 ou 10.")
        return result

    sorted_buckets = sorted(
        buckets,
        key=lambda x: x["avg_rating"]["value"] if x["avg_rating"]["value"] is not None else 0,
        reverse=True
    )

    print(f"\n=== Réalisateurs les mieux notés (minimum {min_films} films) ===")
    for bucket in sorted_buckets[:10]:
        avg_rating = bucket["avg_rating"]["value"]
        print(f"{bucket['key']}: {avg_rating:.2f} ({bucket['doc_count']} films)")

    return result


if __name__ == "__main__":
    es = get_es_client()

    print("=== TESTS analytics.py ===")
    global_stats(es)
    top_genres(es)
    top_directors(es)
    top_actors(es)
    movies_by_decade(es)
    average_rating_by_year(es)
    best_rated_genres(es)
    best_rated_directors(es, min_films=3)