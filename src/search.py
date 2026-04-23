from typing import Optional, List, Dict, Any

from elasticsearch import Elasticsearch

from config import get_es_client, INDEX_NAME


def format_movie_result(hit: Dict[str, Any]) -> Dict[str, Any]:
    source = hit["_source"]
    return {
        "title": source.get("title", "N/A"),
        "year": source.get("year", "N/A"),
        "rating": source.get("rating", "N/A"),
        "directors": ", ".join(source.get("directors", [])),
        "score": round(hit.get("_score", 0), 3) if hit.get("_score") is not None else None
    }


def print_results(results: Dict[str, Any], show_score: bool = True) -> None:
    hits = results["hits"]["hits"]

    if not hits:
        print("Aucun résultat trouvé.")
        return

    for i, hit in enumerate(hits, start=1):
        movie = format_movie_result(hit)
        print(f"{i}. {movie['title']} ({movie['year']})")
        print(f"   Note       : {movie['rating']}")
        print(f"   Réalisateur: {movie['directors']}")
        if show_score:
            print(f"   Score ES   : {movie['score']}")
        print("-" * 50)

    total = results["hits"]["total"]["value"]
    print(f"{total} résultat(s) trouvé(s)")


def search_by_title(es: Elasticsearch, query_text: str, index: str = INDEX_NAME) -> Dict[str, Any]:
    """
    Recherche des films par titre (match query).
    """
    query = {
        "match": {
            "title": query_text
        }
    }

    results = es.search(index=index, query=query, size=10)

    print(f'\nRésultats pour le titre : "{query_text}"')
    print_results(results, show_score=True)

    return results


def search_advanced(
    es: Elasticsearch,
    title: Optional[str] = None,
    actor: Optional[str] = None,
    director: Optional[str] = None,
    genre: Optional[str] = None,
    min_rating: Optional[float] = None,
    max_rating: Optional[float] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    index: str = INDEX_NAME
) -> Dict[str, Any]:
    """
    Recherche avancée multi-critères avec bool query.
    """
    must = []
    filters = []

    if title:
        must.append({"match": {"title": title}})
    if actor:
        must.append({"match": {"actors": actor}})
    if director:
        must.append({"match": {"directors": director}})
    if genre:
        filters.append({"term": {"genres": genre}})

    rating_range = {}
    if min_rating is not None:
        rating_range["gte"] = min_rating
    if max_rating is not None:
        rating_range["lte"] = max_rating
    if rating_range:
        filters.append({"range": {"rating": rating_range}})

    year_range = {}
    if year_from is not None:
        year_range["gte"] = year_from
    if year_to is not None:
        year_range["lte"] = year_to
    if year_range:
        filters.append({"range": {"year": year_range}})

    query = {
        "bool": {
            "must": must,
            "filter": filters
        }
    }

    results = es.search(index=index, query=query, size=10)

    print("\nRésultats recherche avancée")
    print_results(results, show_score=True)

    return results


def search_plot(es: Elasticsearch, keywords: str, index: str = INDEX_NAME) -> Dict[str, Any]:
    """
    Recherche dans le synopsis avec mise en évidence des termes.
    """
    query = {
        "match": {
            "plot": keywords
        }
    }

    highlight = {
        "fields": {
            "plot": {
                "fragment_size": 150,
                "number_of_fragments": 3,
                "pre_tags": ["**"],
                "post_tags": ["**"]
            }
        }
    }

    results = es.search(index=index, query=query, highlight=highlight, size=10)

    hits = results["hits"]["hits"]

    print(f'\nRésultats dans le synopsis pour : "{keywords}"')

    if not hits:
        print("Aucun résultat trouvé.")
        return results

    for i, hit in enumerate(hits, start=1):
        source = hit["_source"]
        print(f"{i}. {source.get('title', 'N/A')} ({source.get('year', 'N/A')})")
        print(f"   Note: {source.get('rating', 'N/A')}")

        if "highlight" in hit and "plot" in hit["highlight"]:
            print("   Extrait(s) :")
            for fragment in hit["highlight"]["plot"]:
                print(f"   - {fragment}")
        else:
            plot = source.get("plot", "")
            print(f"   Plot: {plot[:150]}...")

        print("-" * 50)

    total = results["hits"]["total"]["value"]
    print(f"{total} résultat(s) trouvé(s)")

    return results


def search_fuzzy(
    es: Elasticsearch,
    query_text: str,
    fuzziness: int = 2,
    index: str = INDEX_NAME
) -> Dict[str, Any]:
    """
    Recherche tolérante aux fautes de frappe.
    """
    query = {
        "match": {
            "title": {
                "query": query_text,
                "fuzziness": fuzziness
            }
        }
    }

    results = es.search(index=index, query=query, size=10)

    print(f'\nRésultats recherche floue pour : "{query_text}"')
    print_results(results, show_score=True)

    return results


def suggest_titles(es: Elasticsearch, prefix: str, index: str = INDEX_NAME) -> List[str]:
    """
    Auto-complétion basée sur un préfixe.
    Utilise match_phrase_prefix sur le champ title, ce qui fonctionne avec le mapping actuel.
    """
    query = {
        "match_phrase_prefix": {
            "title": prefix
        }
    }

    results = es.search(index=index, query=query, size=10)

    suggestions = []
    seen = set()

    for hit in results["hits"]["hits"]:
        title = hit["_source"].get("title")
        if title and title not in seen:
            seen.add(title)
            suggestions.append(title)

    print(f'\nSuggestions pour le préfixe : "{prefix}"')

    if not suggestions:
        print("Aucune suggestion trouvée.")
        return suggestions

    for i, title in enumerate(suggestions, start=1):
        print(f"{i}. {title}")

    return suggestions

def recommend_similar_movies(es, movie_title: str, index="movies", size=5):
    """
    Recommande des films similaires à partir d'un titre donné.
    La similarité repose sur :
    - le synopsis / titre (more_like_this)
    - les genres en commun
    - les acteurs en commun
    - les réalisateurs en commun
    """

    # 1. Retrouver le film de référence
    base_query = {
        "query": {
            "match": {
                "title": movie_title
            }
        },
        "size": 1
    }

    base_result = es.search(index=index, body=base_query)
    hits = base_result["hits"]["hits"]

    if not hits:
        return None, []

    base_hit = hits[0]
    base_id = base_hit["_id"]
    base_movie = base_hit["_source"]

    title = base_movie.get("title", "")
    plot = base_movie.get("plot", "")
    genres = base_movie.get("genres", [])
    actors = base_movie.get("actors", [])
    directors = base_movie.get("directors", [])

    should_clauses = []

    # 2. Similarité textuelle via more_like_this
    should_clauses.append({
        "more_like_this": {
            "fields": ["title", "plot"],
            "like": [
                {
                    "_index": index,
                    "_id": base_id
                }
            ],
            "min_term_freq": 1,
            "min_doc_freq": 1
        }
    })

    # 3. Bonus de similarité par genres
    for genre in genres:
        should_clauses.append({
            "term": {
                "genres": {
                    "value": genre,
                    "boost": 3.0
                }
            }
        })

    # 4. Bonus de similarité par acteurs
    for actor in actors:
        should_clauses.append({
            "term": {
                "actors.keyword": {
                    "value": actor,
                    "boost": 2.0
                }
            }
        })

    # 5. Bonus de similarité par réalisateurs
    for director in directors:
        should_clauses.append({
            "term": {
                "directors.keyword": {
                    "value": director,
                    "boost": 2.5
                }
            }
        })

    rec_query = {
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1,
                "must_not": [
                    {
                        "ids": {
                            "values": [base_id]
                        }
                    }
                ]
            }
        },
        "size": size
    }

    rec_result = es.search(index=index, body=rec_query)
    recommendations = rec_result["hits"]["hits"]

    return base_movie, recommendations

def print_recommendations(base_movie, recommendations):
    if base_movie is None:
        print("Film introuvable.")
        return

    print(f"\n=== Recommandations similaires à : {base_movie.get('title', 'N/A')} ===\n")

    if not recommendations:
        print("Aucune recommandation trouvée.")
        return

    for i, hit in enumerate(recommendations, start=1):
        source = hit["_source"]
        title = source.get("title", "N/A")
        year = source.get("year", "N/A")
        rating = source.get("rating", "N/A")
        directors = source.get("directors", [])
        if isinstance(directors, list):
            directors = ", ".join(directors)

        print(f"{i}. {title} ({year})")
        print(f"   Note       : {rating}")
        print(f"   Réalisateur: {directors}")
        print(f"   Score ES   : {round(hit.get('_score', 0), 3)}")
        print("-" * 50)


if __name__ == "__main__":
    es = get_es_client()

    print("=== TESTS search.py ===")
    search_by_title(es, "Inception")
    search_advanced(es, director="Christopher Nolan", min_rating=8)
    search_plot(es, "dream")
    search_fuzzy(es, "Incepion")
    suggest_titles(es, "Star")