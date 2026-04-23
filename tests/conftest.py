"""
Fixtures et configuration commune pour les tests.
"""
import pytest
from unittest.mock import MagicMock


# === DONNÉES FICTIVES RÉUTILISABLES ===

MOCK_MOVIE_INCEPTION = {
    "_id": "id_inception",
    "_score": 8.8,
    "_source": {
        "title": "Inception",
        "year": 2010,
        "rating": 8.8,
        "plot": "A thief who steals corporate secrets through the use of dream-sharing technology is given the inverse task of planting an idea...",
        "directors": ["Christopher Nolan"],
        "actors": ["Leonardo DiCaprio", "Marion Cotillard", "Joseph Gordon-Levitt"],
        "genres": ["Action", "Sci-Fi", "Thriller"]
    }
}

MOCK_MOVIE_INTERSTELLAR = {
    "_id": "id_interstellar",
    "_score": 8.6,
    "_source": {
        "title": "Interstellar",
        "year": 2014,
        "rating": 8.6,
        "plot": "A team of explorers travel through a wormhole in space in an attempt to ensure humanity's survival...",
        "directors": ["Christopher Nolan"],
        "actors": ["Matthew McConaughey", "Anne Hathaway", "Jessica Chastain"],
        "genres": ["Adventure", "Drama", "Sci-Fi"]
    }
}

MOCK_MOVIE_DARK_KNIGHT = {
    "_id": "id_dark_knight",
    "_score": 9.0,
    "_source": {
        "title": "The Dark Knight",
        "year": 2008,
        "rating": 9.0,
        "plot": "When the menace known as the Joker wreaks havoc and chaos on the people of Gotham...",
        "directors": ["Christopher Nolan"],
        "actors": ["Christian Bale", "Heath Ledger", "Aaron Eckhart"],
        "genres": ["Action", "Crime", "Drama"]
    }
}

MOCK_MOVIE_TITANIC = {
    "_id": "id_titanic",
    "_score": 7.8,
    "_source": {
        "title": "Titanic",
        "year": 1997,
        "rating": 7.8,
        "plot": "A seventeen-year-old aristocrat falls in love with a kind but poor artist aboard the luxurious, ill-fated R.M.S. Titanic...",
        "directors": ["James Cameron"],
        "actors": ["Leonardo DiCaprio", "Kate Winslet"],
        "genres": ["Drama", "Romance"]
    }
}

MOCK_MOVIE_BAD = {
    "_id": "id_bad",
    "_score": 3.0,
    "_source": {
        "title": "A Bad Movie",
        "year": 2000,
        "rating": 3.0,
        "plot": "A terrible film with no merit...",
        "directors": ["Unknown"],
        "actors": ["Unknown Actor"],
        "genres": ["Comedy"]
    }
}


# === FIXTURES POUR MOCK ELASTICSEARCH ===

@pytest.fixture
def mock_es():
    """Crée un mock Elasticsearch basique."""
    es = MagicMock()
    return es


@pytest.fixture
def mock_es_with_inception(mock_es):
    """Mock ES retournant Inception pour une recherche par titre."""
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [MOCK_MOVIE_INCEPTION]
        }
    }
    return mock_es


@pytest.fixture
def mock_es_empty_results(mock_es):
    """Mock ES retournant aucun résultat."""
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 0},
            "hits": []
        }
    }
    return mock_es


@pytest.fixture
def mock_es_multiple_results(mock_es):
    """Mock ES retournant plusieurs films."""
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 3},
            "hits": [
                MOCK_MOVIE_INCEPTION,
                MOCK_MOVIE_INTERSTELLAR,
                MOCK_MOVIE_DARK_KNIGHT
            ]
        }
    }
    return mock_es


@pytest.fixture
def mock_es_with_highlight(mock_es):
    """Mock ES retournant des résultats avec highlight."""
    movie_with_highlight = {
        **MOCK_MOVIE_INCEPTION,
        "highlight": {
            "plot": [
                "...a thief who steals **corporate secrets** through the use of...",
                "...is given the inverse task of planting an **idea** into..."
            ]
        }
    }
    mock_es.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [movie_with_highlight]
        }
    }
    return mock_es


@pytest.fixture
def mock_es_for_recommendations(mock_es):
    """Mock ES avec comportement double : retrouve le film de base, puis trouve des recommandations."""
    # Premier appel : trouver Inception
    # Deuxième appel : recommandations
    mock_es.search.side_effect = [
        {
            "hits": {
                "total": {"value": 1},
                "hits": [MOCK_MOVIE_INCEPTION]
            }
        },
        {
            "hits": {
                "total": {"value": 2},
                "hits": [MOCK_MOVIE_INTERSTELLAR, MOCK_MOVIE_DARK_KNIGHT]
            }
        }
    ]
    return mock_es


@pytest.fixture
def mock_es_global_stats(mock_es):
    """Mock ES pour la fonction global_stats."""
    mock_es.search.side_effect = [
        # Premier appel: agrégations globales
        {
            "aggregations": {
                "avg_rating": {"value": 7.42},
                "min_rating": {"value": 3.0},
                "max_rating": {"value": 9.0}
            },
            "hits": {"hits": []}
        },
        # Deuxième appel: meilleur film
        {
            "hits": {
                "hits": [MOCK_MOVIE_DARK_KNIGHT]
            }
        },
        # Troisième appel: pire film
        {
            "hits": {
                "hits": [MOCK_MOVIE_BAD]
            }
        }
    ]
    mock_es.count.return_value = {"count": 1000}
    return mock_es


@pytest.fixture
def mock_es_top_genres(mock_es):
    """Mock ES pour la fonction top_genres."""
    mock_es.search.return_value = {
        "aggregations": {
            "genres": {
                "buckets": [
                    {"key": "Drama", "doc_count": 450},
                    {"key": "Comedy", "doc_count": 380},
                    {"key": "Action", "doc_count": 320},
                    {"key": "Sci-Fi", "doc_count": 180}
                ]
            }
        },
        "hits": {"hits": []}
    }
    return mock_es


@pytest.fixture
def mock_es_best_rated_genres(mock_es):
    """Mock ES pour la fonction best_rated_genres."""
    mock_es.search.return_value = {
        "aggregations": {
            "genres": {
                "buckets": [
                    {"key": "Drama", "avg_rating": {"value": 6.68}, "doc_count": 150},
                    {"key": "Action", "avg_rating": {"value": 6.45}, "doc_count": 120},
                    {"key": "Comedy", "avg_rating": {"value": 6.10}, "doc_count": 140},
                    {"key": "Sci-Fi", "avg_rating": {"value": 7.20}, "doc_count": 85}
                ]
            }
        },
        "hits": {"hits": []}
    }
    return mock_es

