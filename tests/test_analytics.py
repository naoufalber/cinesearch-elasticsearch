"""
Tests unitaires pour analytics.py
Tous les tests utilisent des mocks Elasticsearch, sans connexion réelle.
"""
import pytest
from unittest.mock import MagicMock, call
import sys
from pathlib import Path

# Permettre l'importation depuis src
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analytics import (
    global_stats,
    top_genres,
    top_directors,
    top_actors,
    movies_by_decade,
    average_rating_by_year,
    best_rated_genres,
    best_rated_directors
)
from config import INDEX_NAME


# =============================================================================
# TESTS: global_stats
# =============================================================================

class TestGlobalStats:
    """Tests pour la fonction global_stats."""

    def test_global_stats_with_complete_data(self, mock_es_global_stats, capsys):
        """
        Arrange: Mock ES avec statistiques complètes
        Act: Appel global_stats
        Assert: Retourne un dictionnaire avec toutes les info et 3 appels ES
        """
        # Arrange
        # Act
        result = global_stats(mock_es_global_stats)

        # Assert
        # Vérifier que es.search a été appelé 3 fois (aggs + best + worst) et es.count une fois
        assert mock_es_global_stats.search.call_count == 3
        assert mock_es_global_stats.count.call_count == 1

        # Vérifier le résultat
        aggs = result["aggregations"]
        assert aggs["avg_rating"]["value"] == 7.42
        assert aggs["min_rating"]["value"] == 3.0
        assert aggs["max_rating"]["value"] == 9.0

    def test_global_stats_first_call_with_aggs(self, mock_es_global_stats):
        """
        Arrange: Mock ES
        Act: Appel global_stats
        Assert: Vérifie le premier appel avec agrégations
        """
        # Arrange
        # Act
        global_stats(mock_es_global_stats)

        # Assert
        first_call = mock_es_global_stats.search.call_args_list[0]
        call_kwargs = first_call[1]

        assert call_kwargs["size"] == 0  # Pas de hits, juste aggs
        assert "aggs" in call_kwargs
        assert "avg_rating" in call_kwargs["aggs"]
        assert "min_rating" in call_kwargs["aggs"]
        assert "max_rating" in call_kwargs["aggs"]

    def test_global_stats_second_call_best_movie(self, mock_es_global_stats):
        """
        Arrange: Mock ES
        Act: Appel global_stats
        Assert: Vérifie le deuxième appel pour le meilleur film
        """
        # Arrange
        # Act
        global_stats(mock_es_global_stats)

        # Assert
        second_call = mock_es_global_stats.search.call_args_list[1]
        call_kwargs = second_call[1]

        assert call_kwargs["size"] == 1
        assert call_kwargs["sort"] == [{"rating": {"order": "desc"}}]

    def test_global_stats_third_call_worst_movie(self, mock_es_global_stats):
        """
        Arrange: Mock ES
        Act: Appel global_stats
        Assert: Vérifie le troisième appel pour le pire film
        """
        # Arrange
        # Act
        global_stats(mock_es_global_stats)

        # Assert
        third_call = mock_es_global_stats.search.call_args_list[2]
        call_kwargs = third_call[1]

        assert call_kwargs["size"] == 1
        assert call_kwargs["sort"] == [{"rating": {"order": "asc"}}]

    def test_global_stats_count_call(self, mock_es_global_stats):
        """
        Arrange: Mock ES
        Act: Appel global_stats
        Assert: Vérifie l'appel à es.count
        """
        # Arrange
        # Act
        global_stats(mock_es_global_stats)

        # Assert
        mock_es_global_stats.count.assert_called_once()
        call_kwargs = mock_es_global_stats.count.call_args[1]
        assert call_kwargs["index"] == INDEX_NAME


# =============================================================================
# TESTS: top_genres
# =============================================================================

class TestTopGenres:
    """Tests pour la fonction top_genres."""

    def test_top_genres_with_results(self, mock_es_top_genres, capsys):
        """
        Arrange: Mock ES avec genres
        Act: Appel top_genres
        Assert: Retourne les agrégations avec les buckets
        """
        # Arrange
        # Act
        result = top_genres(mock_es_top_genres)

        # Assert
        assert "aggregations" in result
        assert "genres" in result["aggregations"]
        buckets = result["aggregations"]["genres"]["buckets"]
        assert len(buckets) == 4
        assert buckets[0]["key"] == "Drama"
        assert buckets[0]["doc_count"] == 450

    def test_top_genres_agg_config(self, mock_es_top_genres):
        """
        Arrange: Mock ES
        Act: Appel top_genres avec size custom
        Assert: Vérifie la configuration d'agrégation
        """
        # Arrange
        # Act
        top_genres(mock_es_top_genres, size=5)

        # Assert
        call_kwargs = mock_es_top_genres.search.call_args[1]
        aggs_config = call_kwargs["aggs"]["genres"]
        assert aggs_config["terms"]["field"] == "genres"
        assert aggs_config["terms"]["size"] == 5

    def test_top_genres_size_parameter(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel top_genres avec size=20
        Assert: Vérifie que size=20 est passé
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"genres": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        top_genres(mock_es, size=20)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        assert call_kwargs["aggs"]["genres"]["terms"]["size"] == 20


# =============================================================================
# TESTS: top_directors
# =============================================================================

class TestTopDirectors:
    """Tests pour la fonction top_directors."""

    def test_top_directors_with_results(self, mock_es):
        """
        Arrange: Mock ES avec réalisateurs
        Act: Appel top_directors
        Assert: Retourne les buckets des réalisateurs
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "directors": {
                    "buckets": [
                        {"key": "Christopher Nolan", "doc_count": 5},
                        {"key": "Quentin Tarantino", "doc_count": 4},
                        {"key": "Steven Spielberg", "doc_count": 6}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = top_directors(mock_es)

        # Assert
        buckets = result["aggregations"]["directors"]["buckets"]
        assert len(buckets) == 3
        assert buckets[0]["key"] == "Christopher Nolan"

    def test_top_directors_uses_keyword_field(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel top_directors
        Assert: Vérifie l'utilisation de directors.keyword
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"directors": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        top_directors(mock_es)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        field = call_kwargs["aggs"]["directors"]["terms"]["field"]
        assert field == "directors.keyword"


# =============================================================================
# TESTS: top_actors
# =============================================================================

class TestTopActors:
    """Tests pour la fonction top_actors."""

    def test_top_actors_with_results(self, mock_es):
        """
        Arrange: Mock ES avec acteurs
        Act: Appel top_actors
        Assert: Retourne les buckets des acteurs
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "actors": {
                    "buckets": [
                        {"key": "Leonardo DiCaprio", "doc_count": 10},
                        {"key": "Tom Hanks", "doc_count": 8}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = top_actors(mock_es)

        # Assert
        buckets = result["aggregations"]["actors"]["buckets"]
        assert len(buckets) == 2

    def test_top_actors_uses_keyword_field(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel top_actors
        Assert: Vérifie l'utilisation de actors.keyword
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"actors": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        top_actors(mock_es)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        field = call_kwargs["aggs"]["actors"]["terms"]["field"]
        assert field == "actors.keyword"


# =============================================================================
# TESTS: movies_by_decade
# =============================================================================

class TestMoviesByDecade:
    """Tests pour la fonction movies_by_decade."""

    def test_movies_by_decade_with_results(self, mock_es):
        """
        Arrange: Mock ES avec distribution par décennies
        Act: Appel movies_by_decade
        Assert: Retourne les buckets des décennies
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "decades": {
                    "buckets": [
                        {"key": 1990, "doc_count": 50},
                        {"key": 2000, "doc_count": 150},
                        {"key": 2010, "doc_count": 200},
                        {"key": 2020, "doc_count": 100}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = movies_by_decade(mock_es)

        # Assert
        buckets = result["aggregations"]["decades"]["buckets"]
        assert len(buckets) == 4
        assert buckets[2]["key"] == 2010
        assert buckets[2]["doc_count"] == 200

    def test_movies_by_decade_histogram_config(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel movies_by_decade
        Assert: Vérifie la configuration d'histogramme
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"decades": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        movies_by_decade(mock_es)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        histogram_config = call_kwargs["aggs"]["decades"]["histogram"]
        assert histogram_config["field"] == "year"
        assert histogram_config["interval"] == 10
        assert histogram_config["min_doc_count"] == 1


# =============================================================================
# TESTS: average_rating_by_year
# =============================================================================

class TestAverageRatingByYear:
    """Tests pour la fonction average_rating_by_year."""

    def test_average_rating_by_year_with_results(self, mock_es):
        """
        Arrange: Mock ES avec moyennes par année
        Act: Appel average_rating_by_year
        Assert: Retourne les buckets avec les moyennes
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "years": {
                    "buckets": [
                        {
                            "key": 2010,
                            "doc_count": 5,
                            "avg_rating": {"value": 7.5},
                            "movie_count": {"value": 5}
                        },
                        {
                            "key": 2015,
                            "doc_count": 8,
                            "avg_rating": {"value": 7.2},
                            "movie_count": {"value": 8}
                        }
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = average_rating_by_year(mock_es)

        # Assert
        buckets = result["aggregations"]["years"]["buckets"]
        assert len(buckets) == 2
        assert buckets[0]["avg_rating"]["value"] == 7.5

    def test_average_rating_by_year_nested_aggs(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel average_rating_by_year
        Assert: Vérifie les agrégations imbriquées
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"years": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        average_rating_by_year(mock_es)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        nested_aggs = call_kwargs["aggs"]["years"]["aggs"]
        assert "avg_rating" in nested_aggs
        assert "movie_count" in nested_aggs


# =============================================================================
# TESTS: best_rated_genres
# =============================================================================

class TestBestRatedGenres:
    """Tests pour la fonction best_rated_genres."""

    def test_best_rated_genres_with_results(self, mock_es_best_rated_genres, capsys):
        """
        Arrange: Mock ES avec genres et leurs moyennes
        Act: Appel best_rated_genres
        Assert: Retourne les genres triés par note moyenne décroissante
        """
        # Arrange
        # Act
        result = best_rated_genres(mock_es_best_rated_genres)

        # Assert
        buckets = result["aggregations"]["genres"]["buckets"]
        assert len(buckets) == 4
        # Vérifier le tri (trié automatiquement par Elasticsearch)
        assert buckets[0]["key"] == "Drama"
        assert buckets[0]["avg_rating"]["value"] == 6.68

    def test_best_rated_genres_agg_config(self, mock_es_best_rated_genres):
        """
        Arrange: Mock ES
        Act: Appel best_rated_genres avec size custom
        Assert: Vérifie la configuration d'agrégation avec nested avg_rating
        """
        # Arrange
        # Act
        best_rated_genres(mock_es_best_rated_genres, size=5)

        # Assert
        call_kwargs = mock_es_best_rated_genres.search.call_args[1]
        aggs = call_kwargs["aggs"]["genres"]

        assert aggs["terms"]["field"] == "genres"
        assert aggs["terms"]["size"] == 5
        assert "avg_rating" in aggs["terms"]["order"]
        assert aggs["terms"]["order"]["avg_rating"] == "desc"

    def test_best_rated_genres_nested_avg_rating(self, mock_es_best_rated_genres):
        """
        Arrange: Mock ES
        Act: Appel best_rated_genres
        Assert: Vérifie l'agrégation imbriquée avg_rating
        """
        # Arrange
        # Act
        best_rated_genres(mock_es_best_rated_genres)

        # Assert
        call_kwargs = mock_es_best_rated_genres.search.call_args[1]
        nested_aggs = call_kwargs["aggs"]["genres"]["aggs"]

        assert "avg_rating" in nested_aggs
        assert nested_aggs["avg_rating"]["avg"]["field"] == "rating"


# =============================================================================
# TESTS: best_rated_directors
# =============================================================================

class TestBestRatedDirectors:
    """Tests pour la fonction best_rated_directors."""

    def test_best_rated_directors_with_min_films(self, mock_es):
        """
        Arrange: Mock ES avec réalisateurs et leurs moyennes
        Act: Appel best_rated_directors avec min_films=3
        Assert: Retourne les réalisateurs triés par note moyenne
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "directors": {
                    "buckets": [
                        {"key": "Christopher Nolan", "doc_count": 5, "avg_rating": {"value": 8.1}},
                        {"key": "Quentin Tarantino", "doc_count": 8, "avg_rating": {"value": 7.8}},
                        {"key": "Steven Spielberg", "doc_count": 3, "avg_rating": {"value": 7.6}}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = best_rated_directors(mock_es, min_films=3)

        # Assert
        buckets = result["aggregations"]["directors"]["buckets"]
        assert len(buckets) == 3
        # Vérifier triés par note moyenne
        assert buckets[0]["avg_rating"]["value"] >= buckets[1]["avg_rating"]["value"]

    def test_best_rated_directors_bucket_selector(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel best_rated_directors avec min_films=3
        Assert: Vérifie que bucket_selector est utilisé
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"directors": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        best_rated_directors(mock_es, min_films=3)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        nested_aggs = call_kwargs["aggs"]["directors"]["aggs"]

        assert "keep_only_directors_with_enough_movies" in nested_aggs
        selector = nested_aggs["keep_only_directors_with_enough_movies"]
        assert "bucket_selector" in selector
        assert ">= 3" in selector["bucket_selector"]["script"]

    def test_best_rated_directors_min_films_validation(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel best_rated_directors avec min_films=0 (invalide)
        Assert: Verify min_films est défini par défaut à 3
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"directors": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        best_rated_directors(mock_es, min_films=0)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        script = call_kwargs["aggs"]["directors"]["aggs"]["keep_only_directors_with_enough_movies"]["bucket_selector"]["script"]
        # Le script devrait utiliser la valeur par défaut 3
        assert ">= 3" in script

    def test_best_rated_directors_no_results_message(self, mock_es, capsys):
        """
        Arrange: Mock ES sans résultats
        Act: Appel best_rated_directors
        Assert: Affiche un message "Aucun réalisateur trouvé..."
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {"directors": {"buckets": []}},
            "hits": {"hits": []}
        }

        # Act
        result = best_rated_directors(mock_es, min_films=100)

        # Assert
        captured = capsys.readouterr()
        assert "Aucun réalisateur trouvé" in captured.out or "Essaie une valeur plus petite" in captured.out

    def test_best_rated_directors_sorting(self, mock_es, capsys):
        """
        Arrange: Mock ES avec réalisateurs possédant des notes inégales
        Act: Appel best_rated_directors
        Assert: Vérifie que la fonction affiche les résultats triés par note décroissante
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "directors": {
                    "buckets": [
                        {"key": "Director A", "doc_count": 5, "avg_rating": {"value": 7.0}},
                        {"key": "Director B", "doc_count": 6, "avg_rating": {"value": 8.5}},
                        {"key": "Director C", "doc_count": 4, "avg_rating": {"value": 6.5}}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = best_rated_directors(mock_es, min_films=3)

        # Assert - La fonction trie les buckets avant impression
        captured = capsys.readouterr()
        # Vérifier que Director B (8.50) est affiché avant Director A (7.00)
        assert "Director B: 8.50" in captured.out
        assert "Director A: 7.00" in captured.out
        assert captured.out.index("8.50") < captured.out.index("7.00")

    def test_best_rated_directors_with_valid_ratings(self, mock_es):
        """
        Arrange: Mock ES avec réalisateurs ayant des notes valides
        Act: Appel best_rated_directors
        Assert: Retourne correctement les résultats triés
        """
        # Arrange
        mock_es.search.return_value = {
            "aggregations": {
                "directors": {
                    "buckets": [
                        {"key": "Director A", "doc_count": 5, "avg_rating": {"value": 7.5}},
                        {"key": "Director B", "doc_count": 6, "avg_rating": {"value": 8.2}}
                    ]
                }
            },
            "hits": {"hits": []}
        }

        # Act
        result = best_rated_directors(mock_es, min_films=3)

        # Assert
        buckets = result["aggregations"]["directors"]["buckets"]
        assert len(buckets) == 2
        # Vérifier que Director B est bien présent avec sa note
        assert any(b["key"] == "Director B" and b["avg_rating"]["value"] == 8.2 for b in buckets)

