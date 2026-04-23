"""
Tests unitaires pour search.py
Tous les tests utilisent des mocks Elasticsearch, sans connexion réelle.
"""
import pytest
from unittest.mock import MagicMock, call
import sys
from pathlib import Path

# Permettre l'importation depuis src
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from search import (
    search_by_title,
    search_advanced,
    search_plot,
    search_fuzzy,
    suggest_titles,
    recommend_similar_movies,
    format_movie_result
)
from config import INDEX_NAME


# =============================================================================
# TESTS: search_by_title
# =============================================================================

class TestSearchByTitle:
    """Tests pour la fonction search_by_title."""

    def test_search_by_title_with_results(self, mock_es_with_inception, capsys):
        """
        Arrange: Mock ES avec un film trouvé (Inception)
        Act: Appel search_by_title avec "Inception"
        Assert: Vérifie que es.search() est appelé correctement et retourne le bon résultat
        """
        # Arrange
        query_text = "Inception"

        # Act
        result = search_by_title(mock_es_with_inception, query_text)

        # Assert
        assert result["hits"]["total"]["value"] == 1
        assert len(result["hits"]["hits"]) == 1
        assert result["hits"]["hits"][0]["_source"]["title"] == "Inception"

        # Vérifier l'appel à es.search
        mock_es_with_inception.search.assert_called_once()
        call_kwargs = mock_es_with_inception.search.call_args[1]
        assert call_kwargs["index"] == INDEX_NAME
        assert "match" in call_kwargs["query"]
        assert call_kwargs["query"]["match"]["title"] == query_text

    def test_search_by_title_no_results(self, mock_es_empty_results, capsys):
        """
        Arrange: Mock ES avec aucun résultat
        Act: Appel search_by_title avec une requête vide
        Assert: Vérifie que la fonction retourne une liste vide
        """
        # Arrange
        query_text = "NonExistentMovie12345"

        # Act
        result = search_by_title(mock_es_empty_results, query_text)

        # Assert
        assert result["hits"]["total"]["value"] == 0
        assert len(result["hits"]["hits"]) == 0

    def test_search_by_title_with_custom_index(self, mock_es):
        """
        Arrange: Mock ES et index personnalisé
        Act: Appel search_by_title avec un index custom
        Assert: Vérifie que l'index custom est utilisé
        """
        # Arrange
        mock_es.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}
        custom_index = "custom_movies"

        # Act
        search_by_title(mock_es, "test", index=custom_index)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        assert call_kwargs["index"] == custom_index

    def test_search_by_title_with_size_parameter(self, mock_es, capsys):
        """
        Arrange: Mock ES
        Act: Appel search_by_title (qui utilise size=10 par défaut)
        Assert: Vérifie que size=10 est passé à es.search
        """
        # Arrange
        mock_es.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

        # Act
        search_by_title(mock_es, "test")

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        assert call_kwargs.get("size") == 10


# =============================================================================
# TESTS: search_advanced
# =============================================================================

class TestSearchAdvanced:
    """Tests pour la fonction search_advanced."""

    def test_search_advanced_all_parameters(self, mock_es, capsys):
        """
        Arrange: Mock ES avec tous les paramètres remplis
        Act: Appel search_advanced avec titre, acteur, réalisateur, genre, ratings et years
        Assert: Vérifie que la requête bool contient les bons éléments
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(
            mock_es,
            title="Inception",
            actor="Leonardo DiCaprio",
            director="Christopher Nolan",
            genre="Sci-Fi",
            min_rating=8.0,
            max_rating=10.0,
            year_from=2000,
            year_to=2020
        )

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]

        # Vérifier la structure bool
        assert "bool" in query
        assert "must" in query["bool"]
        assert "filter" in query["bool"]

        # Vérifier les critères de texte (must)
        must_clauses = query["bool"]["must"]
        assert any(c.get("match", {}).get("title") == "Inception" for c in must_clauses)
        assert any(c.get("match", {}).get("actors") == "Leonardo DiCaprio" for c in must_clauses)
        assert any(c.get("match", {}).get("directors") == "Christopher Nolan" for c in must_clauses)

        # Vérifier les filtres
        filters = query["bool"]["filter"]
        assert any(c.get("term", {}).get("genres") == "Sci-Fi" for c in filters)

        # Vérifier rating range
        rating_filter = next((c for c in filters if "range" in c and "rating" in c["range"]), None)
        assert rating_filter is not None
        assert rating_filter["range"]["rating"]["gte"] == 8.0
        assert rating_filter["range"]["rating"]["lte"] == 10.0

        # Vérifier year range
        year_filter = next((c for c in filters if "range" in c and "year" in c["range"]), None)
        assert year_filter is not None
        assert year_filter["range"]["year"]["gte"] == 2000
        assert year_filter["range"]["year"]["lte"] == 2020

    def test_search_advanced_only_title(self, mock_es, capsys):
        """
        Arrange: Mock ES avec seulement le paramètre titre
        Act: Appel search_advanced avec uniquement title
        Assert: Vérifie que seul le critère titre est dans must
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(mock_es, title="Inception")

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        must_clauses = query["bool"]["must"]

        assert len(must_clauses) == 1
        assert must_clauses[0]["match"]["title"] == "Inception"
        assert len(query["bool"]["filter"]) == 0

    def test_search_advanced_rating_only(self, mock_es, capsys):
        """
        Arrange: Mock ES avec seulement les paramètres de rating
        Act: Appel search_advanced avec min_rating et max_rating
        Assert: Vérifie que le filtre rating contient les deux limites
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(mock_es, min_rating=7.0, max_rating=9.0)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        filters = query["bool"]["filter"]

        rating_filter = next((c for c in filters if "range" in c and "rating" in c["range"]), None)
        assert rating_filter is not None
        assert rating_filter["range"]["rating"]["gte"] == 7.0
        assert rating_filter["range"]["rating"]["lte"] == 9.0

    def test_search_advanced_year_range_only(self, mock_es, capsys):
        """
        Arrange: Mock ES avec seulement les paramètres d'année
        Act: Appel search_advanced avec year_from et year_to
        Assert: Vérifie que le filtre year est bien construit
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(mock_es, year_from=2000, year_to=2015)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        filters = query["bool"]["filter"]

        year_filter = next((c for c in filters if "range" in c and "year" in c["range"]), None)
        assert year_filter is not None
        assert year_filter["range"]["year"]["gte"] == 2000
        assert year_filter["range"]["year"]["lte"] == 2015

    def test_search_advanced_no_parameters(self, mock_es, capsys):
        """
        Arrange: Mock ES sans paramètres optionnels
        Act: Appel search_advanced sans aucun critère
        Assert: Vérifie que la requête bool reste valide (must et filter vides)
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(mock_es)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        assert "bool" in query
        assert query["bool"]["must"] == []
        assert query["bool"]["filter"] == []

    def test_search_advanced_genre_term_filter(self, mock_es, capsys):
        """
        Arrange: Mock ES avec genre
        Act: Appel search_advanced avec genre
        Assert: Vérifie que le genre utilise term, pas match
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {"total": {"value": 1}, "hits": []}
        }

        # Act
        result = search_advanced(mock_es, genre="Action")

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        filters = query["bool"]["filter"]

        genre_filter = next((c for c in filters if "term" in c), None)
        assert genre_filter is not None
        assert genre_filter["term"]["genres"] == "Action"


# =============================================================================
# TESTS: search_plot
# =============================================================================

class TestSearchPlot:
    """Tests pour la fonction search_plot."""

    def test_search_plot_with_results_and_highlight(self, mock_es_with_highlight, capsys):
        """
        Arrange: Mock ES avec résultats et highlight
        Act: Appel search_plot
        Assert: Vérifie la requête match sur plot et le highlight
        """
        # Arrange
        keywords = "dream"

        # Act
        result = search_plot(mock_es_with_highlight, keywords)

        # Assert
        assert result["hits"]["total"]["value"] == 1
        movie = result["hits"]["hits"][0]
        assert "highlight" in movie
        assert "plot" in movie["highlight"]

        # Vérifier l'appel à es.search
        call_kwargs = mock_es_with_highlight.search.call_args[1]
        assert call_kwargs["query"]["match"]["plot"] == keywords
        assert "highlight" in call_kwargs
        assert "plot" in call_kwargs["highlight"]["fields"]

    def test_search_plot_highlight_config(self, mock_es_with_highlight):
        """
        Arrange: Mock ES
        Act: Appel search_plot
        Assert: Vérifie la configuration du highlight (balises, fragment_size)
        """
        # Arrange
        mock_es = mock_es_with_highlight

        # Act
        search_plot(mock_es, "test")

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        highlight_config = call_kwargs["highlight"]["fields"]["plot"]
        assert highlight_config["pre_tags"] == ["**"]
        assert highlight_config["post_tags"] == ["**"]
        assert highlight_config["fragment_size"] == 150
        assert highlight_config["number_of_fragments"] == 3

    def test_search_plot_no_results(self, mock_es_empty_results, capsys):
        """
        Arrange: Mock ES sans résultats
        Act: Appel search_plot
        Assert: Vérifie que la fonction gère les résultats vides
        """
        # Arrange
        # Act
        result = search_plot(mock_es_empty_results, "nonexistent")

        # Assert
        assert result["hits"]["total"]["value"] == 0
        assert len(result["hits"]["hits"]) == 0


# =============================================================================
# TESTS: search_fuzzy
# =============================================================================

class TestSearchFuzzy:
    """Tests pour la fonction search_fuzzy."""

    def test_search_fuzzy_with_default_fuzziness(self, mock_es_with_inception, capsys):
        """
        Arrange: Mock ES avec Inception, fuzziness par défaut
        Act: Appel search_fuzzy avec fuzziness par défaut (2)
        Assert: Vérifie que la requête fuzzy utilise fuzziness=2
        """
        # Arrange
        query_text = "Incepion"  # Typo intentionnel

        # Act
        result = search_fuzzy(mock_es_with_inception, query_text)

        # Assert
        call_kwargs = mock_es_with_inception.search.call_args[1]
        query = call_kwargs["query"]
        match_title = query["match"]["title"]

        assert match_title["query"] == query_text
        assert match_title["fuzziness"] == 2

    def test_search_fuzzy_with_custom_fuzziness(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel search_fuzzy avec fuzziness=1
        Assert: Vérifie que fuzziness=1 est utilisé
        """
        # Arrange
        mock_es.search.return_value = {"hits": {"total": {"value": 1}, "hits": []}}

        # Act
        search_fuzzy(mock_es, "test", fuzziness=1)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        assert query["match"]["title"]["fuzziness"] == 1

    def test_search_fuzzy_no_results(self, mock_es_empty_results, capsys):
        """
        Arrange: Mock ES sans résultats
        Act: Appel search_fuzzy
        Assert: Vérifie que la fonction retourne une liste vide correctement
        """
        # Arrange
        # Act
        result = search_fuzzy(mock_es_empty_results, "xyz")

        # Assert
        assert result["hits"]["total"]["value"] == 0


# =============================================================================
# TESTS: suggest_titles
# =============================================================================

class TestSuggestTitles:
    """Tests pour la fonction suggest_titles (autocomplétion)."""

    def test_suggest_titles_with_results(self, mock_es, capsys):
        """
        Arrange: Mock ES avec plusieurs suggestions
        Act: Appel suggest_titles
        Assert: Retourne une liste de titres
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {
                "total": {"value": 3},
                "hits": [
                    {"_source": {"title": "Star Wars"}},
                    {"_source": {"title": "Stardust"}},
                    {"_source": {"title": "Star Trek"}}
                ]
            }
        }

        # Act
        result = suggest_titles(mock_es, "Star")

        # Assert
        assert len(result) == 3
        assert "Star Wars" in result
        assert "Stardust" in result
        assert "Star Trek" in result

    def test_suggest_titles_uses_match_phrase_prefix(self, mock_es):
        """
        Arrange: Mock ES
        Act: Appel suggest_titles
        Assert: Vérifie l'utilisation de match_phrase_prefix
        """
        # Arrange
        prefix = "Incp"
        mock_es.search.return_value = {"hits": {"total": {"value": 0}, "hits": []}}

        # Act
        suggest_titles(mock_es, prefix)

        # Assert
        call_kwargs = mock_es.search.call_args[1]
        query = call_kwargs["query"]
        assert "match_phrase_prefix" in query
        assert query["match_phrase_prefix"]["title"] == prefix

    def test_suggest_titles_no_results(self, mock_es_empty_results, capsys):
        """
        Arrange: Mock ES sans résultats
        Act: Appel suggest_titles
        Assert: Retourne une liste vide
        """
        # Arrange
        # Act
        result = suggest_titles(mock_es_empty_results, "xyz")

        # Assert
        assert result == []

    def test_suggest_titles_deduplication(self, mock_es, capsys):
        """
        Arrange: Mock ES avec doublons dans les résultats
        Act: Appel suggest_titles
        Assert: Vérifie que les doublons sont supprimés
        """
        # Arrange
        mock_es.search.return_value = {
            "hits": {
                "total": {"value": 3},
                "hits": [
                    {"_source": {"title": "Inception"}},
                    {"_source": {"title": "Inception"}},  # Doublon
                    {"_source": {"title": "Interstellar"}}
                ]
            }
        }

        # Act
        result = suggest_titles(mock_es, "In")

        # Assert
        assert len(result) == 2
        assert result.count("Inception") == 1


# =============================================================================
# TESTS: recommend_similar_movies
# =============================================================================

class TestRecommendSimilarMovies:
    """Tests pour la fonction recommend_similar_movies."""

    def test_recommend_similar_movies_base_movie_found(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES qui retrouve le film de base et ses recommandations
        Act: Appel recommend_similar_movies
        Assert: Vérifie que la fonction retourne le film de base et les recommandations
        """
        # Arrange
        movie_title = "Inception"

        # Act
        base_movie, recommendations = recommend_similar_movies(
            mock_es_for_recommendations,
            movie_title
        )

        # Assert
        assert base_movie is not None
        assert base_movie["title"] == "Inception"
        assert len(recommendations) == 2
        assert recommendations[0]["_source"]["title"] == "Interstellar"
        assert recommendations[1]["_source"]["title"] == "The Dark Knight"

    def test_recommend_similar_movies_base_movie_not_found(self, mock_es_empty_results):
        """
        Arrange: Mock ES sans résultats pour le film de base
        Act: Appel recommend_similar_movies
        Assert: Retourne (None, [])
        """
        # Arrange
        movie_title = "NonExistentMovie"

        # Act
        base_movie, recommendations = recommend_similar_movies(
            mock_es_empty_results,
            movie_title
        )

        # Assert
        assert base_movie is None
        assert recommendations == []

    def test_recommend_similar_movies_two_search_calls(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES avec side_effect pour deux appels
        Act: Appel recommend_similar_movies
        Assert: Vérifie que es.search est appelé 2 fois
        """
        # Arrange - mock_es_for_recommendations est déjà configuré avec 2 appels

        # Act
        recommend_similar_movies(mock_es_for_recommendations, "Inception")

        # Assert
        assert mock_es_for_recommendations.search.call_count == 2

    def test_recommend_similar_movies_excludes_base_movie(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES
        Act: Appel recommend_similar_movies
        Assert: Vérifie que la deuxième requête exclut le film de base via must_not
        """
        # Arrange
        movie_id = "id_inception"

        # Act
        recommend_similar_movies(mock_es_for_recommendations, "Inception")

        # Assert - Vérifier le deuxième appel
        second_call = mock_es_for_recommendations.search.call_args_list[1]
        rec_query = second_call[1]["body"]
        bool_query = rec_query["query"]["bool"]

        # Vérifier la clause must_not
        assert "must_not" in bool_query
        assert "ids" in bool_query["must_not"][0]
        assert movie_id in bool_query["must_not"][0]["ids"]["values"]

    def test_recommend_similar_movies_uses_more_like_this(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES
        Act: Appel recommend_similar_movies
        Assert: Vérifie que la deuxième requête utilise more_like_this sur title et plot
        """
        # Arrange
        # mock_es_for_recommendations est déjà configuré

        # Act
        recommend_similar_movies(mock_es_for_recommendations, "Inception")

        # Assert
        second_call = mock_es_for_recommendations.search.call_args_list[1]
        rec_query = second_call[1]["body"]
        should_clauses = rec_query["query"]["bool"]["should"]

        # Vérifier la présence de more_like_this
        mlt_clause = next((c for c in should_clauses if "more_like_this" in c), None)
        assert mlt_clause is not None
        assert "title" in mlt_clause["more_like_this"]["fields"]
        assert "plot" in mlt_clause["more_like_this"]["fields"]

    def test_recommend_similar_movies_boosts_genres(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES
        Act: Appel recommend_similar_movies avec film contenant genres
        Assert: Vérifie que les genres sont boostés dans should
        """
        # Arrange - mock_es_for_recommendations est déjà configuré

        # Act
        recommend_similar_movies(mock_es_for_recommendations, "Inception")

        # Assert
        second_call = mock_es_for_recommendations.search.call_args_list[1]
        rec_query = second_call[1]["body"]
        should_clauses = rec_query["query"]["bool"]["should"]

        # Vérifier les boosted genre clauses
        genre_clauses = [c for c in should_clauses if "term" in c and "genres" in c.get("term", {})]
        assert len(genre_clauses) > 0

        # Vérifier le boost
        for clause in genre_clauses:
            boost = clause["term"]["genres"].get("boost", 1.0)
            assert boost >= 1.0

    def test_recommend_similar_movies_custom_size(self, mock_es_for_recommendations):
        """
        Arrange: Mock ES
        Act: Appel recommend_similar_movies avec size custom
        Assert: Vérifie que size est utilisé dans la deuxième requête
        """
        # Arrange - mock_es_for_recommendations est déjà configuré

        # Act
        recommend_similar_movies(mock_es_for_recommendations, "Inception", size=10)

        # Assert
        second_call = mock_es_for_recommendations.search.call_args_list[1]
        rec_query = second_call[1]["body"]
        assert rec_query["size"] == 10


# =============================================================================
# TESTS: format_movie_result (fonction utilitaire)
# =============================================================================

class TestFormatMovieResult:
    """Tests pour la fonction format_movie_result."""

    def test_format_movie_result_all_fields(self):
        """
        Arrange: Hit complet avec tous les champs
        Act: Appel format_movie_result
        Assert: Retourne un dictionnaire bien formaté
        """
        # Arrange
        hit = {
            "_id": "id_inception",
            "_score": 8.8,
            "_source": {
                "title": "Inception",
                "year": 2010,
                "rating": 8.8,
                "plot": "A thief who steals corporate secrets...",
                "directors": ["Christopher Nolan"],
                "actors": ["Leonardo DiCaprio"],
                "genres": ["Action", "Sci-Fi", "Thriller"]
            }
        }

        # Act
        result = format_movie_result(hit)

        # Assert
        assert result["title"] == "Inception"
        assert result["year"] == 2010
        assert result["rating"] == 8.8
        assert "Christopher Nolan" in result["directors"]
        assert result["score"] == 8.8

    def test_format_movie_result_missing_fields(self):
        """
        Arrange: Hit avec certains champs manquants
        Act: Appel format_movie_result
        Assert: Utilise des valeurs par défaut "N/A"
        """
        # Arrange
        hit = {
            "_score": 5.0,
            "_source": {
                "title": "Test Movie"
                # Autres champs omis
            }
        }

        # Act
        result = format_movie_result(hit)

        # Assert
        assert result["title"] == "Test Movie"
        assert result["year"] == "N/A"
        assert result["rating"] == "N/A"
        assert result["directors"] == ""
        assert result["score"] == 5.0

    def test_format_movie_result_none_score(self):
        """
        Arrange: Hit without _score
        Act: Appel format_movie_result
        Assert: score est None
        """
        # Arrange
        hit = {
            "_source": {
                "title": "Test"
            }
        }

        # Act
        result = format_movie_result(hit)

        # Assert
        assert result["score"] is None

