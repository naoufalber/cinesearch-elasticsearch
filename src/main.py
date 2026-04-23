from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError, NotFoundError
from tabulate import tabulate

from config import get_es_client
from search import (
    search_by_title,
    search_advanced,
    search_plot,
    search_fuzzy,
    suggest_titles,
)
from analytics import (
    global_stats,
    top_genres,
    top_directors,
    top_actors,
    movies_by_decade,
    average_rating_by_year,
    best_rated_genres,
    best_rated_directors,
)


def print_menu() -> None:
    print("\n" + "╔" + "═" * 42 + "╗")
    print("║         CinéSearch — Menu Principal      ║")
    print("╠" + "═" * 42 + "╣")
    print("║  1. Recherche par titre                  ║")
    print("║  2. Recherche avancée (multi-critères)   ║")
    print("║  3. Recherche dans le synopsis           ║")
    print("║  4. Recherche floue (tolérance erreurs)  ║")
    print("║  5. Auto-complétion de titre             ║")
    print("║  6. Statistiques globales                ║")
    print("║  7. Top réalisateurs / acteurs / genres  ║")
    print("║  8. Analyses avancées                    ║")
    print("║  9. Recommandations de films similaire   ║")
    print("║  0. Quitter                              ║")
    print("╚" + "═" * 42 + "╝")


def prompt_optional_float(message: str):
    value = input(message).strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        print("Valeur invalide, champ ignoré.")
        return None


def prompt_optional_int(message: str):
    value = input(message).strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        print("Valeur invalide, champ ignoré.")
        return None


def display_search_table(results, show_score: bool = True) -> None:
    hits = results["hits"]["hits"]

    if not hits:
        print("Aucun résultat trouvé.")
        return

    rows = []
    for i, hit in enumerate(hits, start=1):
        src = hit["_source"]
        row = [
            i,
            src.get("title", "N/A"),
            src.get("year", "N/A"),
            src.get("rating", "N/A"),
            ", ".join(src.get("directors", [])),
        ]
        if show_score:
            row.append(round(hit.get("_score", 0), 3) if hit.get("_score") is not None else None)
        rows.append(row)

    headers = ["#", "Titre", "Année", "Note", "Réalisateur"]
    if show_score:
        headers.append("Score ES")

    print(tabulate(rows, headers=headers, tablefmt="grid"))


def handle_search_by_title(es: Elasticsearch) -> None:
    query = input("Titre à rechercher : ").strip()
    if not query:
        print("Recherche annulée.")
        return

    results = search_by_title(es, query)
    print("\nAffichage tabulaire :")
    display_search_table(results, show_score=True)


def handle_advanced_search(es: Elasticsearch) -> None:
    print("\nLaisse vide un champ que tu ne veux pas utiliser.")
    title = input("Titre : ").strip() or None
    actor = input("Acteur : ").strip() or None
    director = input("Réalisateur : ").strip() or None
    genre = input("Genre exact (ex: Drama) : ").strip() or None
    min_rating = prompt_optional_float("Note minimale : ")
    max_rating = prompt_optional_float("Note maximale : ")
    year_from = prompt_optional_int("Année minimum : ")
    year_to = prompt_optional_int("Année maximum : ")

    results = search_advanced(
        es,
        title=title,
        actor=actor,
        director=director,
        genre=genre,
        min_rating=min_rating,
        max_rating=max_rating,
        year_from=year_from,
        year_to=year_to,
    )

    print("\nAffichage tabulaire :")
    display_search_table(results, show_score=True)


def handle_plot_search(es: Elasticsearch) -> None:
    keywords = input("Mots-clés à chercher dans le synopsis : ").strip()
    if not keywords:
        print("Recherche annulée.")
        return

    search_plot(es, keywords)


def handle_fuzzy_search(es: Elasticsearch) -> None:
    query = input("Titre approximatif : ").strip()
    if not query:
        print("Recherche annulée.")
        return

    fuzziness = prompt_optional_int("Fuzziness (défaut = 2) : ")
    if fuzziness is None:
        fuzziness = 2

    results = search_fuzzy(es, query, fuzziness=fuzziness)
    print("\nAffichage tabulaire :")
    display_search_table(results, show_score=True)


def handle_suggestions(es: Elasticsearch) -> None:
    prefix = input("Préfixe du titre : ").strip()
    if not prefix:
        print("Recherche annulée.")
        return

    suggest_titles(es, prefix)


def handle_global_stats(es: Elasticsearch) -> None:
    global_stats(es)


def handle_top_lists(es: Elasticsearch) -> None:
    print("\n1. Top genres")
    print("2. Top réalisateurs")
    print("3. Top acteurs")
    print("4. Films par décennie")

    choice = input("Choix : ").strip()

    if choice == "1":
        top_genres(es)
    elif choice == "2":
        top_directors(es)
    elif choice == "3":
        top_actors(es)
    elif choice == "4":
        movies_by_decade(es)
    else:
        print("Choix invalide.")


def handle_advanced_analytics(es: Elasticsearch) -> None:
    print("\n1. Note moyenne par année")
    print("2. Genres les mieux notés")
    print("3. Réalisateurs les mieux notés")

    choice = input("Choix : ").strip()

    if choice == "1":
        average_rating_by_year(es)

    elif choice == "2":
        best_rated_genres(es)

    elif choice == "3":
        print("Le sujet recommande un minimum de 3 films.")
        min_films = prompt_optional_int("Nombre minimum de films [Entrée = 3] : ")

        if min_films is None:
            min_films = 3

        if min_films > 50:
            print("Valeur très élevée pour ce dataset.")
            print("Je remets la valeur à 3 pour garder un résultat exploitable.")
            min_films = 3

        best_rated_directors(es, min_films=min_films)

    else:
        print("Choix invalide.")


def main() -> None:
    print("Bienvenue dans CinéSearch")

    try:
        es = get_es_client()
        info = es.info()
        print(f"Connecté à Elasticsearch {info['version']['number']}")
    except ESConnectionError:
        print("Impossible de se connecter à Elasticsearch.")
        print("Vérifie que Docker est lancé et que la stack tourne.")
        return
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return

    while True:
        try:
            print_menu()
            choice = input("Ton choix : ").strip()

            if choice == "1":
                handle_search_by_title(es)
            elif choice == "2":
                handle_advanced_search(es)
            elif choice == "3":
                handle_plot_search(es)
            elif choice == "4":
                handle_fuzzy_search(es)
            elif choice == "5":
                handle_suggestions(es)
            elif choice == "6":
                handle_global_stats(es)
            elif choice == "7":
                handle_top_lists(es)
            elif choice == "8":
                handle_advanced_analytics(es)
            elif choice == "9":
                title = input("Titre du film de référence : ").strip()
                if not title:
                    print("Veuillez saisir un titre.")
                else:
                    from search import recommend_similar_movies, print_recommendations
                    base_movie, recommendations = recommend_similar_movies(es, title)
                    print_recommendations(base_movie, recommendations)
            elif choice == "0":
                print("Au revoir !")
                break
                break
            else:
                print("Choix invalide. Merci de sélectionner une option du menu.")

        except NotFoundError:
            print("Index introuvable. Vérifie que l'indexation a bien été faite.")
        except KeyboardInterrupt:
            print("\nInterruption utilisateur. Fermeture de CinéSearch.")
            break
        except Exception as e:
            print(f"Une erreur est survenue : {e}")


if __name__ == "__main__":
    main()