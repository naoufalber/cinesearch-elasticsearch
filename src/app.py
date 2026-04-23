import streamlit as st
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError, NotFoundError

from search import recommend_similar_movies

# ============================================================
# Configuration
# ============================================================
ES_URL = "http://localhost:9200"
INDEX_NAME = "movies"
PAGE_SIZE = 10

st.set_page_config(
    page_title="CinéSearch",
    page_icon="🎬",
    layout="wide"
)

# ============================================================
# Connexion Elasticsearch
# ============================================================
@st.cache_resource
def get_es_client():
    return Elasticsearch(ES_URL)

def check_es():
    try:
        es = get_es_client()
        if not es.ping():
            return False, "Elasticsearch ne répond pas."
        if not es.indices.exists(index=INDEX_NAME):
            return False, f"L'index '{INDEX_NAME}' n'existe pas."
        return True, "Connexion OK"
    except ESConnectionError:
        return False, "Impossible de se connecter à Elasticsearch."
    except Exception as e:
        return False, f"Erreur : {e}"

# ============================================================
# Helpers d'affichage
# ============================================================
def movie_to_row(hit):
    src = hit.get("_source", {})
    return {
        "Titre": src.get("title", "N/A"),
        "Année": src.get("year", "N/A"),
        "Note": src.get("rating", "N/A"),
        "Réalisateur(s)": ", ".join(src.get("directors", [])) if isinstance(src.get("directors"), list) else src.get("directors", "N/A"),
        "Acteurs": ", ".join(src.get("actors", [])) if isinstance(src.get("actors"), list) else src.get("actors", "N/A"),
        "Genres": ", ".join(src.get("genres", [])) if isinstance(src.get("genres"), list) else src.get("genres", "N/A"),
        "Score": round(hit.get("_score", 0), 3) if hit.get("_score") is not None else None,
        "Synopsis": src.get("plot", ""),
        "Affiche": src.get("image_url", "")
    }

def display_movies(hits, show_plot=False, show_image=False, show_score=True):
    if not hits:
        st.warning("Aucun résultat trouvé.")
        return

    rows = []
    for hit in hits:
        row = movie_to_row(hit)
        if not show_score:
            row.pop("Score", None)
        if not show_plot:
            row.pop("Synopsis", None)
        if not show_image:
            row.pop("Affiche", None)
        rows.append(row)

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)

def display_movie_cards(hits, highlight_field=None):
    if not hits:
        st.warning("Aucun résultat trouvé.")
        return

    for hit in hits:
        src = hit.get("_source", {})
        title = src.get("title", "Titre inconnu")
        year = src.get("year", "N/A")
        rating = src.get("rating", "N/A")
        directors = ", ".join(src.get("directors", [])) if isinstance(src.get("directors"), list) else src.get("directors", "N/A")
        genres = ", ".join(src.get("genres", [])) if isinstance(src.get("genres"), list) else src.get("genres", "N/A")
        actors = ", ".join(src.get("actors", [])) if isinstance(src.get("actors"), list) else src.get("actors", "N/A")
        image_url = src.get("image_url")
        plot = src.get("plot", "")

        with st.container(border=True):
            col1, col2 = st.columns([1, 4])

            with col1:
                if image_url:
                    st.image(image_url, use_container_width=True)
                else:
                    st.markdown("🎬")

            with col2:
                st.subheader(f"{title} ({year})")
                st.write(f"**Note :** {rating}")
                st.write(f"**Réalisateur(s) :** {directors}")
                st.write(f"**Genres :** {genres}")
                st.write(f"**Acteurs :** {actors}")

                if highlight_field and "highlight" in hit and highlight_field in hit["highlight"]:
                    st.write("**Extrait pertinent :**")
                    for fragment in hit["highlight"][highlight_field]:
                        st.markdown(f"... {fragment} ...", unsafe_allow_html=True)
                else:
                    if plot:
                        st.write(f"**Synopsis :** {plot}")

                if hit.get("_score") is not None:
                    st.caption(f"Score Elasticsearch : {round(hit['_score'], 3)}")

# ============================================================
# Fonctions de recherche
# ============================================================
def search_by_title(es, query, size=PAGE_SIZE):
    body = {
        "query": {
            "match": {
                "title": query
            }
        }
    }
    return es.search(index=INDEX_NAME, **body, size=size)

def search_advanced(es, title=None, actor=None, director=None,
                    genre=None, min_rating=None, max_rating=None,
                    year_from=None, year_to=None, size=PAGE_SIZE):
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
            "must": must if must else [{"match_all": {}}],
            "filter": filters
        }
    }

    return es.search(index=INDEX_NAME, query=query, size=size)

def search_plot(es, keywords, size=PAGE_SIZE):
    query = {
        "match": {
            "plot": keywords
        }
    }

    highlight = {
        "fields": {
            "plot": {
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
                "fragment_size": 150,
                "number_of_fragments": 3
            }
        }
    }

    return es.search(index=INDEX_NAME, query=query, highlight=highlight, size=size)

def search_fuzzy(es, query_text, fuzziness="AUTO", size=PAGE_SIZE):
    query = {
        "match": {
            "title": {
                "query": query_text,
                "fuzziness": fuzziness
            }
        }
    }
    return es.search(index=INDEX_NAME, query=query, size=size)

def autocomplete_title(es, prefix, size=10):
    # Le sujet demande une auto-complétion de titre ; une approche prefix est cohérente ici. :contentReference[oaicite:2]{index=2}
    query = {
        "prefix": {
            "title.keyword": prefix
        }
    }

    # Si title.keyword n'existe pas, fallback vers match_phrase_prefix
    try:
        return es.search(index=INDEX_NAME, query=query, size=size)
    except Exception:
        fallback_query = {
            "match_phrase_prefix": {
                "title": prefix
            }
        }
        return es.search(index=INDEX_NAME, query=fallback_query, size=size)

# ============================================================
# Fonctions analytics
# ============================================================
def global_stats(es):
    aggs = {
        "avg_rating": {"avg": {"field": "rating"}},
        "min_rating": {"min": {"field": "rating"}},
        "max_rating": {"max": {"field": "rating"}}
    }

    result = es.search(index=INDEX_NAME, size=0, aggs=aggs)
    total = result["hits"]["total"]["value"]
    avg_rating = result["aggregations"]["avg_rating"]["value"]
    min_rating = result["aggregations"]["min_rating"]["value"]
    max_rating = result["aggregations"]["max_rating"]["value"]

    best_movie = es.search(
        index=INDEX_NAME,
        size=1,
        query={"exists": {"field": "rating"}},
        sort=[{"rating": {"order": "desc"}}]
    )

    worst_movie = es.search(
        index=INDEX_NAME,
        size=1,
        query={"exists": {"field": "rating"}},
        sort=[{"rating": {"order": "asc"}}]
    )

    return {
        "total": total,
        "avg_rating": avg_rating,
        "min_rating": min_rating,
        "max_rating": max_rating,
        "best_movie": best_movie["hits"]["hits"][0]["_source"] if best_movie["hits"]["hits"] else None,
        "worst_movie": worst_movie["hits"]["hits"][0]["_source"] if worst_movie["hits"]["hits"] else None,
    }

def terms_aggregation(es, field, agg_name, size=10):
    result = es.search(
        index=INDEX_NAME,
        size=0,
        aggs={
            agg_name: {
                "terms": {
                    "field": field,
                    "size": size
                }
            }
        }
    )
    return result["aggregations"][agg_name]["buckets"]

def avg_rating_by_genre(es, size=10):
    result = es.search(
        index=INDEX_NAME,
        size=0,
        aggs={
            "genres": {
                "terms": {
                    "field": "genres",
                    "size": size
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
    )

    buckets = result["aggregations"]["genres"]["buckets"]
    rows = []
    for bucket in buckets:
        rows.append({
            "Genre": bucket["key"],
            "Nb films": bucket["doc_count"],
            "Note moyenne": round(bucket["avg_rating"]["value"], 2) if bucket["avg_rating"]["value"] is not None else None
        })

    rows.sort(key=lambda x: (x["Note moyenne"] if x["Note moyenne"] is not None else 0), reverse=True)
    return rows

# ============================================================
# Interface
# ============================================================
st.title("🎬 CinéSearch")
st.caption("Moteur de recherche de films avec Elasticsearch + Streamlit")

ok, message = check_es()
if not ok:
    st.error(message)
    st.stop()
else:
    st.success(message)

es = get_es_client()

menu = st.sidebar.radio(
    "Navigation",
    [
        "Recherche par titre",
        "Recherche avancée",
        "Recherche dans le synopsis",
        "Recherche floue",
        "Auto-complétion",
        "Statistiques globales",
        "Top réalisateurs / acteurs / genres",
        "Recommandations",
    ]
)

# ------------------------------------------------------------
# 1. Recherche par titre
# ------------------------------------------------------------
if menu == "Recherche par titre":
    st.header("Recherche par titre")
    query = st.text_input("Titre du film")

    if st.button("Rechercher", key="title_search"):
        if not query.strip():
            st.warning("Veuillez saisir un titre.")
        else:
            results = search_by_title(es, query)
            hits = results["hits"]["hits"]
            st.write(f"**{len(hits)} résultat(s)**")
            display_movie_cards(hits)

# ------------------------------------------------------------
# 2. Recherche avancée
# ------------------------------------------------------------
elif menu == "Recherche avancée":
    st.header("Recherche avancée (multi-critères)")

    col1, col2 = st.columns(2)

    with col1:
        title = st.text_input("Titre")
        actor = st.text_input("Acteur")
        director = st.text_input("Réalisateur")
        genre = st.text_input("Genre exact (ex: Drama, Action, Comedy)")

    with col2:
        min_rating = st.number_input("Note minimale", min_value=0.0, max_value=10.0, value=0.0, step=0.1)
        max_rating = st.number_input("Note maximale", min_value=0.0, max_value=10.0, value=10.0, step=0.1)
        year_from = st.number_input("Année min", min_value=1900, max_value=2100, value=1900, step=1)
        year_to = st.number_input("Année max", min_value=1900, max_value=2100, value=2100, step=1)

    if st.button("Lancer la recherche avancée"):
        results = search_advanced(
            es,
            title=title or None,
            actor=actor or None,
            director=director or None,
            genre=genre or None,
            min_rating=min_rating if min_rating > 0 else None,
            max_rating=max_rating if max_rating < 10 else None,
            year_from=year_from if year_from > 1900 else None,
            year_to=year_to if year_to < 2100 else None,
        )
        hits = results["hits"]["hits"]
        st.write(f"**{len(hits)} résultat(s)**")
        display_movie_cards(hits)

# ------------------------------------------------------------
# 3. Recherche dans le synopsis
# ------------------------------------------------------------
elif menu == "Recherche dans le synopsis":
    st.header("Recherche full-text dans le synopsis")
    keywords = st.text_input("Mots-clés à chercher dans le synopsis")

    if st.button("Rechercher dans le synopsis"):
        if not keywords.strip():
            st.warning("Veuillez saisir un ou plusieurs mots-clés.")
        else:
            results = search_plot(es, keywords)
            hits = results["hits"]["hits"]
            st.write(f"**{len(hits)} résultat(s)**")
            display_movie_cards(hits, highlight_field="plot")

# ------------------------------------------------------------
# 4. Recherche floue
# ------------------------------------------------------------
elif menu == "Recherche floue":
    st.header("Recherche floue")
    query = st.text_input("Titre approximatif (ex: Incepion, Sttar Warss)")
    fuzziness = st.selectbox("Fuzziness", ["AUTO", "1", "2"])

    if st.button("Lancer la recherche floue"):
        if not query.strip():
            st.warning("Veuillez saisir un titre approximatif.")
        else:
            results = search_fuzzy(es, query, fuzziness=fuzziness)
            hits = results["hits"]["hits"]
            st.write(f"**{len(hits)} résultat(s)**")
            display_movie_cards(hits)

# ------------------------------------------------------------
# 5. Auto-complétion
# ------------------------------------------------------------
elif menu == "Auto-complétion":
    st.header("Auto-complétion de titre")
    prefix = st.text_input("Début du titre")

    if prefix.strip():
        results = autocomplete_title(es, prefix)
        hits = results["hits"]["hits"]

        suggestions = []
        for hit in hits:
            title = hit["_source"].get("title")
            year = hit["_source"].get("year")
            if title:
                suggestions.append(f"{title} ({year})")

        if suggestions:
            st.write("### Suggestions")
            for s in suggestions:
                st.write(f"- {s}")
        else:
            st.info("Aucune suggestion.")

# ------------------------------------------------------------
# 6. Statistiques globales
# ------------------------------------------------------------
elif menu == "Statistiques globales":
    st.header("Statistiques globales")

    stats = global_stats(es)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nombre total de films", stats["total"])
    c2.metric("Note moyenne", f"{stats['avg_rating']:.2f}" if stats["avg_rating"] is not None else "N/A")
    c3.metric("Note minimum", f"{stats['min_rating']:.2f}" if stats["min_rating"] is not None else "N/A")
    c4.metric("Note maximum", f"{stats['max_rating']:.2f}" if stats["max_rating"] is not None else "N/A")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Film le mieux noté")
        best = stats["best_movie"]
        if best:
            st.write(f"**Titre :** {best.get('title', 'N/A')}")
            st.write(f"**Note :** {best.get('rating', 'N/A')}")
            st.write(f"**Année :** {best.get('year', 'N/A')}")
            st.write(f"**Réalisateur(s) :** {', '.join(best.get('directors', [])) if isinstance(best.get('directors'), list) else best.get('directors', 'N/A')}")

    with col2:
        st.subheader("Film le moins bien noté")
        worst = stats["worst_movie"]
        if worst:
            st.write(f"**Titre :** {worst.get('title', 'N/A')}")
            st.write(f"**Note :** {worst.get('rating', 'N/A')}")
            st.write(f"**Année :** {worst.get('year', 'N/A')}")
            st.write(f"**Réalisateur(s) :** {', '.join(worst.get('directors', [])) if isinstance(worst.get('directors'), list) else worst.get('directors', 'N/A')}")

# ------------------------------------------------------------
# 7. Tops
# ------------------------------------------------------------
elif menu == "Top réalisateurs / acteurs / genres":
    st.header("Top réalisateurs / acteurs / genres")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Top genres",
        "Top réalisateurs",
        "Top acteurs",
        "Genres les mieux notés"
    ])

    with tab1:
        buckets = terms_aggregation(es, "genres", "genres", size=10)
        df = pd.DataFrame([{"Genre": b["key"], "Nombre de films": b["doc_count"]} for b in buckets])
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.bar_chart(df.set_index("Genre"))

    with tab2:
        # Le sujet attend un champ multi-field keyword pour directeurs/acteurs. :contentReference[oaicite:3]{index=3}
        buckets = terms_aggregation(es, "directors.keyword", "directors", size=10)
        df = pd.DataFrame([{"Réalisateur": b["key"], "Nombre de films": b["doc_count"]} for b in buckets])
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.bar_chart(df.set_index("Réalisateur"))

    with tab3:
        buckets = terms_aggregation(es, "actors.keyword", "actors", size=10)
        df = pd.DataFrame([{"Acteur": b["key"], "Nombre de films": b["doc_count"]} for b in buckets])
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            st.bar_chart(df.set_index("Acteur"))

    with tab4:
        rows = avg_rating_by_genre(es, size=10)
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True)
        if not df.empty:
            chart_df = df.set_index("Genre")[["Note moyenne"]]
            st.bar_chart(chart_df)

# ------------------------------------------------------------
# 8. Recommandations
# ------------------------------------------------------------
elif menu == "Recommandations":
    st.header("Recommandations de films similaires")

    movie_title = st.text_input("Titre du film de référence")

    if st.button("Trouver des films similaires"):
        if not movie_title.strip():
            st.warning("Veuillez saisir un titre.")
        else:
            base_movie, recommendations = recommend_similar_movies(es, movie_title)

            if base_movie is None:
                st.error("Film introuvable.")
            else:
                st.subheader(f"Film de référence : {base_movie.get('title', 'N/A')}")
                st.write(f"**Année :** {base_movie.get('year', 'N/A')}")
                st.write(f"**Note :** {base_movie.get('rating', 'N/A')}")
                st.write(f"**Genres :** {', '.join(base_movie.get('genres', []))}")
                st.write(f"**Réalisateur(s) :** {', '.join(base_movie.get('directors', [])) if isinstance(base_movie.get('directors'), list) else base_movie.get('directors', 'N/A')}")

                st.markdown("---")
                st.subheader("Films recommandés")

                if not recommendations:
                    st.info("Aucune recommandation trouvée.")
                else:
                    display_movie_cards(recommendations)