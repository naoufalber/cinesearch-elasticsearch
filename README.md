
CinéSearch — mini moteur de recherche et d’analyse de films
===================================================================

Ce répertoire contient un projet complet pour mettre en place un petit moteur de recherche et d’analyse sur un jeu de données d’environ 5 000 films. L’objectif est de découvrir la suite Elastic (Elasticsearch et Kibana) avec Python et Docker, de créer un index bien typé, d’indexer les films, puis de proposer des recherches variées via une interface en ligne de commande et un tableau de bord dans Kibana.

Sommaire
--------

- [Structure du projet](#structure-du-projet)
- [Prérequis](#prérequis)
- [Installation et mise en place](#installation-et-mise-en-place)
- [Indexation des données](#indexation-des-données)
- [Lancer l’application en ligne de commande](#lancer-lapplication-en-ligne-de-commande)
- [Installation des dépendances pour les tests](#Installation-des-dépendances-pour-les-tests)


Structure du projet
-------------------

La structure du répertoire `cinesearch` est la suivante :

- **`docker-compose.yml`** – définition des services Docker pour lancer Elasticsearch et Kibana.
- **`requirements.txt`** – liste des dépendances Python à installer (client Elasticsearch, tabulate…).
- **`data/`** – contient le fichier `movies_cleaned_v2.json` (≈5000 films) utilisé pour l’indexation.
- **`src/`** – scripts Python du projet :
  - **`config.py`** – configuration centralisée (URL d’Elasticsearch, nom de l’index, chemin du dataset).
  - **`indexer.py`** – crée le mapping de l’index et indexe toutes les données en bulk.
  - **`search.py`** – contient les fonctions de recherche (simple, avancée, synopsis, floue, auto-complétion).
  - **`analytics.py`** – agrégations et statistiques (stats globales, tops, histogrammes, analyses avancées).
  - **`main.py`** – interface CLI proposant un menu interactif pour exploiter les fonctions de `search.py` et `analytics.py`.
- **`screenshots/`** – dossier contenant les captures d’écran Kibana.

Prérequis
-----------

Pour exécuter ce projet, assurez-vous d’avoir :

- **Python ≥ 3.10** et `pip` installés sur votre machine.
- **Docker** et **Docker Compose** installés et fonctionnels.
- Un terminal (bash ou PowerShell) pour exécuter les commandes.

Installation et mise en place
-----------------------------

1. **Récupérer le projet**
   Clonez ou téléchargez ce dépôt, puis placez-vous dans le dossier racine :

   ```bash
   cd cinesearch
   ```

2. **Créer un environnement virtuel et installer les dépendances Python**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Démarrer les services Elasticsearch et Kibana**

   Le fichier `docker-compose.yml` définit deux services : Elasticsearch (port 9200) et Kibana (port 5601).

   ```bash
   docker compose up -d
   ```

   Laissez quelques secondes/minutes pour le premier démarrage. Vous pouvez tester la connexion au cluster avec :

   ```bash
   curl http://localhost:9200
   ```

   Kibana est accessible depuis votre navigateur à `http://localhost:5601`.

4. **Configurer `config.py` si besoin**

   Ouvrez `src/config.py` et vérifiez les constantes :

   ```python
   ES_URL = "http://localhost:9200"
   INDEX_NAME = "movies"
   DATA_FILE = Path(__file__).resolve().parent.parent / "data" / "movies_cleaned_v2.json"
   ```

   Modifiez-les si vous changez de port, d’index ou de chemin de fichier.

Indexation des données
-------------------------

Avant de pouvoir faire des recherches, il faut créer un index avec le bon mapping et y insérer les films.

Depuis le dossier `src/`, lancez :

```bash
cd src
python indexer.py
```

Ce script supprime l’index existant s’il existe, crée un mapping explicite (champs `text`, `keyword`, `integer`, `float`, `date`, etc.), ajoute un champ `plot_tokens` pour le nuage de mots, puis indexe tous les documents via l’API bulk. Un résumé s’affiche : nombre de lignes lues, nombre de films indexés, temps d’exécution.

Lancer l’application en ligne de commande
-----------------------------------------

Une fois l’index créé et alimenté, vous pouvez utiliser l’interface CLI pour interroger et analyser les films.

Toujours dans le dossier `src/`, exécutez :

```bash
python main.py
```

Un menu s’affiche avec plusieurs options :

1. **Recherche par titre** – saisissez un titre (ou mot-clé) et obtenez les correspondances par pertinence.
2. **Recherche avancée** – filtrez par titre, acteur, réalisateur, genre, note mini / maxi et plage d’années.
3. **Recherche dans le synopsis** – recherchez des mots dans le champ `plot` avec surlignage des correspondances.
4. **Recherche floue** – tolérance aux fautes de frappe (fuzzy match). Exemple : « Incepion » renverra « Inception ».
5. **Auto-complétion** – propose des titres dès les premières lettres tapées.
6. **Statistiques globales** – nombre total de films, moyenne des notes, film le mieux et le moins bien noté.
7. **Top listes et histogrammes** – top genres, réalisateurs, acteurs, distribution par décennie.
8. **Analyses avancées** – évolution de la note moyenne par année, genres les mieux notés, réalisateurs les mieux notés (selon un minimum de films).
9. **Quitter**

Chaque recherche affiche les résultats au format tableau. Pour revenir au menu principal, il suffit de valider.


Interface Web (Streamlit)
--------------------------

En complément de l’interface en ligne de commande, une **interface web a été développée avec Streamlit**.

Elle permet d’utiliser toutes les fonctionnalités du moteur de recherche dans une interface graphique simple et interactive.

### Lancer l’application web

Avant de pouvoir lancer l'application Streamlit, assurez-vous d'avoir installé les dépendances nécessaires (voir la section "Installation et mise en place" ci-dessus).

Depuis le dossier `src/`, exécutez :

```bash
python -m streamlit run app.py
```





# Installation des dépendances pour les tests

### Option 1 : Installer les dépendances de test uniquement
```bash
pip install -r requirements-dev.txt
```

### Option 2 : Verifier l'installation
```bash
pip show pytest pytest-mock
```

---

## Pour lancer les tests

### Tous les tests avec verbose
```bash
pytest -v
```

### Seulement les tests de search.py
```bash
pytest tests/test_search.py -v
```

### Seulement les tests d'analytics.py
```bash
pytest tests/test_analytics.py -v
```
