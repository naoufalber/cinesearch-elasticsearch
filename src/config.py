from pathlib import Path
from elasticsearch import Elasticsearch

ES_URL = "http://localhost:9200"
INDEX_NAME = "movies"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "movies_cleaned_v2.json"


def get_es_client() -> Elasticsearch:
    return Elasticsearch(ES_URL)