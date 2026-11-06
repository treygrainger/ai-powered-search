import time

import requests
from qdrant_client import QdrantClient, models

from engines.Engine import AdvancedFeatures, Engine
from engines.qdrant.config import (
    DUMMY_VECTOR_NAME,
    DUMMY_VECTOR_SIZE,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_URL,
    get_schema,
)
from engines.qdrant.QdrantCollection import QdrantCollection


DISTANCE_MAP = {
    "Cosine": models.Distance.COSINE,
    "Dot": models.Distance.DOT,
    "Euclid": models.Distance.EUCLID,
    "Euclidean": models.Distance.EUCLID,
    "Manhattan": models.Distance.MANHATTAN,
}


class QdrantEngine(Engine):
    def __init__(self, host=None):
        super().__init__("qdrant")
        self.host = host or QDRANT_HOST
        self.port = QDRANT_PORT
        self.base_url = f"http://{self.host}:{self.port}"
        self.client = QdrantClient(url=self.base_url, prefer_grpc=False, timeout=60)

    def get_supported_advanced_features(self):
        return [AdvancedFeatures.LTR]

    def health_check(self, log=False, retries=0):
        status = False
        for i in range(retries + 1):
            try:
                response = requests.get(f"{self.base_url}/readyz", timeout=5)
                status = response.status_code == 200
                if log:
                    print(f"Qdrant is {'online' if status else 'offline'}")
                if status:
                    break
                time.sleep(5)
            except Exception:
                if i == retries:
                    if log:
                        print("Qdrant failed the health check.")
                    status = False
                else:
                    time.sleep(5)
        return status

    def print_status(self, response):
        print(f"Status: {'Success' if response else 'Failure'}")

    def does_collection_exist(self, name):
        try:
            names = {c.name for c in self.client.get_collections().collections}
            return name.lower() in names or name in names
        except Exception:
            return False

    def _payload_schema_type(self, kind):
        mapping = {
            "text": models.PayloadSchemaType.TEXT,
            "keyword": models.PayloadSchemaType.KEYWORD,
            "integer": models.PayloadSchemaType.INTEGER,
            "float": models.PayloadSchemaType.FLOAT,
            "bool": models.PayloadSchemaType.BOOL,
            "geo": models.PayloadSchemaType.GEO,
        }
        return mapping.get(kind, models.PayloadSchemaType.KEYWORD)

    def _build_vectors_config(self, schema):
        vector_fields = schema.get("vector_fields") or {}
        if not vector_fields:
            return {
                DUMMY_VECTOR_NAME: models.VectorParams(
                    size=DUMMY_VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                )
            }
        config = {}
        for name, meta in vector_fields.items():
            distance = DISTANCE_MAP.get(meta.get("distance", "Cosine"), models.Distance.COSINE)
            config[name] = models.VectorParams(size=meta["size"], distance=distance)
        return config

    def create_collection(self, name, force_rebuild=True, log=False):
        collection_name = name.lower()
        schema = get_schema(collection_name)
        exists = self.does_collection_exist(collection_name)
        if force_rebuild and exists:
            print(f'Wiping "{collection_name}" collection')
            self.client.delete_collection(collection_name=collection_name)
            exists = False

        if not exists:
            print(f'Creating "{collection_name}" collection')
            vectors_config = self._build_vectors_config(schema)
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=vectors_config,
            )
            for field_name, meta in schema.get("fields", {}).items():
                kind = meta.get("kind")
                if kind == "vector":
                    continue
                try:
                    self.client.create_payload_index(
                        collection_name=collection_name,
                        field_name=field_name,
                        field_schema=self._payload_schema_type(kind),
                    )
                except Exception as ex:
                    if log:
                        print(f"Index create skipped/failed for {field_name}: {ex}")
            if log:
                print(f"Created Qdrant collection {collection_name} with vectors={list(vectors_config)}")
        else:
            print(f'Using existing "{collection_name}" collection')

        collection = self.get_collection(collection_name)
        self.print_status(True)
        return collection

    def get_collection(self, name):
        return QdrantCollection(name.lower(), client=self.client)
