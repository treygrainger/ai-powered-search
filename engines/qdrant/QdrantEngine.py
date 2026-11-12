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

    def _text_index_schema(self, bm25=True, on_disk=True):
        """Full-text payload index; BM25 on 1.20+ with on-disk index to save RAM."""
        scoring = None
        if bm25:
            try:
                scoring = models.TextScoringParams(type=models.TextScoringType.BM25)
            except Exception:
                scoring = True
        try:
            return models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                lowercase=True,
                scoring=scoring,
                on_disk=on_disk,
            )
        except TypeError:
            try:
                return models.TextIndexParams(
                    type="text",
                    tokenizer=models.TokenizerType.WORD,
                    lowercase=True,
                    scoring=True if bm25 else None,
                    on_disk=on_disk,
                )
            except Exception:
                return models.PayloadSchemaType.TEXT

    def _build_vectors_config(self, schema):
        # on_disk=True keeps dense/dummy vectors mmap'd instead of fully resident in RAM.
        vector_fields = schema.get("vector_fields") or {}
        if not vector_fields:
            return {
                DUMMY_VECTOR_NAME: models.VectorParams(
                    size=DUMMY_VECTOR_SIZE,
                    distance=models.Distance.COSINE,
                    on_disk=True,
                )
            }
        config = {}
        for name, meta in vector_fields.items():
            distance = DISTANCE_MAP.get(meta.get("distance", "Cosine"), models.Distance.COSINE)
            kwargs = {"size": meta["size"], "distance": distance, "on_disk": True}
            config[name] = models.VectorParams(**kwargs)
        return config

    def _is_payload_only(self, schema):
        """True when schema has no real dense vectors (dummy vector only)."""
        return not (schema.get("vector_fields") or {})

    def _hnsw_config(self, payload_only=False):
        # Payload-only labs: disable HNSW entirely (m=0) — dummy vectors are never searched.
        # Real vector collections: smaller on-disk graph for lab machines.
        try:
            if payload_only:
                return models.HnswConfigDiff(m=0, ef_construct=8, on_disk=True)
            return models.HnswConfigDiff(m=16, ef_construct=64, on_disk=True)
        except TypeError:
            if payload_only:
                return models.HnswConfigDiff(m=0)
            return models.HnswConfigDiff(m=16, ef_construct=64)

    def _optimizers_config(self, payload_only=False):
        try:
            # Huge indexing_threshold on payload-only avoids building vector indexes for 2M+ dummies.
            return models.OptimizersConfigDiff(
                memmap_threshold=10000,
                indexing_threshold=10**9 if payload_only else 20000,
            )
        except TypeError:
            return None

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
            payload_only = self._is_payload_only(schema)
            vectors_config = self._build_vectors_config(schema)
            create_kwargs = {
                "collection_name": collection_name,
                "vectors_config": vectors_config,
                "hnsw_config": self._hnsw_config(payload_only=payload_only),
            }
            opt = self._optimizers_config(payload_only=payload_only)
            if opt is not None:
                create_kwargs["optimizers_config"] = opt
            # Prefer mmap'd payload for large signal tables.
            try:
                create_kwargs["on_disk_payload"] = True
            except Exception:
                pass
            self.client.create_collection(**create_kwargs)

            searchable = set(schema.get("searchable_fields") or [])
            # Optional explicit filter indexes; default = searchable only.
            # Empty searchable_fields (e.g. signals) => no payload indexes (saves multi-GB RAM).
            indexed = set(schema.get("indexed_fields") or searchable)
            for field_name, meta in schema.get("fields", {}).items():
                kind = meta.get("kind")
                if kind == "vector":
                    continue
                if field_name not in searchable and field_name not in indexed:
                    continue
                try:
                    if kind == "text" and field_name in searchable:
                        field_schema = self._text_index_schema(bm25=True, on_disk=True)
                    elif kind == "text":
                        field_schema = models.PayloadSchemaType.KEYWORD
                    else:
                        field_schema = self._payload_schema_type(kind)
                    self.client.create_payload_index(
                        collection_name=collection_name,
                        field_name=field_name,
                        field_schema=field_schema,
                    )
                except Exception as ex:
                    if log:
                        print(f"Index create skipped/failed for {field_name}: {ex}")
            if log:
                print(
                    f"Created Qdrant collection {collection_name} "
                    f"vectors={list(vectors_config)} payload_only={payload_only} "
                    f"indexed={sorted(indexed)}"
                )
        else:
            print(f'Using existing "{collection_name}" collection')

        collection = self.get_collection(collection_name)
        self.print_status(True)
        return collection

    def get_collection(self, name):
        return QdrantCollection(name.lower(), client=self.client)
