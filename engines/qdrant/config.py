import os
import uuid

QDRANT_HOST = os.getenv("AIPS_QDRANT_HOST") or "aips-qdrant"
QDRANT_PORT = int(os.getenv("AIPS_QDRANT_PORT") or "6333")
QDRANT_URL = f"http://{QDRANT_HOST}:{QDRANT_PORT}"
QDRANT_GRPC_PORT = int(os.getenv("AIPS_QDRANT_GRPC_PORT") or "6334")

# Dummy dense vector used for collections that are payload/BM25 only.
DUMMY_VECTOR_NAME = "aips_dummy"
DUMMY_VECTOR_SIZE = 1
DEFAULT_BOOST_FIELD = "upc"
ID_NAMESPACE = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def point_id_from_value(value):
    """Map arbitrary document ids to a stable Qdrant point id (UUID)."""
    if value is None:
        return str(uuid.uuid4())
    if isinstance(value, int) and value >= 0:
        return value
    text = str(value)
    try:
        # Prefer numeric ids when they fit unsigned 64-bit range used by Qdrant.
        as_int = int(text)
        if as_int >= 0:
            return as_int
    except (TypeError, ValueError):
        pass
    return str(uuid.uuid5(ID_NAMESPACE, text))


def field(kind, **kwargs):
    """kind: text | keyword | integer | float | bool | geo"""
    return {"kind": kind, **kwargs}


def text_field(**kwargs):
    return field("text", searchable=True, **kwargs)


def keyword_field(**kwargs):
    return field("keyword", searchable=False, **kwargs)


def integer_field(**kwargs):
    return field("integer", **kwargs)


def float_field(**kwargs):
    return field("float", **kwargs)


def bool_field(**kwargs):
    return field("bool", **kwargs)


def geo_field(**kwargs):
    return field("geo", **kwargs)


def vector_field(size, distance="Cosine", **kwargs):
    return {"kind": "vector", "size": size, "distance": distance, **kwargs}


def basic_schema(fields, id_field="id", vector_fields=None, boost_field=None,
                  searchable_fields=None):
    schema = {
        "id_field": id_field,
        "fields": fields,
        "vector_fields": vector_fields or {},
        "boost_field": boost_field,
        "searchable_fields": searchable_fields,
    }
    if searchable_fields is None:
        schema["searchable_fields"] = [
            name for name, meta in fields.items() if meta.get("kind") == "text"
        ]
    return schema


def body_title_schema():
    return basic_schema({
        "post_type_id": text_field(),
        "accepted_answer_id": text_field(),
        "parent_id": text_field(),
        "creation_date": text_field(),
        "deletion_date": text_field(),
        "score": text_field(),
        "view_count": text_field(),
        "body": text_field(),
        "owner_user_id": text_field(),
        "owner_display_name": text_field(),
        "last_editor_user_id": text_field(),
        "last_editor_display_name": text_field(),
        "last_edit_date": text_field(),
        "last_activity_date": text_field(),
        "title": text_field(),
        "tags": text_field(),
        "answer_count": text_field(),
        "comment_count": text_field(),
        "favorite_count": text_field(),
        "closed_date": text_field(),
        "community_owned_date": text_field(),
        "category": text_field(),
    }, searchable_fields=["title", "body", "tags"])


def signals_boosting_schema():
    # Aggregate boost tables are small; BM25 on query matches notebook searches.
    return basic_schema({
        "query": text_field(),
        "doc": keyword_field(),
        "boost": integer_field(),
    }, searchable_fields=["query"])


PRODUCTS_FIELDS = {
    "upc": text_field(),
    "name": text_field(),
    "name_fuzzy": text_field(),
    "short_description": text_field(),
    "long_description": text_field(),
    "manufacturer": text_field(),
    "has_promotion": text_field(),
    "_text_": text_field(),
}

PRODUCTS_SCHEMA = basic_schema(
    PRODUCTS_FIELDS,
    id_field="upc",
    boost_field="upc",
    searchable_fields=["name", "short_description", "long_description",
                       "manufacturer", "upc", "name_fuzzy", "_text_"],
)

PRODUCT_BOOSTS_SCHEMA = basic_schema(
    {**PRODUCTS_FIELDS, "signals_boosts": text_field()},
    id_field="upc",
    boost_field="upc",
    searchable_fields=["name", "short_description", "long_description",
                       "manufacturer", "upc", "signals_boosts", "_text_"],
)

PRODUCT_PROMOS_SCHEMA = basic_schema(
    PRODUCTS_FIELDS,
    id_field="upc",
    boost_field="upc",
    searchable_fields=["name", "short_description", "long_description",
                       "manufacturer", "upc", "_text_"],
)

SCHEMAS = {
    "cat_in_the_hat": basic_schema({
        "id": text_field(),
        "title": text_field(),
        "description": text_field(),
        "_text_": text_field(),
    }, id_field="id", searchable_fields=["title", "description", "_text_"]),
    "products": PRODUCTS_SCHEMA,
    "products_with_signals_boosts": PRODUCT_BOOSTS_SCHEMA,
    "products_with_promotions": PRODUCT_PROMOS_SCHEMA,
    "jobs": basic_schema({
        "job_description": text_field(),
        "company_description": text_field(),
        "company_country": text_field(),
        "_text_": text_field(),
    }, searchable_fields=["job_description", "company_description",
                          "company_country", "_text_"]),
    # Large collections: prefer keyword fields; avoid BM25 on every column.
    "signals": basic_schema({
        "query_id": keyword_field(),
        "user": keyword_field(),
        "type": keyword_field(),
        "target": keyword_field(),
        "signal_time": keyword_field(),
        "query": keyword_field(),
    }, searchable_fields=[]),
    "signals_boosting": basic_schema({
        # query stays text+BM25 so notebook searches like query="ipad" still rank.
        "query": text_field(),
        "doc": keyword_field(),
        "boost": integer_field(),
    }, searchable_fields=["query"]),
    "basic_signals_boosts": signals_boosting_schema(),
    "normalized_signals_boosts": signals_boosting_schema(),
    "signals_boosts_with_spam": signals_boosting_schema(),
    "signals_boosts_anti_spam": signals_boosting_schema(),
    "signals_boosts_weighted_types": signals_boosting_schema(),
    "signals_boosts_time_weighted": signals_boosting_schema(),
    "user_product_implicit_preferences": basic_schema({
        "user": text_field(),
        "product": text_field(),
        "rating": integer_field(),
    }),
    "user_item_recommendations": basic_schema({
        "user": text_field(),
        "product": text_field(),
        "boost": float_field(),
    }),
    "user_product_cluster": basic_schema({
        "user": text_field(),
        "product_id": text_field(),
        "product_name": text_field(),
        "label": text_field(),
    }, searchable_fields=["product_name", "label"]),
    "stackexchange": body_title_schema(),
    "health": body_title_schema(),
    "cooking": body_title_schema(),
    "scifi": body_title_schema(),
    "travel": body_title_schema(),
    "devops": body_title_schema(),
    "reviews": basic_schema({
        "id": text_field(),
        "business_name": text_field(),
        "name": text_field(),
        "city": text_field(),
        "state": text_field(),
        "content": text_field(),
        "categories": text_field(),
        "doc_type": text_field(),
        "stars_rating": integer_field(),
        "location_coordinates": geo_field(),
        "_text_": text_field(),
    }, id_field="id", searchable_fields=["content", "business_name", "name",
                                         "city", "categories", "_text_"]),
    "entities": basic_schema({
        "id": text_field(),
        "surface_form": text_field(),
        "canonical_form": text_field(),
        "type": text_field(),
        "popularity": integer_field(),
        "semantic_function": text_field(),
        "location_coordinates": text_field(),
        "country": text_field(),
        "admin_area": text_field(),
        "name": text_field(),
    }, id_field="id", searchable_fields=["surface_form", "canonical_form", "name"]),
    "tmdb": basic_schema({
        "id": text_field(),
        "title": text_field(),
        "overview": text_field(),
        "tagline": text_field(),
        "directors": text_field(),
        "cast": text_field(),
        "genres": text_field(),
        "release_date": text_field(),
        "release_year": text_field(),
        "poster_file": text_field(),
        "poster_path": text_field(),
        "vote_average": float_field(),
        "vote_count": integer_field(),
        "movie_image_ids": text_field(),
        "_text_": text_field(),
    }, id_field="id", searchable_fields=["title", "overview", "tagline", "cast",
                                         "genres", "_text_"]),
    "outdoors": basic_schema({
        "id": text_field(),
        "accepted_answer_id": integer_field(),
        "parent_id": text_field(),
        "creation_date": text_field(),
        "score": integer_field(),
        "view_count": integer_field(),
        "body": text_field(),
        "owner_user_id": text_field(),
        "title": text_field(),
        "tags": text_field(),
        "answer_count": integer_field(),
        "post_type": text_field(),
        "url": text_field(),
        "_text_": text_field(),
    }, id_field="id", searchable_fields=["title", "body", "tags", "_text_"]),
    "tmdb_with_embeddings": basic_schema(
        {
            "title": text_field(),
            "movie_id": text_field(),
            "image_id": text_field(),
            "_text_": text_field(),
        },
        id_field="image_id",
        vector_fields={"image_embedding": vector_field(512, distance="Dot")},
        searchable_fields=["title", "_text_"],
    ),
    "tmdb_lexical_plus_embeddings": basic_schema(
        {
            "id": text_field(),
            "movie_id": text_field(),
            "title": text_field(),
            "overview": text_field(),
            "image_id": text_field(),
            "_text_": text_field(),
        },
        id_field="id",
        vector_fields={"image_embedding": vector_field(512, distance="Dot")},
        searchable_fields=["title", "overview", "_text_"],
    ),
    "outdoors_with_embeddings": basic_schema(
        {
            "id": text_field(),
            "title": text_field(),
            "_text_": text_field(),
        },
        id_field="id",
        vector_fields={"title_embedding": vector_field(768, distance="Dot")},
        searchable_fields=["title", "_text_"],
    ),
    "outdoors_for_rag": basic_schema(
        {
            "id": text_field(),
            "title": text_field(),
            "body": text_field(),
            "_text_": text_field(),
        },
        id_field="id",
        vector_fields={"title_embedding": vector_field(768, distance="Dot")},
        searchable_fields=["title", "body", "_text_"],
    ),
    "outdoors_quantization": basic_schema(
        {
            "id": text_field(),
            "title": text_field(),
            "body": text_field(),
            "_text_": text_field(),
        },
        id_field="id",
        vector_fields={
            "text_embedding": vector_field(1024, distance="Dot"),
            "binary_text_embedding": vector_field(1024, distance="Euclid"),
        },
        searchable_fields=["title", "body", "_text_"],
    ),
}


def get_schema(name):
    return SCHEMAS.get(name.lower(), basic_schema({"id": text_field(), "_text_": text_field()}))


def get_vector_field_names(collection_name):
    return list(get_schema(collection_name).get("vector_fields", {}).keys())


def get_boost_field(collection_name):
    return get_schema(collection_name).get("boost_field", DEFAULT_BOOST_FIELD)


def get_id_field(collection_name):
    return get_schema(collection_name).get("id_field", "id")
