import os
import copy
from datetime import date

AIPS_ES_HOST = os.getenv("AIPS_ES_HOST") or "aips-elasticsearch"
AIPS_ES_PORT = os.getenv("AIPS_ES_PORT") or "9200"
ES_URL = f"http://{AIPS_ES_HOST}:{AIPS_ES_PORT}"
STATUS_URL = f"{ES_URL}/_cluster/health"


def base_field(type, **kwargs):
    return {"type": type, "store": True} | kwargs


def text_field(**kwargs):
    return base_field("text", **kwargs)


def boolean_field():
    return base_field("boolean")


def double_field():
    return base_field("double")


def integer_field():
    return base_field("integer")


def keyword_field(**kwargs):
    return base_field("keyword", **kwargs)


def date_field():
    return base_field("date")


def basic_schema(field_mappings, id_field="_id"):
    return {
        "id_field": id_field,
        "schema": {"mappings": {"properties": field_mappings}},
    }


def body_title_schema():
    return basic_schema({"title": text_field(), "body": text_field()})


def dense_vector_schema(field_name, dimensions, similarity_score, additional_fields={}):
    similarity = "cosine" if similarity_score == "innerproduct" else "l2_norm"
    schema = {
        "schema": {
            "mappings": {
                "properties": {
                    field_name: {
                        "type": "dense_vector",
                        "dims": dimensions,
                        "index": True,
                        "similarity": similarity,
                    }
                }
            },
        }
    }
    schema["schema"]["mappings"]["properties"].update(additional_fields)
    return schema


PRODUCTS_SCHEMA = basic_schema(
    {
        "upc": text_field(fielddata=True),
        "_text_": text_field(),
        "name_ngram": text_field(analyzer="bigram_analyzer", fielddata=True),
        "name_fuzzy": text_field(analyzer="shingle_analyzer", fielddata=True),
        "short_description_ngram": text_field(analyzer="bigram_analyzer"),
        "name": text_field(copy_to=["name_ngram", "_text_", "name_fuzzy"], fielddata=True),
        "short_description": text_field(copy_to=["short_description_ngram", "_text_"]),
        "long_description": text_field(copy_to="_text_"),
        "manufacturer": text_field(copy_to="_text_"),
        "has_promotion": boolean_field(),
    },
    "upc",
)
PRODUCTS_SCHEMA["schema"]["settings"] = {
    "index": {"mapping": {"total_fields": {"limit": 100000}}},
    "analysis": {
        "filter": {"edge_ngram_filter": {"type": "edge_ngram", "min_gram": 1, "max_gram": 20}},
        "analyzer": {
            "bigram_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "edge_ngram_filter"],
            },
            "shingle_analyzer": {"tokenizer": "standard", "filter": ["lowercase", "shingle"]},
        },
    },
}

PRODUCT_BOOSTS_SCHEMA = copy.deepcopy(PRODUCTS_SCHEMA)
PRODUCT_BOOSTS_SCHEMA["schema"]["mappings"]["properties"]["signals_boosts"] = {
    "type": "rank_features"
}

SIGNALS_BOOSTING_SCHEMA = basic_schema(
    {"query": keyword_field(), "doc": text_field(), "boost": integer_field()}
)

SCHEMAS = {
    "cat_in_the_hat": basic_schema(
        {
            "id": text_field(),
            "title": text_field(),
            "description": text_field() | {"similarity": "BM25", "discount_overlaps": False},
        },
        "id",
    ),
    "products": PRODUCTS_SCHEMA,
    "products_with_signals_boosts": PRODUCT_BOOSTS_SCHEMA,
    "jobs": basic_schema(
        {
            "company_country": text_field(),
            "job_description": text_field(),
            "company_description": text_field(),
        }
    ),
    "signals": basic_schema(
        {
            "query": text_field(),
            "user": text_field(),
            "type": text_field(),
            "target": text_field(),
            "signal_time": date_field(),
        }
    ),
    "signals_boosting": SIGNALS_BOOSTING_SCHEMA,
    "signals_boosts_with_spam": SIGNALS_BOOSTING_SCHEMA,
    "signals_boosts_anti_spam": SIGNALS_BOOSTING_SCHEMA,
    "signals_boosts_weighted_types": SIGNALS_BOOSTING_SCHEMA,
    "signals_boosts_time_weighted": SIGNALS_BOOSTING_SCHEMA,
    "stackexchange": body_title_schema(),
    "health": body_title_schema(),
    "cooking": body_title_schema(),
    "scifi": body_title_schema(),
    "travel": body_title_schema(),
    "devops": body_title_schema(),
    "reviews": basic_schema(
        {
            "id": text_field(),
            "content": text_field(fielddata=True),
            "categories": text_field(copy_to="doc_type", fielddata=True),
            "doc_type": text_field(fielddata=True),
            "stars_rating": integer_field(),
            "city": text_field(fielddata=True),
            "state": text_field(fielddata=True),
            "business_name": text_field(fielddata=True),
            "name": text_field(fielddata=True),
            "location_coordinates": base_field("geo_point"),
        }
    ),
    "tmdb": basic_schema(
        {
            "title": text_field(),
            "overview": text_field(),
            "release_year": double_field(),
        }
    ),
    "outdoors": basic_schema(
        {
            "id": text_field(),
            "post_type": text_field(),
            "accepted_answer_id": integer_field(),
            "parent_id": integer_field(),
            "creation_date": text_field(),
            "score": integer_field(),
            "view_count": integer_field(),
            "body": text_field(fielddata=True),
            "owner_user_id": text_field(),
            "title": text_field(fielddata=True),
            "tags": keyword_field(),
            "url": text_field(),
            "answer_count": integer_field(),
        },
        "id",
    ),
    "tmdb_with_embeddings": dense_vector_schema(
        "image_embedding",
        512,
        "innerproduct",
        {"title": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "tmdb_lexical_plus_embeddings": dense_vector_schema(
        "image_embedding",
        512,
        "innerproduct",
        {
            "title": text_field(),
            "overview": text_field(),
            "movie_id": text_field(),
            "image_id": text_field(),
        },
    ),
    "outdoors_with_embeddings": dense_vector_schema(
        "title_embedding",
        768,
        "innerproduct",
        {"title": text_field()},
    ),
    "outdoors_quantization": dense_vector_schema(
        "text_embedding",
        1024,
        "innerproduct",
        {"title": text_field(), "body": text_field()},
    ),
    "entities": basic_schema(
        {
            "surface_form": keyword_field(),
            "canonical_form": keyword_field(),
            "admin_area": keyword_field(),
            "country": keyword_field(),
            "name": text_field(),
            "popularity": integer_field(),
        }
    ),
    "ubi_queries": basic_schema(
        {
            "timestamp": date_field(),
            "query_id": keyword_field(),
            "client_id": text_field(),
        }
    ),
    "ubi_aips_events": basic_schema(
        {
            "application": text_field(),
            "action_name": text_field(),
            "query_id": keyword_field(),
            "client_id": text_field(),
            "timestamp": date_field(),
            "message_type": text_field(),
            "message": text_field(),
        }
    ),
}
