import os
import copy

OPENSEARCH_HOST = os.getenv("AIPS_OPENSEARCH_HOST") or "aips-opensearch"
OPENSEARCH_PORT = os.getenv("AIPS_OPENSEARCH_PORT") or "9200"
OPENSEARCH_URL = f"http://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}"

def base_field(type, **kwargs):
    #"index": True
    field = {"type": type, "store": True}
    return field | kwargs

def text_field(**kwargs):
    return base_field("text", **kwargs)

def boolean_field():
    return base_field("boolean")

def double_field():
    return base_field("double")


def integer_field():
    return base_field("integer")


def keyword_field():
    return base_field("text")


# {"multiValued": "true", "docValues": "true"}


def string_field():
    return base_field("string")


def basic_schema(field_mappings, id_field="_id"):
    return {
        "id_field": id_field,
        "schema": {"mappings": {"properties": field_mappings}},
    }


def body_title_schema():
    return basic_schema({"title": text_field(), "body": text_field()})

def dense_vector_schema(
    field_name, dimensions, similarity_score, quantization_size, additional_fields={}
):
    data_type_map = {"FLOAT32": "float", "BINARY": "byte"}
    schema = {
        "schema": {
            "settings": {"index": {"knn": True, "knn.algo_param.ef_search": 100}},
            "mappings": {
                "properties": {
                    field_name: {
                        "type": "knn_vector",
                        "dimension": dimensions,
                        "data_type": data_type_map.get(quantization_size, "float"),
                        "method": {
                            "name": "hnsw",
                            "space_type": similarity_score or "l2",
                            "engine": "lucene",
                            "parameters": {"ef_construction": 128, "m": 24},
                        },
                    }
                }
            },
        }
    }
    schema["schema"]["mappings"]["properties"] | additional_fields
    return schema


PRODUCTS_SCHEMA = basic_schema(
    {
        "upc": text_field(fielddata=True),
        "_text_": text_field(),#analyzer="autocomplete"),
        "name": text_field(copy_to="_text_"),
        "manufacturer": text_field(copy_to="_text_"),
        "short_description": text_field(copy_to="_text_"),
        "long_description": text_field(copy_to="_text_"),
        "has_promotion": boolean_field(),
    },
    "upc",
)
PRODUCTS_SCHEMA["schema"]["settings"] = {
    "analysis": {
        "filter": {
            "edge_ngram_filter": {"type": "edge_ngram", "min_gram": 1, "max_gram": 20}
        },
        "analyzer": {
            "autocomplete": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "edge_ngram_filter"],
            }
        },
    }
}

PRODUCT_BOOSTS_SCHEMA = copy.deepcopy(PRODUCTS_SCHEMA)
PRODUCT_BOOSTS_SCHEMA["schema"]["settings"] = {
    "analysis": {
        "analyzer": {
            "boost_analyzer": {
                "tokenizer": {"type": "simple_pattern_split", "pattern": ","},
                "filter": [
                    "lowercase",
                    {
                        "type": "delimited_payload",
                        "delimiter": "|",
                        "encoding": "float",
                    },
                ],
            }
        },
        "filter": {
            "edge_ngram_filter": {"type": "edge_ngram", "min_gram": 1, "max_gram": 20}
        },
    }
}
PRODUCT_BOOSTS_SCHEMA["schema"]["mappings"]["properties"]["signals_boosts"] = {
    "type": "text",
    "fielddata": "true",
    "index": "true",
    "position_increment_gap": 100,
    "analyzer": "boost_analyzer",
}

SCHEMAS = {
    "cat_in_the_hat": basic_schema(
        {
            "id": text_field(),
            "title": text_field(),
            "description": text_field()
            | {"similarity": "BM25", "discount_overlaps": False},
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
    "stackexchange": body_title_schema(),
    "health": body_title_schema(),
    "cooking": body_title_schema(),
    "scifi": body_title_schema(),
    "travel": body_title_schema(),
    "devops": body_title_schema(),
    "tmdb": basic_schema(
        {
            "title": text_field(),
            "overview": text_field(),
            "release_year": double_field(),
        }
    ),
    "outdoors": basic_schema(
        {
            "post_type": string_field(),
            "accepted_answer_id": integer_field(),
            "parent_id": integer_field(),
            "creation_date": string_field(),
            "score": integer_field(),  # rename?
            "view_count": integer_field(),
            "body": text_field(),
            "owner_user_id": text_field(),
            "title": text_field(),
            "tags": keyword_field(),
            "url": string_field(),
            "answer_count": integer_field(),
        }
    ),
    "tmdb_with_embeddings": dense_vector_schema(
        "image",
        512,
        "dot_product",
        "FLOAT32",
        {"title": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "outdoors_with_embeddings": dense_vector_schema(
        "image", 728, "dot_product", "FLOAT32", {"title": text_field()}
    ),
}
