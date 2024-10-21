import os
import copy

OPENSEARCH_HOST = os.getenv("AIPS_OPENSEARCH_HOST") or "aips-opensearch"
OPENSEARCH_PORT = os.getenv("AIPS_OPENSEARCH_PORT") or "9200"
OPENSEARCH_URL = f"http://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}"

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

# {"multiValued": "true", "docValues": "true"}
def date_field():
    return base_field("date")

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
    datatype_map = {"FLOAT32": "float", "BINARY": "byte"}
    schema = {
        "schema": {
            "settings": {"index": {"knn": True, "knn.algo_param.ef_search": 100}},
            "mappings": {
                "properties": {
                    field_name: {
                        "type": "knn_vector",
                        "dimension": dimensions,
                        #"data_type": data_type_map.get(quantization_size, "float"),
                        "method": {
                            "name": "hnsw",
                            "engine": "nmslib",
                            "space_type": similarity_score or "l2",                            
                            "parameters": {"ef_construction": 128, "m": 24},
                        }
                    }
                }
            }
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
        "has_promotion": boolean_field()
    },
    "upc"
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
        }
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
        }
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
            "description": text_field() | {"similarity": "BM25",
                                           "discount_overlaps": False},
        },
        "id"
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
            "signal_time": text_field(),
            "id": integer_field()
        }
    ),
    "signals_boosting": basic_schema(
        {
            "query": keyword_field(),
            "doc": text_field(),
            "boost": integer_field()
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
            "id": text_field(),
            "post_type": text_field(),
            "accepted_answer_id": integer_field(),
            "parent_id": integer_field(),
            "creation_date": text_field(),
            "score": integer_field(),  # rename?
            "view_count": integer_field(),
            "body": text_field(fielddata=True),
            "owner_user_id": text_field(),
            "title": text_field(fielddata=True),
            "tags": keyword_field(),
            "url": text_field(),
            "answer_count": integer_field(),
        },
        "id"
    ),
    "tmdb_with_embeddings": dense_vector_schema(
        "image_embedding",
        512,
        "innerproduct",
        "FLOAT32",
        {"title": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "tmdb_lexical_plus_embeddings": dense_vector_schema(
        "image_embedding",
        512,
        "innerproduct",
        "FLOAT32",
        {"title": text_field(), "overview": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "outdoors_with_embeddings": dense_vector_schema(
        "image_embedding", 728, "dot_product", "FLOAT32", {"title": text_field()}
    ),
    "ubi_queries": basic_schema({
        "timestamp": date_field(), # signal_time
        "query_id": keyword_field(),
        "client_id": text_field()
    }),
    "ubi_aips_events": basic_schema({
        "application": text_field(),
        "action_name": text_field(),
        "query_id": keyword_field(), #linked, linked to queries.query_id
        "client_id": text_field(), #the user, linked to queries.client_id 
        "timestamp": date_field(), # signal_time
        "message_type": text_field(), # type
        "message": text_field(),
        #"event_attributes": {}
    })
}
