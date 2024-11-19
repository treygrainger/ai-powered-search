from datetime import date
import os
import copy

WEAVIATE_HOST = os.getenv("AIPS_WEAVIATE_HOST") or "aips-weaviate"
WEAVIATE_PORT = os.getenv("AIPS_OPENSEARCH_PORT") or "8090"
WEAVIATE_URL = f"http://{WEAVIATE_HOST}:{WEAVIATE_PORT}"

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

def generate_property_list(field_mappings):
    return [{"name": name, "dataType": [value["type"]]}
            for name, value in field_mappings.items()]

def basic_schema(collection_name, field_mappings, id_field="_id"):
    return {
        "id_field": id_field,
        "schema": {"class": collection_name, 
                   "properties": generate_property_list(field_mappings)}}

def body_title_schema(collection_name):
    return basic_schema(collection_name,
                        {"title": text_field(), "body": text_field()})

def dense_vector_schema(collection_name, field_name, dimensions,
                        similarity_score, quantization_size, additional_fields={}
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

PRODUCTS_SCHEMA = basic_schema("products",
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
        "has_promotion": boolean_field()
    },
    "upc"
)
#PRODUCTS_SCHEMA["schema"]["settings"] = {
#    "index": {"mapping": {"total_fields": {"limit": 100000}}},
#    "analysis": {
#        "filter": {
#            "edge_ngram_filter": {"type": "edge_ngram",
#                                  "min_gram": 1,
#                                  "max_gram": 20}
#        },
#        "analyzer": {
#            "bigram_analyzer": {
#                "type": "custom",
#                "tokenizer": "standard",
#                "filter": ["lowercase", "edge_ngram_filter"],
#            },
#            "shingle_analyzer": {
#                "tokenizer": "standard",
#                "filter": ["lowercase", "shingle"]
#            }
#        }
#    }
#}

PRODUCT_BOOSTS_SCHEMA = copy.deepcopy(PRODUCTS_SCHEMA)
PRODUCT_BOOSTS_SCHEMA["schema"]["class"] = "products_with_signals_boosts"
PRODUCT_BOOSTS_SCHEMA["schema"]["mappings"]["properties"]["signals_boosts"] = {
        "type": "rank_features"
    }

def signals_boosting_schema(collection_name):
    return basic_schema(collection_name, {
        "query": keyword_field(),
        "doc": text_field(),
        "boost": integer_field()})

SCHEMAS = {
    "cat_in_the_hat": basic_schema("cat_in_the_hat",
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
    "jobs": basic_schema("jobs",
        {
            "company_country": text_field(),
            "job_description": text_field(),
            "company_description": text_field(),
        }
    ),
    "signals": basic_schema("signals",
        {
            "query": text_field(),
            "user": text_field(),
            "type": text_field(),
            "target": text_field(),
            "signal_time": date_field(),
            "id": integer_field()
        }
    ),
    "signals_boosting": signals_boosting_schema("signals_boosting"),
    "signals_boosts_with_spam": signals_boosting_schema("signals_boosts_with_spam"),
    "signals_boosts_anti_spam": signals_boosting_schema("signals_boosts_anti_spam"),
    "signals_boosts_weighted_types": signals_boosting_schema("signals_boosts_weighted_types"),
    "signals_boosts_time_weighted": signals_boosting_schema("signals_boosts_time_weighted"),
    "stackexchange": body_title_schema("stackexchange"),
    "health": body_title_schema("health"),
    "cooking": body_title_schema("cooking"),
    "scifi": body_title_schema("scifi"),
    "travel": body_title_schema("travel"),
    "devops": body_title_schema("devops"),
    "reviews": basic_schema("reviews", {
        "id": text_field(),
        "content": text_field(fielddata=True),
        "categories": text_field(copy_to="doc_type", fielddata=True),
        "doc_type": text_field(fielddata=True),
        "stars_rating": integer_field(),
        "city": text_field(fielddata=True),
        "state": text_field(fielddata=True),
        "business_name": text_field(fielddata=True),
        "name": text_field(fielddata=True),
        "location_coordinates": base_field("geo_point")}),
    "tmdb": basic_schema("tmdb",
        {
            "title": text_field(),
            "overview": text_field(),
            "release_year": double_field(),
        }
    ),
    "outdoors": basic_schema("outdoors",
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
    "tmdb_with_embeddings": dense_vector_schema("tmdb_with_embeddings",
        "image_embedding",
        512,
        "innerproduct",
        "FLOAT32",
        {"title": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "tmdb_lexical_plus_embeddings": dense_vector_schema("tmdb_lexical_plus_embeddings",
        "image_embedding",
        512,
        "innerproduct",
        "FLOAT32",
        {"title": text_field(), "overview": text_field(), "movie_id": text_field(), "image_id": text_field()},
    ),
    "outdoors_with_embeddings": dense_vector_schema("outdoors_with_embeddings",
        "title_embedding", 768, "innerproduct", "FLOAT32", {"title": text_field()},
    ),
    "ubi_queries": basic_schema("ubi_queries",{
        "timestamp": date_field(), # signal_time
        "query_id": keyword_field(),
        "client_id": text_field()
    }),
    "ubi_aips_events": basic_schema("ubi_aips_events", {
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