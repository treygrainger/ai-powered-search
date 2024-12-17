from datetime import date
import os
import copy

WEAVIATE_HOST = os.getenv("AIPS_WEAVIATE_HOST") or "aips-weaviate"
WEAVIATE_PORT = os.getenv("AIPS_OPENSEARCH_PORT") or "8090"
WEAVIATE_URL = f"http://{WEAVIATE_HOST}:{WEAVIATE_PORT}"


def schema_contains_id_field(collection_name): 
    #For hack $WV8_001 
    collection_name = collection_name.lower()
    return collection_name in SCHEMAS and \
        any(filter(lambda p: p["name"] == "__id",
                   SCHEMAS[collection_name]["schema"]["properties"]))

def get_vector_field_name(collection_name):
    return SCHEMAS.get(collection_name.lower(), {}).get("vector_field", None)

def schema_contains_custom_vector_field(collection_name):
    return (get_vector_field_name(collection_name) is not None)

def base_field(type, **kwargs):
    return {"dataType": [type]} | kwargs
#"indexSearchable": True
def text_field(**kwargs):
    return base_field("text", **kwargs)

def boolean_field():
    return base_field("boolean")

def double_field():
    return base_field("number")

def integer_field():
    return base_field("int")

# {"multiValued": "true", "docValues": "true"}
def date_field():
    return base_field("date")

def generate_property_list(field_mappings):
    return [{"name": name} | value
            for name, value in field_mappings.items()]

def basic_schema(collection_name, field_mappings, vector_field=None):
    schema = {"schema": {"class": collection_name, 
                         "properties": generate_property_list(field_mappings)}}
    if vector_field:
        schema["vector_field"] = vector_field
    return schema

def body_title_schema(collection_name):
    return basic_schema(collection_name,
                        {"title": text_field(), "body": text_field()})

PRODUCTS_SCHEMA = basic_schema("products",
    {
        "upc": text_field(), #fielddata=True
        "_text_": text_field(),
        "name_ngram": text_field(), #analyzer="bigram_analyzer", fielddata=True
        "name_fuzzy": text_field(), #analyzer="shingle_analyzer", fielddata=True
        "short_description_ngram": text_field(), #analyzer="bigram_analyzer"
        "name": text_field(), #copy_to=["name_ngram", "_text_", "name_fuzzy"], fielddata=True
        "short_description": text_field(), #copy_to=["short_description_ngram", "_text_"]
        "long_description": text_field(), #copy_to="_text_"
        "manufacturer": text_field(), #copy_to="_text_"
        "has_promotion": boolean_field()
    }
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

#PRODUCT_BOOSTS_SCHEMA = copy.deepcopy(PRODUCTS_SCHEMA)
#PRODUCT_BOOSTS_SCHEMA["schema"]["class"] = "products_with_signals_boosts"
#PRODUCT_BOOSTS_SCHEMA["schema"]["mappings"]["properties"]["signals_boosts"] = {
        #"type": "rank_features"
    #}

def signals_boosting_schema(collection_name):
    return basic_schema(collection_name, {
        "query": text_field(),
        "doc": text_field(),
        "boost": integer_field()})

SCHEMAS = {
    "cat_in_the_hat": basic_schema("cat_in_the_hat",
        {
            "__id": text_field(),
            "title": text_field(),
            "description": text_field() | {"similarity": "BM25",
                                           "discount_overlaps": False}
        }
    ),
    "products": PRODUCTS_SCHEMA,
    #"products_with_signals_boosts": PRODUCT_BOOSTS_SCHEMA,
    "jobs": basic_schema("jobs",
        {
            "company_country": text_field(),
            "job_description": text_field(),
            "company_description": text_field(),
        }
    ),
    "signals": basic_schema("signals",
        {
            "query_id": text_field(),
            "user": text_field(),
            "type": text_field(),
            "target": text_field(),
            "signal_time": date_field()
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
        "__id": text_field(),
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
            "__id": text_field(),
            "title": text_field(),
            "overview": text_field(),
            "cast": text_field(),
            "release_date": text_field(),
            "release_year": text_field(),
            "poster_file": text_field(),
            "poster_path": text_field(),
            "vote_average": integer_field(),
            "vote_count": integer_field(),
            "movie_image_ids": text_field()
        }
    ),
    "outdoors": basic_schema("outdoors",
        {
            "__id": text_field(),
            "post_type": text_field(),
            "accepted_answer_id": integer_field(),
            "parent_id": text_field(), #integer fields 
            "creation_date": text_field(),
            "score": integer_field(),  # rename?
            "view_count": integer_field(),
            "body": text_field(fielddata=True),
            "owner_user_id": text_field(),
            "title": text_field(fielddata=True),
            "url": text_field(),
            "answer_count": integer_field(),
        }
    ),
    "tmdb_with_embeddings": basic_schema("tmdb_with_embeddings",
        {
            "title": text_field(),
            "movie_id": text_field(),
            "image_id": text_field()
        },
        "image_embedding"),
    "tmdb_lexical_plus_embeddings": basic_schema(
        "tmdb_lexical_plus_embeddings",
        {"title": text_field(), "overview": text_field(),
         "movie_id": text_field(), "image_id": text_field()},
         "image_embedding"
    ),
    "outdoors_with_embeddings": basic_schema("outdoors_with_embeddings",
                                             {"__id": text_field(),
                                              "title": text_field()},
                                             "title_embedding"),
}