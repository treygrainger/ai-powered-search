from datetime import date
import os
import copy
from pydoc import text

WEAVIATE_HOST = os.getenv("AIPS_WEAVIATE_HOST") or "aips-weaviate"
WEAVIATE_PORT = os.getenv("AIPS_OPENSEARCH_PORT") or "8090"
WEAVIATE_URL = f"http://{WEAVIATE_HOST}:{WEAVIATE_PORT}"
DEFAULT_BOOST_FIELD = "boosts"

def get_boost_field(collection_name):
    return SCHEMAS.get(collection_name.lower(), {}).get("boost_field", DEFAULT_BOOST_FIELD)

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
                        {"post_type_id": text_field(),
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
                         "category": text_field()})

def signals_boosting_schema(collection_name):
    return basic_schema(collection_name, {
        "query": text_field(),
        "doc": text_field(),
        "boost": integer_field()})

PRODUCTS_SCHEMA_PROPERTIES = {"upc": text_field(indexSearchable=True),
                              "name": text_field(),
                              "name_fuzzy": text_field(),
                              "short_description": text_field(),
                              "long_description": text_field(),
                              "manufacturer": text_field(),
                              "has_promotion": text_field()}
PRODUCTS_SCHEMA = basic_schema("products", PRODUCTS_SCHEMA_PROPERTIES)
PRODUCTS_SCHEMA["boost_field"] = "upc"

PRODUCT_BOOSTS_SCHEMA = basic_schema("products_with_signals_boosts",
                                     PRODUCTS_SCHEMA_PROPERTIES | {"signals_boosts": text_field()})
PRODUCT_BOOSTS_SCHEMA["boost_field"] = "upc"

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
    "products_with_signals_boosts": PRODUCT_BOOSTS_SCHEMA,
    "jobs": basic_schema("jobs",
        {
            "job_title": text_field(),
            "job_description": text_field(),
            "job_type": text_field(),
            "category": text_field(),
            "job_location": text_field(),
            "job_city": text_field(),
            "job_state": text_field(),
            "job_country": text_field(),
            "job_zip_code": text_field(),
            "job_address": text_field(),
            "min_salary": text_field(),
            "max_salary": text_field(),
            "salary_period": text_field(),
            "apply_url": text_field(),
            "apply_email": text_field(),
            "num_employees": text_field(),
            "industry": text_field(),
            "company_name": text_field(),
            "company_email": text_field(),
            "company_website": text_field(),
            "company_phone": text_field(),
            "company_logo": text_field(),
            "company_description": text_field(),
            "company_location": text_field(),
            "company_city": text_field(),
            "company_state": text_field(),
            "company_country": text_field(),
            "company_zip_code": text_field(),
            "job_date": text_field()
        }
    ),
    "signals": basic_schema("signals",
        {
            "query_id": text_field(),
            "user": text_field(),
            "type": text_field(),
            "target": text_field(),
            "signal_time": text_field()
        }
    ),
    "signals_boosting": signals_boosting_schema("signals_boosting"),
    "basic_signals_boosts": signals_boosting_schema("basic_signals_boosts"),
    "normalized_signals_boosts": signals_boosting_schema("normalized_signals_boosts"),
    "signals_boosts_with_spam": signals_boosting_schema("signals_boosts_with_spam"),
    "signals_boosts_anti_spam": signals_boosting_schema("signals_boosts_anti_spam"),
    "signals_boosts_weighted_types": signals_boosting_schema("signals_boosts_weighted_types"),
    "signals_boosts_time_weighted": signals_boosting_schema("signals_boosts_time_weighted"),
    "user_product_implicit_preferences": basic_schema(
        "user_product_implicit_preferences",
        {"user": text_field(),
         "product": text_field(),
         "rating": integer_field()}),
    "user_item_recommendations": basic_schema(
        "user_item_recommendations",
        {"user": text_field(),
         "product": text_field(),
         "boost": double_field()}),
    "stackexchange": body_title_schema("stackexchange"),
    "health": body_title_schema("health"),
    "cooking": body_title_schema("cooking"),
    "scifi": body_title_schema("scifi"),
    "travel": body_title_schema("travel"),
    "devops": body_title_schema("devops"),
    "reviews": basic_schema("reviews", {
        "__id": text_field(),
        "business_name": text_field(),
        "name": text_field(),
        "city": text_field(),
        "state": text_field(),
        "content": text_field(),
        "categories": text_field(copy_to="doc_type"),
        "stars_rating": integer_field(),
        "location_coordinates": base_field("geoCoordinates")},
        "popularity_vector"), #base_field("geoCoordinates")}),
    "entities": basic_schema("entities",
                             {"__id": text_field(),
                              "surface_form": text_field(),
                              "canonical_form": text_field(),
                              "type": text_field(),
                              "popularity": integer_field(),
                              "semantic_function": text_field()}),
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
            "body": text_field(),
            "owner_user_id": text_field(),
            "title": text_field(),
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
                                             "title_embedding")
}