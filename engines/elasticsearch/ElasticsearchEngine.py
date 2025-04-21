import requests
import os
from engines.Engine import Engine
from engines.elasticsearch.ElasticsearchCollection import ElasticsearchCollection

# Get host and port from environment variables with fallbacks
AIPS_ES_HOST = os.getenv("AIPS_ES_HOST") or "aips-elasticsearch"
AIPS_ES_PORT = os.getenv("AIPS_ES_PORT") or "9200"
ES_URL = f"http://{AIPS_ES_HOST}:{AIPS_ES_PORT}"
STATUS_URL = f"{ES_URL}/_cluster/health"


class ElasticsearchEngine(Engine):
    def __init__(self):
        super().__init__("Elasticsearch")

    def health_check(self):
        try:
            status = requests.get(STATUS_URL).json()["status"] in ["green", "yellow"]
            return status
        except Exception:
            return False

    def print_status(self, response):
        if "acknowledged" in response and response["acknowledged"]:
            print("Status: Success")
        else:
            print("Status: Failure")

    def create_collection(self, name, log=False):
        collection = self.get_collection(name)

        # Delete index if it exists
        try:
            requests.delete(f"{ES_URL}/{name}")
        except:
            pass  # Ignore if index doesn't exist

        # Create new index
        response = requests.put(f"{ES_URL}/{name}").json()

        if log:
            print(response)

        self.apply_schema_for_collection(collection, log=log)
        self.print_status(response)
        return collection

    def get_collection(self, name):
        return ElasticsearchCollection(name)

    def apply_schema_for_collection(self, collection, log=False):
        match collection.name:
            case "cat_in_the_hat":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "description")
            case "products" | "products_with_signals_boosts":
                self.upsert_field(collection, "upc", "text", {"fielddata": True})
                self.upsert_text_field(collection, "manufacturer")
                self.upsert_field(collection, "has_promotion", "boolean")
                self.upsert_text_field(collection, "short_description")
                self.upsert_text_field(collection, "long_description")
                self.upsert_text_field(collection, "name")

                if collection.name == "products_with_signals_boosts":
                    self.upsert_boosts_field(collection, "signals_boosts")
            case "jobs":
                self.upsert_text_field(collection, "company_country")
                self.upsert_text_field(collection, "job_description")
                self.upsert_text_field(collection, "company_description")
            case "stackexchange" | "health" | "cooking" | "scifi" | "travel" | "devops":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
            case "tmdb":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "overview")
                self.upsert_double_field(collection, "release_year")
            case "tmdb_with_embeddings":
                self.upsert_text_field(collection, "movie_id")
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "image_id")
                self.add_vector_field(collection, "image_embedding", 512, "dot_product")
            case "tmdb_lexical_plus_embeddings":
                self.upsert_text_field(collection, "movie_id")
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "image_id")
                self.add_vector_field(collection, "image_embedding", 512, "dot_product")
                self.upsert_text_field(collection, "overview")
            case "outdoors":
                self.upsert_string_field(collection, "post_type")
                self.upsert_integer_field(collection, "accepted_answer_id")
                self.upsert_integer_field(collection, "parent_id")
                self.upsert_string_field(collection, "creation_date")
                self.upsert_integer_field(collection, "score")
                self.upsert_integer_field(collection, "view_count")
                self.upsert_text_field(collection, "body")
                self.upsert_text_field(collection, "owner_user_id")
                self.upsert_text_field(collection, "title")
                self.upsert_keyword_field(collection, "tags")
                self.upsert_string_field(collection, "url")
                self.upsert_integer_field(collection, "answer_count")
            case "outdoors_with_embeddings":
                self.upsert_text_field(collection, "title")
                self.add_vector_field(collection, "title_embedding", 768, "dot_product")
            case "outdoors_quantization":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
                self.add_vector_field(collection, "text_embedding", 1024, "dot_product")
            case "reviews":
                self.upsert_text_field(collection, "content")
                self.upsert_text_field(collection, "categories")
                self.upsert_field(collection, "location_coordinates", "geo_point")
            case "entities":
                self.upsert_string_field(collection, "surface_form")
                self.upsert_string_field(collection, "canonical_form")
                self.upsert_string_field(collection, "admin_area")
                self.upsert_string_field(collection, "country")
                self.upsert_field(collection, "name", "text")
                self.upsert_integer_field(collection, "popularity")
            case "signals":
                self.upsert_text_field(collection, "query_id")
                self.upsert_text_field(collection, "user")
                self.upsert_text_field(collection, "type")
                self.upsert_text_field(collection, "target")
                self.upsert_field(collection, "signal_time", "date")
            case _:
                pass

    def add_vector_field(self, collection, field_name, dimensions, similarity_function):
        mapping = {
            "properties": {
                field_name: {
                    "type": "dense_vector",
                    "dims": dimensions,
                    "index": True,
                    "similarity": "cosine" if similarity_function == "dot_product" else "l2_norm",
                }
            }
        }
        return requests.put(f"{ES_URL}/{collection.name}/_mapping", json=mapping)

    def upsert_text_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "text")

    def upsert_double_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "double")

    def upsert_integer_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "integer")

    def upsert_keyword_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "keyword")

    def upsert_string_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "keyword", {"index": False})

    def upsert_boosts_field(self, collection, field_name):
        self.upsert_field(
            collection, field_name, "text", {"fields": {"keyword": {"type": "keyword"}}}
        )

    def upsert_field(self, collection, field_name, field_type, additional_properties={}):
        properties = {"type": field_type}
        properties.update(additional_properties)

        mapping = {"properties": {field_name: properties}}

        return requests.put(f"{ES_URL}/{collection.name}/_mapping", json=mapping)
