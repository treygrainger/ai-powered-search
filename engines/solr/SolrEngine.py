import requests
from engines.Engine import Engine
from engines.solr.SolrCollection import SolrCollection
from engines.solr.config import SOLR_COLLECTIONS_URL, STATUS_URL, SOLR_URL
class SolrEngine(Engine):
    def __init__(self):
        super().__init__("Solr")

    def health_check(self):
        status = requests.get(STATUS_URL).json()["responseHeader"]["status"] == 0
        if status:
            print("Solr is up and responding.")
            print("Zookeeper is up and responding.")
        return status
    
    def print_status(self, response):        
        print("Status: Success" if response["responseHeader"]["status"] == 0 else
              f"Status: Failure; Response:[ {response} ]" )

    def does_collection_exist(self, name, log=False):
        url = f"{SOLR_URL}/admin/collections?action=LIST&wt=json"
        response = requests.get(url).json()
        return name in response["collections"]
    
    def is_collection_healthy(self, name, expected_count, log=False):
        collection_exists = self.does_collection_exist(name)
        if log: print(f"Collection [{name}] exists? {collection_exists}")
        document_count = self.get_collection(name).get_document_count()
        if log: print(f"Documents expected: {expected_count}")
        if log: print(f"Documents found: {document_count}")
        return collection_exists and document_count == expected_count

    def create_collection(self, name, force_rebuild=True, log=False):
        collection = self.get_collection(name)
        if force_rebuild:
            wipe_collection_params = [("action", "delete"), ("name", name)]
            print(f'Wiping "{name}" collection')
            response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()
            requests.get(f"{SOLR_URL}/admin/configs?action=DELETE&name={name}.AUTOCREATED")

        create_collection_params = [("action", "CREATE"),
                                    ("name", name),
                                    ("numShards", 1),
                                    ("replicationFactor", 1)]
        print(f'Creating "{name}" collection')
        response = requests.post(SOLR_COLLECTIONS_URL + "?commit=true", data=create_collection_params).json()
        if log: print(response)
        self.apply_schema_for_collection(collection, log=log)
        self.print_status(response)
        return collection
    
    def get_collection(self, name):
        return SolrCollection(name)
    
    def apply_schema_for_collection(self, collection, log=False):
        match collection.name:
            case "cat_in_the_hat":
                self.set_search_defaults(collection)
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "description")
            case "products" | "products_with_signals_boosts" | "products_with_promotions":
                self.delete_copy_fields(collection) 
                self.set_search_defaults(collection)
                self.add_copy_field(collection, "*", "_text_")
                self.upsert_text_field(collection, "upc")
                self.upsert_text_field(collection, "manufacturer")
                if collection.name == "products_with_promotions":
                    self.upsert_field(collection, "has_promotion", "boolean")
                self.upsert_text_field(collection, "short_description")
                self.upsert_text_field(collection, "long_description")

                self.upsert_text_field(collection, "name")
                self.add_ngram_field_type(collection)
                self.add_copy_field(collection, "name", "name_ngram")                
                self.add_omit_norms_field_type(collection)
                self.add_copy_field(collection, "name", "name_omit_norms")                
                self.add_copy_field(collection, "name", "name_txt_en_split") 
                if collection.name == "products_with_signals_boosts":
                    self.upsert_boosts_field_type(collection, "boosts")
                    self.upsert_boosts_field(collection, "signals_boosts")
            case "jobs":
                self.set_search_defaults(collection)
                self.upsert_text_field(collection, "company_country")
                self.upsert_text_field(collection, "job_description")
                self.upsert_text_field(collection, "company_description")
            case "stackexchange" | "health" | "cooking" | "scifi" | "travel" | "devops":
                self.set_search_defaults(collection)
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
            case "tmdb" :
                self.set_search_defaults(collection)
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "overview")
                self.upsert_double_field(collection, "release_year")
            case "tmdb_with_embeddings":
                self.upsert_text_field(collection, "movie_id")
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "image_id")
                self.add_vector_field(collection, "image_embedding", 512, "dot_product")
                self.add_vector_field(collection, "image_binary_embedding", 512, "euclidean", "BYTE")
            case "tmdb_lexical_plus_embeddings":
                self.set_search_defaults(collection)
                self.upsert_text_field(collection, "movie_id")
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "image_id")
                self.add_vector_field(collection, "image_embedding", 512, "dot_product")
                self.upsert_text_field(collection, "overview")
            case "outdoors":
                self.set_search_defaults(collection)
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
                self.upsert_string_field(collection,"url")
                self.upsert_integer_field(collection, "answer_count")
            case "outdoors_with_embeddings":
                self.upsert_text_field(collection, "title")
                self.add_vector_field(collection, "title_embedding", 768, "dot_product")
            case "outdoors_quantization":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
                self.add_vector_field(collection, "text_embedding", 1024, "dot_product")
                self.add_vector_field(collection, "binary_text_embedding", 1024, "euclidean", "BYTE")
            case "reviews":
                self.upsert_text_field(collection, "content")
                self.add_delimited_field_type(collection, "commaDelimited", ",\s*")
                self.upsert_text_field(collection, "categories")
                self.upsert_field(collection, "doc_type", "commaDelimited")
                self.add_copy_field(collection, "categories", ["doc_type"])
                self.upsert_field(collection, "location_coordinates", "location")
            case "entities":
                self.add_tag_field_type(collection)
                self.upsert_string_field(collection, "surface_form")
                self.upsert_string_field(collection, "canonical_form")
                self.upsert_string_field(collection, "admin_area")
                self.upsert_string_field(collection, "country")
                self.upsert_field(collection, "name", "text_general")
                self.upsert_integer_field(collection, "popularity")
                self.upsert_field(collection, "name_tag", "tag", {"stored": "false"})
                self.add_copy_field(collection, "name", ["surface_form", "name_tag", "canonical_form"])
                self.add_copy_field(collection, "surface_form", ["name_tag"])
                self.add_tag_request_handler(collection, "/tag", "name_tag")
            case _:
                self.set_search_defaults(collection)
              
    def add_vector_field(self, collection, field_name, dimensions, similarity_function,
                         vector_encoding_size="FLOAT32"):
        field_type = f"{field_name}_vector"
        field_name = f"{field_name}"
        self.delete_field(collection, field_name)
        self.delete_field_type(collection, field_type)
        
        add_field_type = {
            "add-field-type": {
                "name": field_type,
                "class": "solr.DenseVectorField",
                "vectorDimension": dimensions,
                "vectorEncoding": vector_encoding_size,
                "similarityFunction": similarity_function
            }
        }
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field_type)
        self.add_field(collection, field_name, field_type)

    def set_search_defaults(self, collection, default_parser="edismax"):
        request = {
            "update-requesthandler": {
                "name": "/select",
                "class": "solr.SearchHandler",
                "defaults": {"defType": default_parser,
                             "indent": True}
            }
        }
        return requests.post(f"{SOLR_URL}/{collection.name}/config", json=request)
        
    def add_copy_field(self, collection, source, dest):
        request = {"add-copy-field": {"source": source, "dest": dest}}
        return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=request)

    def upsert_text_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "text_general")
    
    def upsert_double_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "pdouble")
    
    def upsert_integer_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "pint")
    
    def upsert_keyword_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "string", {"multiValued": "true", "docValues": "true"})
        
    def upsert_string_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "string", {"indexed": "false", "docValues": "true"})
    
    def upsert_boosts_field(self, collection, field_name, field_type_name="boosts"):
        self.upsert_field(collection, field_name, field_type_name, {"multiValued":"true"})
           
    def upsert_field(self, collection, field_name, type, additional_schema={}):
        self.delete_field(collection, field_name)
        return self.add_field(collection, field_name, type, additional_schema)
    
    def add_field(self, collection, field_name, type, additional_schema={}, log=False):        
        field = {"name": field_name, "type": type,
                 "stored": "true", "indexed": "true", "multiValued": "false"}
        field.update(additional_schema)
        add_field = {"add-field": field}
        resp = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field)
        if log:
            print(resp.json())
        return resp
    
    def delete_field(self, collection, field_name, log=False):
        delete_field = {"delete-field": {"name": field_name}}
        try:
            resp = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_field)
            if log:
                print(resp.json())
            return resp
        except:
            return {}
    
    def delete_field_type(self, collection, field_type_name):
        delete_field_type = {"delete-field-type": {"name": field_type_name}}
        try:
            return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_field_type).json()
        except:
            return {}
        
    def upsert_boosts_field_type(self, collection, field_type_name):

        print(f'Adding "{field_type_name}" field type to collection')
        add_field_type = { 
            "add-field-type": {
                "name": field_type_name,
                "class": "solr.TextField",
                "positionIncrementGap": "100",
                "analyzer": {
                    "tokenizer": {"class": "solr.PatternTokenizerFactory",
                                  "pattern": ","},
                    "filters": [{"class": "solr.LowerCaseFilterFactory"},
                                {"class": "solr.DelimitedPayloadFilterFactory",
                                 "delimiter": "|", "encoder": "float"}]}}}

        return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field_type).json()

    def add_ngram_field_type(self, collection):
        ngram_analyzer = {
            "tokenizer": {"class": "solr.StandardTokenizerFactory"},
            "filters": [{"class": "solr.LowerCaseFilterFactory"},
                        {"class": "solr.NGramFilterFactory",
                         #"preserveOriginal": "true",
                         "minGramSize": "3",
                         "maxGramSize": "6"}]}
        self.add_text_field_type(collection, "ngram", ngram_analyzer,
                                 omit_frequencies_and_positions=True)

    def add_omit_norms_field_type(self, collection):
        text_general_analyzer = {
            "tokenizer": {"class": "solr.StandardTokenizerFactory"},
            "filters": [{"class": "solr.LowerCaseFilterFactory"}]
            }

        self.add_text_field_type(collection, "omit_norms", text_general_analyzer,
                                 omit_norms=True)

    def add_text_field_type(self, collection, name, analyzer, 
                            omit_frequencies_and_positions=False,
                            omit_norms=False):       
        """Create a field type and a corresponding dynamic field."""
        field_type_name = "text_" + name
        dynamic_field_name = "*_" + name
        
        delete_dynamic_field = {"delete-dynamic-field": {"name": dynamic_field_name}}
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_dynamic_field)
        self.delete_field_type(collection, field_type_name)

        add_field_type = {
            "add-field-type": {
                "name": field_type_name,
                "class":"solr.TextField",
                "positionIncrementGap":"100",
                "analyzer": analyzer,
                "omitTermFreqAndPositions": omit_frequencies_and_positions,
                "omitNorms": omit_norms
            }
        }
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field_type)

        add_dynamic_field = {
            "add-dynamic-field": {
                "name": dynamic_field_name,
                "type": field_type_name,
                "stored": True
            }
        }      
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_dynamic_field)

    def delete_copy_fields(self, collection):
        copy_fields = requests.get(f"{SOLR_URL}/{collection.name}/schema/copyfields?wt=json").json()
        for field in copy_fields["copyFields"]:
            delete_copy_field = {"delete-copy-field": {"source": field["source"],
                                                       "dest": field["dest"]}}
            response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_copy_field).json()
    
    def add_tag_request_handler(self, collection, request_name, field):
        request = {
            "add-requesthandler" : {
                "name": request_name,
                "class": "solr.TaggerRequestHandler",
                "defaults": {
                    "field": field,
                    "json.nl": "map",
                    "sort": "popularity desc",
                    "matchText": "true",
                    "fl": "id,canonical_form,surface_form,type,semantic_function,popularity,country,admin_area,location_coordinates"
                }
            }
        }
        return requests.post(f"{SOLR_URL}/{collection.name}/config", json=request)
        
    def add_tag_field_type(self, collection):
        request = {
            "add-field-type": {
                "name": "tag",
                "class": "solr.TextField",
                "postingsFormat": "FST50",
                "omitNorms": "true",
                "omitTermFreqAndPositions": "true",
                "indexAnalyzer": {
                    "tokenizer": {
                        "class": "solr.StandardTokenizerFactory"},
                    "filters": [
                        {"class": "solr.EnglishPossessiveFilterFactory"},
                        {"class": "solr.ASCIIFoldingFilterFactory"},
                        {"class": "solr.LowerCaseFilterFactory"},
                        {"class": "solr.ConcatenateGraphFilterFactory", "preservePositionIncrements": "false"}
                    ]},
                "queryAnalyzer": {
                    "tokenizer": {
                        "class": "solr.StandardTokenizerFactory"},
                    "filters": [
                        {"class": "solr.EnglishPossessiveFilterFactory"},
                        {"class": "solr.ASCIIFoldingFilterFactory"},
                        {"class": "solr.LowerCaseFilterFactory"}
                    ]}
                }
            }
        return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=request).text
    
    def add_delimited_field_type(self, collection, field_name, pattern):
        request = {
            "add-field-type": {
                "name": field_name,
                "class": "solr.TextField",
                "positionIncrementGap": 100,
                "omitTermFreqAndPositions": "true",
                "indexAnalyzer": {
                    "tokenizer": {
                        "class": "solr.PatternTokenizerFactory",
                        "pattern": pattern
                    }
                }
            }
        }
        return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=request)
