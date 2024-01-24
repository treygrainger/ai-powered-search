import requests
import os
from IPython.display import display,HTML
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from solr_collection import SolrCollection
from env import *

def product_search_request(query):
    return {
        "query": query,
        "fields": ["upc", "name", "manufacturer", "score"],
        "limit": 5,
        "params": {
            "qf": "name manufacturer longDescription",
            "defType": "edismax",
            "indent": "true",
            "sort": "score desc, upc asc"
        }
    }
    
def skg_request(query):
    return {
        "query": query,
        "params": {
            "qf": "title body",
            "fore": "{!type=$defType qf=$qf v=$q}",
            "back": "*:*",
            "defType": "edismax",
            "rows": 0,
            "echoParams": "none",
            "omitHeader": "true"
        },
        "facet": {
            "body": {
                "type": "terms",
                "field": "body",
                "sort": { "relatedness": "desc"},
                "mincount": 2,
                "limit": 8,
                "facet": {
                    "relatedness": {
                        "type": "func",
                        "func": "relatedness($fore,$back)"
                        #"min_popularity": 0.0005
                    }
                }  
            }
        }
    }

class SolrEngine:
    def __init__(self):
        pass

    def health_check(self):
        return requests.get(STATUS_URL).json()["responseHeader"]["status"] == 0

    def create_collection(self, name):
        wipe_collection_params = [
            ("action", "delete"),
            ("name", name)
        ]
        print(f'Wiping "{name}" collection')
        response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()
        requests.get(f"{SOLR_URL}/admin/configs?action=DELETE&name={name}.AUTOCREATED")

        create_collection_params = [
            ("action", "CREATE"),
            ("name", name),
            ("numShards", 1),
            ("replicationFactor", 1) ]
        print(f'Creating "{name}" collection')
        response = requests.post(SOLR_COLLECTIONS_URL + "?commit=true", data=create_collection_params).json()
        
        self.apply_schema_for_collection(self.get_collection(name))
        self.print_status(response)
        return SolrCollection(name)

    def get_collection(self, name):
        return SolrCollection(name)
    
    def apply_schema_for_collection(self, collection):
        match collection.name:
            case "cat_in_the_hat":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "description")
            case "products" | "products_with_signals_boosts":
                self.add_copy_field(collection, "*", "_text_")
                self.upsert_text_field(collection, "upc")
                self.upsert_text_field(collection, "name")
                self.upsert_text_field(collection, "manufacturer")
                self.upsert_text_field(collection, "shortDescription")
                self.upsert_text_field(collection, "longDescription")
            case "jobs":
                self.upsert_text_field(collection, "company_country")
                self.upsert_text_field(collection, "job_description")
                self.upsert_text_field(collection, "company_description")
            case "stackexchange" | "health" | "cooking" | "scifi" | "travel" | "devops":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
            case "tmdb" :
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "overview")
                self.upsert_double_field(collection, "release_year")  
            case "outdoors":
                self.upsert_string_field(collection,"url")
                self.upsert_integer_field(collection,"post_type_id")
                self.upsert_integer_field(collection,"accepted_answer_id")
                self.upsert_integer_field(collection,"parent_id")
                self.upsert_integer_field(collection,"score")
                self.upsert_integer_field(collection,"view_count")
                self.upsert_text_field(collection,"body")
                self.upsert_text_field(collection,"title")
                self.upsert_keyword_field(collection,"tags")
                self.upsert_integer_field(collection,"answer_count")
                self.upsert_integer_field(collection,"owner_user_id")
            case "reviews":
                self.add_delimited_field_type(collection, "commaDelimited", ",\s*")
                self.add_delimited_field_type(collection, "pipeDelimited", "\|\s*") #necessary? is this used
                self.upsert_field(collection, "doc_type", "commaDelimited", {"multiValued": "true"})
                self.add_copy_field(collection, "categories_t", ["doc_type"])
                self.upsert_field(collection, "location_p", "location")
                self.add_copy_field(collection, "location_pt_s", ["location_p"])
            case "entities":
                self.add_tag_field_type(collection)
                self.upsert_string_field(collection, "surface_form")
                self.upsert_string_field(collection, "canonical_form")
                self.upsert_field(collection, "name", "text_general")
                self.upsert_integer_field(collection, "popularity")
                self.upsert_field(collection, "name_tag", "tag", {"stored": "false"})
                self.add_copy_field(collection, "name", ["surface_form", "name_tag", "canonical_form"])
                self.add_copy_field(collection, "population_i", ["popularity"]) #what is the source field here?
                self.add_copy_field(collection, "surface_form", ["name_tag"])
                self.add_tag_request_handler(collection, "/tag", "name_tag")
            case _:
                pass
            
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
        delete_field = {"delete-field":{"name": field_name}}
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_field)
        field = {"name": field_name, "type": type, "stored": "true", "indexed": "true", "multiValued": "false"}
        field.update(additional_schema)
        add_field = {"add-field": field}
        return requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field)
        
    def upsert_boosts_field_type(self, collection, field_type_name):
        delete_field_type = {"delete-field-type":{ "name":field_type_name }}
        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=delete_field_type).json()

        print(f'Adding "{field_type_name}" field type to collection')
        add_field_type = { 
            "add-field-type" : {
                "name": field_type_name,
                "class":"solr.TextField",
                "positionIncrementGap":"100",
                "analyzer" : {
                    "tokenizer": {
                        "class":"solr.PatternTokenizerFactory",
                        "pattern": "," },
                    "filters":[
                        { "class":"solr.LowerCaseFilterFactory" },
                        { "class":"solr.DelimitedPayloadFilterFactory", "delimiter": "|", "encoder": "float" }]}}}

        response = requests.post(f"{SOLR_URL}/{collection.name}/schema", json=add_field_type).json()
        self.print_status(response)

    def add_copy_field(self, collection, source, dest):
        request = {"add-copy-field": {"source": source, "dest": dest}}
        requests.post(f"{SOLR_URL}/{collection.name}/schema", json=request)
    
    def add_tag_request_handler(self, collection, request_name, field):
        request = {
            "add-requesthandler" : {
                "name": request_name,
                "class": "solr.TaggerRequestHandler",
                "defaults": {"field": field}
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
        print(f"{SOLR_URL}/{collection.name}/schema")
        print(requests.post(f"{SOLR_URL}/{collection.name}/schema", json=request).text)
    
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
    
    def enable_ltr(self, collection):
        collection_config_url = f"{SOLR_URL}/{collection.name}/config"

        del_ltr_query_parser = { "delete-queryparser": "ltr" }
        add_ltr_q_parser = {
        "add-queryparser": {
            "name": "ltr",
                "class": "org.apache.solr.ltr.search.LTRQParserPlugin"
            }
        }

        print(f"Adding LTR QParser for {collection.name} collection")
        response = requests.post(collection_config_url, json=del_ltr_query_parser)
        response = requests.post(collection_config_url, json=add_ltr_q_parser)
        self.print_status(response.json())

        del_ltr_transformer = { "delete-transformer": "features" }
        add_transformer =  {
        "add-transformer": {
            "name": "features",
            "class": "org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory",
            "fvCacheName": "QUERY_DOC_FV"
        }}

        print(f"Adding LTR Doc Transformer for {collection.name} collection")
        response = requests.post(collection_config_url, json=del_ltr_transformer)
        response = requests.post(collection_config_url, json=add_transformer)
        self.print_status(response.json())    

    def delete_feature_store(self, collection, name):
        return requests.delete(f"{SOLR_URL}/{collection.name}/schema/feature-store/{name}")

    def create_feature_store(self, collection, features):
        return requests.put(f"{SOLR_URL}/{collection.name}/schema/feature-store", json=features)

    def upload_model(self, collection, model):
        return requests.put(f"{SOLR_URL}/{collection.name}/schema/model-store", json=model)
    
    def log_query(self, collection, featureset, ids, options={}, id_field="id"):
        efi = " ".join(f'efi.{k}="{v}"' for k, v in options.items())
        params = {
            "fl": f"{id_field},[features store={featureset} {efi}]",
            "q": "{{!terms f={}}}{}".format(id_field, ",".join(ids)) if ids else "*:*",
            "rows": 1000,
            "wt": "json"
        }
        resp = collection.search(data=params)
        docs = resp.json()["response"]["docs"]
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split("=")[1]), d["[features]"].split(",")))
            d["ltr_features"] = features

        return docs
    
    def spell_check(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection.name}/spell", json=request).json()

    def print_status(self, solr_response):
        print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

    def docs_from_response(self, response):
        return response["response"]["docs"]
    
    def tag_query(self, collection_name, query):
        url_params = "json.nl=map&sort=popularity%20desc&matchText=true&echoParams=all&fl=id,type,canonical_form,surface_form,name,country:countrycode_s,admin_area:admin_code_1_s,popularity,*_p,semantic_function"
        return requests.post(f"{SOLR_URL}/{collection_name}/tag?{url_params}", query).json()
