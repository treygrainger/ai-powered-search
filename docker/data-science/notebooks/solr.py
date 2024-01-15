import requests
import os
from IPython.display import display,HTML
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import SolrCollection

AIPS_SOLR_HOST = "aips-solr"
AIPS_ZK_HOST="aips-zk"
#AIPS_SOLR_HOST = "localhost"
#AIPS_ZK_HOST = "localhost"
AIPS_SOLR_PORT = os.getenv('AIPS_SOLR_PORT') or '8983'
AIPS_ZK_PORT= os.getenv('AIPS_ZK_PORT') or '2181'

SOLR_URL = f'http://{AIPS_SOLR_HOST}:{AIPS_SOLR_PORT}/solr'
SOLR_COLLECTIONS_URL = f'{SOLR_URL}/admin/collections'
STATUS_URL = f'{SOLR_URL}/admin/zookeeper/status'

class SolrEngine:
    def __init__(self):
        pass

    def health_check(self):
        return requests.get(STATUS_URL).json()["responseHeader"]["status"] == 0

    def create_collection(self, name):
        wipe_collection_params = [
            ('action', "delete"),
            ('name', name)
        ]
        print(f"Wiping '{name}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()
        self.print_status(response)

        create_collection_params = [
            ('action', "CREATE"),
            ('name', name),
            ('numShards', 1),
            ('replicationFactor', 1) ]
        print(f"Creating '{name}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=create_collection_params).json()
        
        self.apply_schema_for_collection(name)
        self.print_status(response)
        return SolrCollection(name)

    def get_collection(self, name):
        return SolrCollection(name)
    
    def apply_schema_for_collection(self, collection):
        match collection:
            case "cat_in_the_hat":
                self.upsert_text_field("title")
                self.upsert_text_field("description")
            case "products" | "products_with_signals_boosts":
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
        
    def upsert_field(self, collection, field_name, type, additional_schema={}):
        delete_field = {"delete-field":{"name": field_name}}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=delete_field)
        field = {"name": field_name, "type": type, "stored": "true", "indexed": "true", "multiValued": "false"}
        field.update(additional_schema)
        add_field = {"add-field": field}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=add_field)

    def enable_ltr(self, collection):
        collection_config_url = f'{SOLR_URL}/{collection.name}/config'

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
        return requests.delete(f'{SOLR_URL}/{collection}/schema/feature-store/{name}')

    def create_feature_store(self, collection, features):
        return requests.put(f'{SOLR_URL}/{collection}/schema/feature-store', json=features)

    def log_query(self, collection, featureset, ids, options={}, id_field='id'):
        efi = ' '.join(f'efi.{k}="{v}"' for k, v in options.items())
        params = {
            'fl': f"{id_field},[features store={featureset} {efi}]",
            'q': "{{!terms f={}}}{}".format(id_field, ','.join(ids)) if ids else "*:*",
            'rows': 1000,
            'wt': 'json'
        }
        resp = collection.search(data=params)
        docs = resp.json()['response']['docs']
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split('=')[1]), d['[features]'].split(',')))
            d['ltr_features'] = features

        return docs
    
    def spell_check(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/spell", json=request)
    
    def print_status(self, solr_response):
        print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

    def response_docs(self, response):
        return response.json()["response"]["docs"]
        
    def documents_as_html(self, response):
        return str(response.json()["response"]["docs"]).replace('\\n', '').replace(", '", ",<br/>'")
