import requests
import os
from IPython.display import display,HTML
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

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

    def print_status(self, solr_response):
        print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

    def create_collection(self, collection):
        wipe_collection_params = [
            ('action', "delete"),
            ('name', collection)
        ]
        print(f"Wiping '{collection}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()
        self.print_status(response)

        create_collection_params = [
            ('action', "CREATE"),
            ('name', collection),
            ('numShards', 1),
            ('replicationFactor', 1) ]
        print(f"Creating '{collection}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=create_collection_params).json()
        
        self.apply_schema_for_collection(collection)
        self.print_status(response)

    def apply_schema_for_collection(self, collection):
        match collection:
            case "cat_in_the_hat":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "description")
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
            case "stackexchange" | "health" | "cooking" | "scifi" | "outdoors" | "travel" | "devops":
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "body")
            case "tmdb" :
                self.upsert_text_field(collection, "title")
                self.upsert_text_field(collection, "overview")
                self.upsert_double_field(collection, "release_year")                
            case _:
                pass
            
    def populate_collection_from_csv(self, collection, file, more_opts=False):
        print(f"Loading {collection}")
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        if more_opts:
            reader = reader.option("charset", "utf-8").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("delimiter", ",")
        csv_df = reader.load(file)
        if more_opts:
            # We can rely on automatic generation of IDs, or we can create them ourselves. 
            # If we do it, comment out previous line
            # .withColumn("id", concat(col("category"), lit("_") col("id")))
            csv_df = csv_df.withColumn("category", lit(collection)).drop("id")
        print(f"{collection} Schema: ")
        csv_df.printSchema()
        options = {"zkhost": AIPS_ZK_HOST, "collection": collection,
                   "gen_uniq_key": "true", "commit_within": "5000"}
        csv_df.write.format("solr").options(**options).mode("overwrite").save()
        print("Status: Success")

    def populate_collection_from_csv(self, collection, file):
        print(f"Loading {collection}")
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
        print(f"{collection} Schema: ")
        csv_df.printSchema()
        options = {"zkhost": AIPS_ZK_HOST, "collection": collection,
                   "gen_uniq_key": "true", "commit_within": "5000"}
        csv_df.write.format("solr").options(**options).mode("overwrite").save()
        print("Status: Success")
    
    def populate_from_spark(self, collection, query,
                            spark=SparkSession.builder.appName("AIPS").getOrCreate()):
        opts = {"zkhost": "aips-zk", "collection": collection,
                "gen_uniq_key": "true", "commit_within": "5000"}
        spark.sql(query).write.format("solr").options(**opts).mode("overwrite").save()
    
    def upsert_text_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "text_general")
        
    def upsert_double_field(self, collection, field_name):
        self.upsert_field(collection, field_name, "pdouble")
        
    def upsert_field(self, collection, field_name, type):
        delete_field = {"delete-field":{ "name": field_name }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=delete_field)
        add_field = {"add-field":{ "name": field_name, "type": type, "stored": "true",
                                  "indexed": "true", "multiValued": "false" }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=add_field)
    
    def add_documents(self, collection, docs):
        print(f"\nAdding Documents to '{collection}' collection")
        return requests.post(f"{SOLR_URL}/{collection}/update?commit=true", json=docs).json()

    def enable_ltr(self, collection_name):
        collection_config_url = f'{SOLR_URL}/{collection_name}/config'

        del_ltr_query_parser = { "delete-queryparser": "ltr" }
        add_ltr_q_parser = {
        "add-queryparser": {
            "name": "ltr",
                "class": "org.apache.solr.ltr.search.LTRQParserPlugin"
            }
        }

        print(f"Adding LTR QParser for {collection_name} collection")
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

        print(f"Adding LTR Doc Transformer for {collection_name} collection")
        response = requests.post(collection_config_url, json=del_ltr_transformer)
        response = requests.post(collection_config_url, json=add_transformer)
        self.print_status(response.json())    

    def delete_feature_store(self, collection, name):
        return requests.delete(f'{SOLR_URL}/{collection}/schema/feature-store/{name}')

    def create_feature_store(self, collection, features):
        return requests.put(f'{SOLR_URL}/{collection}/schema/feature-store', json=features)
    
    def search(self, collection, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{collection}/select", json=request, data=data)

    def docs(self, response):
        return response.json()["response"]["docs"]
        
    def docs_as_html(self, response):
        return str(response.json()["response"]["docs"]).replace('\\n', '').replace(", '", ",<br/>'")
    
    def spell_check(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/spell", json=request)
    
    def log_query(self, collection, featureset, ids, options={}, id_field='id'):
        efi = ' '.join(f'efi.{k}="{v}"' for k, v in options.items())
        params = {
            'fl': f"{id_field},[features store={featureset} {efi}]",
            'q': "{{!terms f={}}}{}".format(id_field, ','.join(ids)) if ids else "*:*",
            'rows': 1000,
            'wt': 'json'
        }
        resp = self.search(collection, data=params)
        docs = resp.json()['response']['docs']
        # Clean up features to consistent format
        for d in docs:
            features = list(map(lambda f : float(f.split('=')[1]), d['[features]'].split(',')))
            d['ltr_features'] = features

        return docs
