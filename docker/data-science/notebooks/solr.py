import requests
import os
from IPython.display import display,HTML
from pyspark.sql import SparkSession

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
            case "products":
                self.upsert_text_field(collection, "upc")
                self.upsert_text_field(collection, "name")
                self.upsert_text_field(collection, "manufacturer")
                self.upsert_text_field(collection, "shortDescription")
                self.upsert_text_field(collection, "longDescription")
            case _:
                pass

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

    def upsert_text_field(self, collection, field_name):
        delete_field = {"delete-field":{ "name":field_name }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=delete_field).json()
        add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true", "multiValued":"false" }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=add_field).json()

    def add_documents(self, collection, docs):
        print(f"\nAdding Documents to '{collection}' collection")
        return requests.post(f"{SOLR_URL}/{collection}/update?commit=true", json=docs).json()

    def search(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/select", json=request)

    def docs(self, response):
        return response.json()["response"]["docs"]
        
    def docs_as_html(self, response):
        return str(response.json()["response"]["docs"]).replace('\\n', '').replace(", '", ",<br/>'")