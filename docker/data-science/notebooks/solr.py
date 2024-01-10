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
            case _:
                pass
    def populate_collection_from_csv(self, collection, file):
        self.populate_collection_from_csv(collection, file, False)
        
    def populate_collection_from_csv(self, collection, file, more_opts):
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
        delete_field = {"delete-field":{ "name":field_name }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=delete_field).json()
        add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true", "multiValued":"false" }}
        response = requests.post(f"{SOLR_URL}/{collection}/schema", json=add_field).json()

    def upsert_boosts_field(self, collection_name, field_name, field_type_name="boosts"):
        delete_field = {"delete-field":{ "name":field_name }}
        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=delete_field).json()

        self.upsert_boosts_field_type(collection_name, field_type_name);
        
        print(f"Adding '{field_name}' field to collection")
        add_field = {"add-field":{ "name":field_name, "type":"boosts", "stored":"true", "indexed":"true", "multiValued":"true" }}
        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=add_field).json()

        self.print_status(response)

    def upsert_boosts_field_type(self, collection_name, field_type_name):
        delete_field_type = {"delete-field-type":{ "name":field_type_name }}
        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=delete_field_type).json()

        print(f"Adding '{field_type_name}' field type to collection")
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

        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=add_field_type).json()
        self.print_status(response)

    def add_documents(self, collection, docs):
        print(f"\nAdding Documents to '{collection}' collection")
        return requests.post(f"{SOLR_URL}/{collection}/update?commit=true", json=docs).json()

    def docs(self, response):
        return response.json()["response"]["docs"]
        
    def docs_as_html(self, response):
        return str(response.json()["response"]["docs"]).replace('\\n', '').replace(", '", ",<br/>'")
    
    def search(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/select", json=request)

    def spell_check(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/spell", json=request)
