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
name = None

class SolrCollection:
    def __init__(self, name):
        self.name = name

    def write_from_csv(self, file, more_opts=False):
        print(f"Loading {name}")
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        if more_opts:
            reader = reader.option("charset", "utf-8").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("delimiter", ",")
        csv_df = reader.load(file)
        if more_opts:
            # We can rely on automatic generation of IDs, or we can create them ourselves. 
            # If we do it, comment out previous line
            # .withColumn("id", concat(col("category"), lit("_") col("id")))
            csv_df = csv_df.withColumn("category", lit(name)).drop("id")
        print(f"{name} Schema: ")
        csv_df.printSchema()
        options = {"zkhost": AIPS_ZK_HOST, "collection": name,
                   "gen_uniq_key": "true", "commit_within": "5000"}
        csv_df.write.format("solr").options(**options).mode("overwrite").save()
        print("Status: Success")

    def write_from_csv(self, file):
        print(f"Loading {name}")
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
        print(f"{name} Schema: ")
        csv_df.printSchema()
        options = {"zkhost": AIPS_ZK_HOST, "collection": name,
                   "gen_uniq_key": "true", "commit_within": "5000"}
        csv_df.write.format("solr").options(**options).mode("overwrite").save()
        print("Status: Success")
    
    def write_from_dataframe(self, dataframe):
        opts = {"zkhost": "aips-zk", "collection": name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        dataframe.write.format("solr").options(**opts).mode("overwrite").save()
        
    def write(self, docs):
        print(f"\nAdding Documents to '{self.name}' collection")
        return requests.post(f"{SOLR_URL}/{self.name}/update?commit=true", json=docs).json()
    
    def search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()