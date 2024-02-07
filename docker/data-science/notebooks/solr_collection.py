import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from env import *

class SolrCollection:
    def __init__(self, name):
        #response = requests.get(f"{SOLR_COLLECTIONS_URL}/?action=LIST")
        #print(response)
        #collections = response.json()["collections"]
        #if name.lower() not in [s.lower() for s in collections]:
        #    raise ValueError(f"Collection name invalid. '{name}' does not exists.")
        self.name = name
        
    def commit(self):
        return requests.post(f"{SOLR_URL}/{self.name}/update?commit=true")

    def write_from_csv(self, file, more_opts=False):
        print(f"Loading {self.name}")
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
        reader = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        if more_opts:
            reader = reader.option("charset", "utf-8").option("quote", "\"").option("escape", "\"").option("multiLine","true").option("delimiter", ",")
        csv_df = reader.load(file)
        if more_opts and "category" in more_opts:
            # We can rely on automatic generation of IDs, or we can create them ourselves. 
            # If we do it, comment out previous line
            # .withColumn("id", concat(col("category"), lit("_") col("id")))
            csv_df = csv_df.withColumn("category", lit(more_opts.get("category"))).drop("id")
        print(f"{self.name} Schema: ")
        csv_df.printSchema()
        options = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                   "gen_uniq_key": "true", "commit_within": "5000"}
        csv_df.write.format("solr").options(**options).mode("overwrite").save()
        self.commit()
        print("Status: Success")
        self.commit()
    
    def write_from_dataframe(self, dataframe):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        dataframe.write.format("solr").options(**opts).mode("overwrite").save()
        self.commit()
    
    def write_from_sql(self, query, spark=SparkSession.builder.appName("AIPS").getOrCreate()):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        spark.sql(query).write.format("solr").options(**opts).mode("overwrite").save()
        self.commit()
    
    def add_documents(self, docs, commit=True):
        print(f"\nAdding Documents to '{self.name}' collection")
        response = requests.post(f"{SOLR_URL}/{self.name}/update?commit={commit}", json=docs).json()
        if (commit):
            self.commit()
        return response
    
    def commit(self):
        #Improve functionality
        requests.post(f"{SOLR_URL}/{self.name}/update?commit=true").json()
        
    def write(self, docs):
        return self.add_documents(docs)
    
    def search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()
