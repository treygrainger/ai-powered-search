import requests
from env import *
import time

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
        time.sleep(10) # temporary

    def write(self, dataframe):
        opts = {"zkhost": AIPS_ZK_HOST, "collection": self.name,
                "gen_uniq_key": "true", "commit_within": "5000"}
        dataframe.write.format("solr").options(**opts).mode("overwrite").save()
        self.commit()
    
    def add_documents(self, docs, commit=True):
        print(f"\nAdding Documents to '{self.name}' collection")
        response = requests.post(f"{SOLR_URL}/{self.name}/update?commit=true", json=docs).json()
        if commit:
            self.commit()
        return response
    
    def search(self, request=None, data=None):
        return requests.post(f"{SOLR_URL}/{self.name}/select", json=request, data=data).json()
    
    def delete_feature_store(self, store_name):
        return requests.delete(f"{SOLR_URL}/{self.name}/schema/feature-store/{store_name}")