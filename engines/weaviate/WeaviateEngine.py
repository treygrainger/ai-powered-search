import json
import time

import requests

from engines.Engine import AdvancedFeatures, Engine
from engines.weaviate.config import WEAVIATE_URL, SCHEMAS
from engines.weaviate.WeaviateCollection import WeaviateCollection

STATUS_URL = f"{WEAVIATE_URL}/v1/.well-known/live"

#https://weaviate.io/developers/weaviate/introduction
#https://weaviate.io/developers/weaviate/concepts/search#search-process

class WeaviateEngine(Engine):
    def __init__(self):
        super().__init__("weaviate")

    def does_collection_exist(self, name):
        response = requests.get(f"{WEAVIATE_URL}/v1/schema/{name.capitalize()}")
        return response.status_code == 200
    
    def get_supported_advanced_features(self):
        return [AdvancedFeatures.LTR]

    def health_check(self, log=False, retries=0):
        status = False
        for i in range(retries + 1):
            try:
                status = requests.get(STATUS_URL).status_code == 200
                if log: print(f"Weaviate is {'online' if status else 'offline'}")
                if status:
                    break
                time.sleep(5)
            except:
                if i == retries:
                    if log: print("Weaviate failed the health check.")
                    status = False
                else: 
                    time.sleep(5)
        return status
    
    def print_status(self, response):
        if isinstance(response, requests.Response):
            status = response.status_code == 200
        else:
            status = response        
        print(f"Status: {'Success' if status else 'Failure'}")

    def create_collection(self, name, log=False):
        print(f'Wiping "{name}" collection')
        requests.delete(f"{WEAVIATE_URL}/v1/schema/{name.capitalize()}")

        print(f'Creating "{name}" collection')
        request = SCHEMAS[name]["schema"] if name in SCHEMAS else {}
        response = requests.post(f"{WEAVIATE_URL}/v1/schema", json=request)
        if log: 
            print("Schema:", json.dumps(request, indent=2))
            print("Status:", response)
            print("Response:", response.json())
        collection = self.get_collection(name)
        self.print_status(response)
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        return WeaviateCollection(name.capitalize())