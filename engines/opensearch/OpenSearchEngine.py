import base64
import json

import requests

from engines.Engine import Engine
import engines.opensearch.config as config
from engines.opensearch.OpenSearchCollection import OpenSearchCollection

class OpenSearchEngine(Engine):
    def __init__(self, os_url=config.OPENSEARCH_URL,
                 access_key=None, access_secret=None):
        self.os_url = os_url
        self.__headers = {"Concent-Type": "application/json"}
        
        if access_key and access_secret:
            token = base64.b64encode(f"{access_key}:{access_secret}".encode()).decode()
            self.__headers["Authorization"] = f"ApiKey {token}"
            self.__access_key = access_key
            self.__access_secret = access_secret
            print(access_secret)
            print(token)
        super().__init__("opensearch")

    def health_check(self):
        status = requests.get(f"{self.os_url}/_cluster/health",
                              headers=self.__headers).json()["status"] in ["green", "yellow"]
        if status:
            print("OpenSearch engine is online")
        return status
    
    def print_status(self, response):
        "Prints the resulting status of a search engine request"
        print(json.dumps(response, indent=2))

    def create_collection(self, name, log=False):
        print(f'Wiping "{name}" collection')
        response = requests.delete(f"{self.os_url}/{name}",
                                   headers=self.__headers).json()

        print(f'Creating "{name}" collection')
        collection = self.get_collection(name)
        request = config.SCHEMAS[name]["schema"] if name in config.SCHEMAS else {}
        response = requests.put(f"{self.os_url}/{name}",
                                headers=self.__headers, json=request).json()
        if log: 
            print("Post:", json.dumps(f"{self.os_url}/{name}"))
            print("Post heads:", json.dumps(f"{self.__headers}/{name}"))
            print("Schema:", json.dumps(request, indent=2))
        if log: 
            print("Status:", json.dumps(response, indent=2))
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        id_field = config.SCHEMAS.get(name, {}).get("id_field", "_id")
        return OpenSearchCollection(name, id_field, self.os_url, self.__access_key,
                                    self.__access_secret, self.__headers)