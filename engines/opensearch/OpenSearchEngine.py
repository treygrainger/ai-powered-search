import json

import requests

from engines.Engine import AdvancedFeatures, Engine
from engines.opensearch.config import OPENSEARCH_URL, SCHEMAS
from engines.opensearch.OpenSearchCollection import OpenSearchCollection

STATUS_URL = f"{OPENSEARCH_URL}/_cluster/health"

class OpenSearchEngine(Engine):
    def __init__(self):
        super().__init__("OpenSearch")

    def get_supported_advanced_features(self):
        return [AdvancedFeatures.LTR]

    def health_check(self):
        status = requests.get(STATUS_URL).json()["status"] in ["green", "yellow"]
        if status: print("OpenSearch engine is online")
        return status
    
    def does_collection_exist(self, name):
        response = requests.get(f"{STATUS_URL}/{name}")
        return response.status_code == 200
    
    def is_collection_healthy(self, name, expected_count, log=False):
        exists = self.does_collection_exist(name)
        document_count = self.get_collection(name).get_document_count()
        if log: print(exists, document_count)
        return exists and document_count  == expected_count

    def print_status(self, response):
        print(json.dumps(response, indent=2))
        pass

    def create_collection(self, name, force_rebuild=True, log=False):
        if force_rebuild:
            print(f'Wiping "{name}" collection')
            response = requests.delete(f"{OPENSEARCH_URL}/{name}").json()

        print(f'Creating "{name}" collection')
        collection = self.get_collection(name)
        request = SCHEMAS[name]["schema"] if name in SCHEMAS else {}
        response = requests.put(f"{OPENSEARCH_URL}/{name}", json=request).json()
        if log: print("Schema:", json.dumps(request, indent=2))
        if log: print("Status:", json.dumps(response, indent=2))
        return collection

    def get_collection(self, name):
        "Returns initialized object for a given collection"
        id_field = SCHEMAS.get(name, {}).get("id_field", "_id")
        return OpenSearchCollection(name, id_field)
    
    def cleanup_querygroup_id_error_bug(self):
        resp = requests.put("http://aips-opensearch:9200/_cluster/settings",
                            json={"persistent" : {"logger.org.opensearch.wlm.QueryGroupTask": "ERROR"}})
        return resp.json()
