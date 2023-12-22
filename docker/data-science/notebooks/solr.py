import requests
import os
import re

#AIPS_SOLR_HOST = "aips-solr"
AIPS_SOLR_HOST = "localhost"
AIPS_SOLR_PORT = os.getenv('AIPS_SOLR_PORT') or '8983'

SOLR_URL = f'http://{AIPS_SOLR_HOST}:{AIPS_SOLR_PORT}/solr'
SOLR_COLLECTIONS_URL = f'{SOLR_URL}/admin/collections'

class SolrAdapter:
    def __init__(self):
        pass

    def print_status(self, solr_response):
        print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

    def create_collection(self):
        self.create_collection("")

    def create_collection(self, collection_name):
        #Wipe previous collection
        wipe_collection_params = [
            ('action', "delete"),
            ('name', collection_name)
        ]

        print(f"Wiping '{collection_name}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=wipe_collection_params).json()

        #Create collection
        create_collection_params = [
            ('action', "CREATE"),
            ('name', collection_name),
            ('numShards', 1),
            ('replicationFactor', 1) ]

        print(create_collection_params)

        print(f"Creating '{collection_name}' collection")
        response = requests.post(SOLR_COLLECTIONS_URL, data=create_collection_params).json()
        
        self.apply_schema_for_collection(collection_name)
        self.print_status(response)

    def apply_schema_for_collection(self, collection_name):
        match collection_name:
            case "cat_in_the_hat":
                self.upsert_text_field(collection_name, "title")
                self.upsert_text_field(collection_name, "description")
            case _:
                pass

    def upsert_text_field(self, collection_name, field_name):
        delete_field = {"delete-field":{ "name":field_name }}
        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=delete_field).json()
        add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true", "multiValued":"false" }}
        response = requests.post(f"{SOLR_URL}/{collection_name}/schema", json=add_field).json()

    def add_documents(self, collection, docs):
        return requests.post(f"{SOLR_URL}/{collection}/update?commit=true", json=docs).json()

    def search(self, collection, request):
        return requests.post(f"{SOLR_URL}/{collection}/select", json=request)

    def format_documents(self, response):
        return str(response.json()["response"]["docs"]).replace('\\n', '').replace(", '", ",<br/>'")