import requests
from env import SOLR_URL

url_params = """
json.nl=map&sort=popularity%20desc&matchText=true&echoParams=all&
fl=id,type,canonical_form,surface_form,name,country:countrycode_s,
admin_area:admin_code_1_s,popularity,*_p,semantic_function"""

class TextTagger:
    def __init__(self, collection_name):
        self.collection_name = collection_name

    def tag_query(self, query, url_params=url_params):
        return requests.post(f"{SOLR_URL}/{self.collection_name}/tag?{url_params}", query).json()