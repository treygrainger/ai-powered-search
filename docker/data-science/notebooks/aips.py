import requests
import os
import re

#AIPS_SOLR_HOST = "aips-solr"
#AIPS_NOTEBOOK_HOST="aips-notebook"
#AIPS_ZK_HOST="aips-zk"
AIPS_SOLR_HOST = "localhost"
AIPS_NOTEBOOK_HOST="localhost"
AIPS_ZK_HOST="localhost"
AIPS_SOLR_PORT = "8983"
AIPS_NOTEBOOK_PORT="8888"
AIPS_ZK_PORT="2181"

solr_url = 'http://' + AIPS_SOLR_HOST + ':' + AIPS_SOLR_PORT + '/solr/'
solr_collections_api = solr_url + 'admin/collections'

def healthcheck():
  import requests

  status_url = solr_url + 'admin/zookeeper/status'

  try:
    response = requests.get(status_url).json()
    if (response["responseHeader"]["status"] == 0):
      print ("Solr is up and responding.")
      print ("Zookeeper is up and responding.\n")
      print ("All Systems are ready. Happy Searching!")
  except:
      print ("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")

def print_status(solr_response):
  print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

def create_collection(collection_name):
    #Wipe previous collection
  wipe_collection_params = [
      ('action', "delete"),
      ('name', collection_name)
  ]

  print("Wiping '" + collection_name + "' collection")
  response = requests.post(solr_collections_api, data=wipe_collection_params).json()

  #Create collection
  create_collection_params = [
      ('action', "CREATE"),
      ('name', collection_name),
      ('numShards', 1),
      ('replicationFactor', 1) ]

  print(create_collection_params)

  print("Creating " + collection_name + "' collection")
  response = requests.post(solr_collections_api, data=create_collection_params).json()
  print_status(response)

def enable_ltr(collection_name):

    collection_config_url = solr_url + collection_name + "/config"

    del_ltr_query_parser = { "delete-queryparser": "ltr" }
    add_ltr_q_parser = {
     "add-queryparser": {
        "name": "ltr",
            "class": "org.apache.solr.ltr.search.LTRQParserPlugin"
        }
    }

    print("Del/Adding LTR QParser for " + collection_name + "' collection")
    response = requests.post(collection_config_url, json=del_ltr_query_parser).json()
    response = requests.post(collection_config_url, json=add_ltr_q_parser).json()
    print_status(response)

    del_ltr_transformer = { "delete-transformer": "features" }
    add_transformer =  {
      "add-transformer": {
        "name": "features",
        "class": "org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory",
        "fvCacheName": "QUERY_DOC_FV"
    }}

    print("Adding LTR Doc Transformer for " + collection_name + "' collection")
    response = requests.post(collection_config_url, json=del_ltr_transformer).json()
    response = requests.post(collection_config_url, json=add_transformer).json()
    print_status(response)
    
def delete_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

def upsert_text_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"text_general", "stored":"true", "indexed":"true", "multiValued":"false" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)

def upsert_double_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"pdouble", "stored":"true", "indexed":"true", "multiValued":"false" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)
    
def upsert_integer_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"pint", "stored":"true", "indexed":"true", "multiValued":"false" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)

def upsert_keyword_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"string", "stored":"true", "indexed":"true", "multiValued":"true", "docValues":"true" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)
    
def upsert_string_field(collection_name, field_name):
    #clear out old field to ensure this function is idempotent
    delete_field = {"delete-field":{ "name":field_name }}
    response = requests.post(solr_url + collection_name + "/schema", json=delete_field).json()

    print("Adding '" + field_name + "' field to collection")
    add_field = {"add-field":{ "name":field_name, "type":"string", "stored":"true", "indexed":"false", "multiValued":"false", "docValues":"true" }}
    response = requests.post(solr_url + collection_name + "/schema", json=add_field).json()
    print_status(response)
    
def num2str(number):
  return str(round(number,4)) #round to 4 decimal places for readibility

def vec2str(vector):
  return "[" + ", ".join(map(num2str,vector)) + "]"

def tokenize(text):
  return text.replace(".","").replace(",","").lower().split()

def render_search_results(query, results):
    file_path = os.path.dirname(os.path.abspath(__file__))
    search_results_template_file = os.path.join(file_path + "/data/retrotech/templates/", "search-results.html")
    with open(search_results_template_file) as file:
        file_content = file.read()

        template_syntax = "<!-- BEGIN_TEMPLATE[^>]*-->(.*)<!-- END_TEMPLATE[^>]*-->"
        header_template = re.sub(template_syntax, "", file_content, flags=re.S)

        results_template_syntax = "<!-- BEGIN_TEMPLATE: SEARCH_RESULTS -->(.*)<!-- END_TEMPLATE: SEARCH_RESULTS[^>]*-->"
        x = re.search(results_template_syntax, file_content, flags=re.S)
        results_template = x.group(1)

        separator_template_syntax = "<!-- BEGIN_TEMPLATE: SEPARATOR -->(.*)<!-- END_TEMPLATE: SEPARATOR[^>]*-->"
        x = re.search(separator_template_syntax, file_content, flags=re.S)
        separator_template = x.group(1)

        rendered = header_template.replace("${QUERY}", query)
        for result in results:
            rendered += results_template.replace("${NAME}", result['name'] if 'name' in result else "UNKNOWN") \
                .replace("${MANUFACTURER}", result['manufacturer'] if 'manufacturer' in result else "UNKNOWN") \
                .replace("${DESCRIPTION}", result['shortDescription'] if 'shortDescription' in result else "") \
                .replace("${IMAGE_URL}", "../data/retrotech/images/" + \
                         (result['upc'] if \
                          ('upc' in result and os.path.exists(file_path + "/data/retrotech/images/" + result['upc'] + ".jpg") \
                         ) else "unavailable") + ".jpg")

            rendered += separator_template

        return rendered

"""
class environment:

  self._AIPS_SOLR_HOST = "aips-solr"
  self._AIPS_NOTEBOOK_HOST="aips-notebook"
  self._AIPS_ZK_HOST="aips-zk"
  self._AIPS_SOLR_PORT = "8983"
  self._AIPS_NOTEBOOK_PORT="8888"
  self._AIPS_ZK_PORT="2181"
  self._AIPS_SOLR_URL=''
  self._AIPS_SOLR_COLLECTIONS_API=''

  if "AIPS_HOST" in os.environ:
    self._AIPS_SOLR_HOST=os.environ['AIPS_HOST']
    self._AIPS_NOTEBOOK_HOST=os.environ['AIPS_HOST']
    self._AIPS_ZK_HOST=os.environ['AIPS_HOST']

  if "AIPS_SOLR_HOST" in os.environ:
    self._AIPS_SOLR_HOST=os.environ['AIPS_SOLR_HOST']

  if "AIPS_NOTEBOOK_HOST" in os.environ:
    self._AIPS_NOTEBOOK_HOST=os.environ['AIPS_NOTEBOOK_HOST']

  if "AIPS_ZK_HOST" in os.environ:
    self._AIPS_ZK_HOST=os.environ['AIPS_ZK_HOST']

  if "AIPS_SOLR_PORT" in os.environ:
    self._AIPS_SOLR_PORT=os.environ['AIPS_SOLR_PORT']

  if "AIPS_NOTEBOOK_HOST" in os.environ:
    self._AIPS_NOTEBOOK_PORT=os.environ['AIPS_NOTEBOOK_PORT']

  if "AIPS_ZK_HOST" in os.environ:
    self._AIPS_ZK_PORT=os.environ['AIPS_ZK_PORT']

  self._AIPS_SOLR_URL = 'http://' + AIPS_SOLR_HOST + ':' + AIPS_SOLR_PORT + '/solr/'
  self._AIPS_SOLR_COLLECTIONS_API = AIPS_SOLR_URL + 'admin/collections'

  @property
  def SOLR_HOST(self):
    return self._AIPS_SOLR_HOST

  @property
  def NOTEBOOK_HOST(self):
    return self._AIPS_SOLR_HOST

  @property
  def ZK_HOST(self):
    return self._AIPS_ZK_HOST

  @property
  def SOLR_PORT(self):
    return self._AIPS_SOLR_PORT

  @property
  def NOTEBOOK_PORT(self):
    return self._AIPS_NOTEBOOK_PORT

  @property
  def ZK_PORT(self):
    return self._AIPS_ZK_PORT

  @property
  def SOLR_URL(self):
    return self._AIPS_SOLR_URL

  @property
  def SOLR_COLLECTIONS_API(self):
    return self._AIPS_SOLR_COLLECTIONS_API
"""
