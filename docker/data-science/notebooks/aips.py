
from IPython.display import display,HTML
import os
import pandas as pd
from pyspark.sql import SparkSession
import re
import requests
from solr import SolrEngine
from env import SOLR_URL, AIPS_ZK_HOST
ENGINE = SolrEngine()

def get_engine():
    return ENGINE

def healthcheck():
    try:
        if (get_engine().health_check()):
            print ("Solr is up and responding.")
            print ("Zookeeper is up and responding.\n")
            print ("All Systems are ready. Happy Searching!")
    except:
        print ("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")

def print_status(solr_response):
      print("Status: Success" if solr_response["responseHeader"]["status"] == 0 else "Status: Failure; Response:[ " + str(solr_response) + " ]" )

def num2str(number):
    return str(round(number,4)) #round to 4 decimal places for readibility

def vec2str(vector):
    return "[" + ", ".join(map(num2str,vector)) + "]"

def tokenize(text):
    return text.replace(".","").replace(",","").lower().split()

def img_path_for_upc(upc):
    # file_path = os.path.dirname(os.path.abspath(__file__))
    expected_jpg_path = f"../data/retrotech/images/{upc}.jpg"
    unavailable_jpg_path = "../data/retrotech/images/unavailable.jpg"
    return expected_jpg_path if os.path.exists(expected_jpg_path) else unavailable_jpg_path

def as_html(data):
    return str(data).replace('\\n', '').replace(", '", ",<br/>'")

def display_search(query, documents):
    doc_html = as_html(documents)
    display(HTML(f"<strong>Query</strong>: <i>{query}</i><br/><br/><strong>Results:</strong>"))
    display(HTML(doc_html))
   
def display_product_search(query, documents):
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
        for result in documents:
            rendered += results_template.replace("${NAME}", result['name'] if 'name' in result else "UNKNOWN") \
                .replace("${MANUFACTURER}", result['manufacturer'] if 'manufacturer' in result else "UNKNOWN") \
                .replace("${DESCRIPTION}", result['shortDescription'] if 'shortDescription' in result else "") \
                .replace("${IMAGE_URL}", "../data/retrotech/images/" + \
                         (result['upc'] if \
                          ('upc' in result and os.path.exists(file_path + "/data/retrotech/images/" + result['upc'] + ".jpg") \
                         ) else "unavailable") + ".jpg")

            rendered += separator_template
    display(HTML((rendered)))
    
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

def create_view(collection, view_name, spark=None):
    if not spark:
        spark = SparkSession.builder.appName("AIPS").getOrCreate()
    opts = {"zkhost": AIPS_ZK_HOST, "collection": collection.name}    
    spark.read.format("solr").options(**opts).load().createOrReplaceTempView(view_name)
  
def fetch_products(doc_ids):
    doc_ids = ["%s" % doc_id for doc_id in doc_ids]
    query = "upc:( " + " OR ".join(doc_ids) + " )"
    params = {'q':  query, 'wt': 'json', 'rows': len(doc_ids)}
    resp = requests.get(f"{SOLR_URL}/products/select", params=params)
    df = pd.DataFrame(resp.json()['response']['docs'])
    df['upc'] = df['upc'].astype('int64')

    df.insert(0, 'image', df.apply(lambda row: "<img height=\"100\" src=\"" + img_path_for_upc(row['upc']) + "\">", axis=1))

    return df

def render_judged(products, judged, grade_col='ctr', label=""):
    """ Render the computed judgments alongside the productns and description data"""
    w_prods = judged.merge(products, left_on='doc_id', right_on='upc', how='left')

    w_prods = w_prods[[grade_col, 'image', 'upc', 'name', 'shortDescription']]

    return HTML(f"<h1>{label}</h1>" + w_prods.to_html(escape=False))

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
