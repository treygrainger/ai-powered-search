import aips.environment as environment
from aips.environment import SOLR_URL
from engines.solr import SolrLTR, SolrSemanticKnowledgeGraph, SolrEntityExtractor, SolrSparseSemanticSearch
from engines.solr.SolrEngine import SolrEngine

import os
from IPython.display import display,HTML
import pandas
import re
import requests

engine_type_map = {"SOLR": SolrEngine()}

def get_engine():
    return engine_type_map[environment.get("AIPS_SEARCH_ENGINE", "SOLR")]

def set_engine(engine_name):    
    engine_name = engine_name.upper()
    if engine_name not in engine_type_map:
        raise ValueError(f"No search engine implementation found for {engine_name}")
    else:
        environment.set("AIPS_SEARCH_ENGINE", engine_name)

def get_ltr_engine(collection):
    return SolrLTR(collection)

def get_semantic_knowledge_graph(collection):
    return SolrSemanticKnowledgeGraph(collection)

def get_entity_extractor(collection):
    return SolrEntityExtractor(collection)

def get_sparse_semantic_search():
    return SolrSparseSemanticSearch()

def healthcheck():
    try:
        if get_engine().health_check():
            print("Solr is up and responding.")
            print("Zookeeper is up and responding.\n")
            print("All Systems are ready. Happy Searching!")
    except:
        print("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")

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

def remove_new_lines(data):
    return str(data).replace('\\n', '').replace('\\N', '')

def as_html(data):
    return remove_new_lines(data).replace(", '", ",<br/>'")

def display_search(query, documents):
    doc_html = as_html(documents)
    display(HTML(f"<strong>Query</strong>: <i>{query}</i><br/><br/><strong>Results:</strong>"))
    display(HTML(doc_html))
   
def display_product_search(query, documents):
    rendered_html = render_search_results(query, documents)
    display(HTML((rendered_html)))
    
def render_search_results(query, results):
    file_path = os.path.dirname(os.path.abspath(__file__))
    search_results_template_file = os.path.join(file_path + "/../data/retrotech/templates/", "search-results.html")
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
            rendered += results_template.replace("${NAME}", result.get("name", "UNKNOWN")) \
                .replace("${MANUFACTURER}", result.get("manufacturer", "UNKNOWN")) \
                .replace("${DESCRIPTION}", remove_new_lines(result.get("short_description", ""))) \
                .replace("${IMAGE_URL}", "../data/retrotech/images/" + \
                         (result['upc'] if \
                          ('upc' in result and os.path.exists(file_path + "/../data/retrotech/images/" + result['upc'] + ".jpg") \
                         ) else "unavailable") + ".jpg")

            rendered += separator_template
    return rendered

def fetch_products(doc_ids):
    doc_ids = ["%s" % doc_id for doc_id in doc_ids]
    query = "upc:( " + " OR ".join(doc_ids) + " )"
    params = {'q':  query, 'wt': 'json', 'rows': len(doc_ids)}
    resp = requests.get(f"{SOLR_URL}/products/select", params=params)
    df = pandas.DataFrame(resp.json()['response']['docs'])
    df['upc'] = df['upc'].astype('int64')

    df.insert(0, 'image', df.apply(lambda row: "<img height=\"100\" src=\"" + img_path_for_upc(row['upc']) + "\">", axis=1))

    return df

def render_judged(products, judged, grade_col='ctr', label=""):
    """ Render the computed judgments alongside the productns and description data"""
    w_prods = judged.merge(products, left_on='doc_id', right_on='upc', how='left')

    w_prods = w_prods[[grade_col, 'image', 'upc', 'name', 'short_description']]

    return HTML(f"<h1>{label}</h1>" + w_prods.to_html(escape=False))