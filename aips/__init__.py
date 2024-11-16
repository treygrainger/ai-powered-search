from gc import collect
import aips.environment as environment
from engines.solr import SolrLTR, SolrSemanticKnowledgeGraph, SolrEntityExtractor, SolrSparseSemanticSearch
from engines.solr.SolrEngine import SolrEngine
from engines.solr.SolrCollection import SolrCollection

from engines.opensearch.OpenSearchCollection import OpenSearchCollection
from engines.opensearch.OpenSearchEngine import OpenSearchEngine
from engines.opensearch.OpenSearchLTR import OpenSearchLTR
from engines.opensearch.OpenSearchSparseSemanticSearch import OpenSearchSparseSemanticSearch

import os
from IPython.display import display, HTML
import pandas
import re

engine_type_map = {"SOLR": SolrEngine(),
                   "OPENSEARCH": OpenSearchEngine()}

def get_engine(override=None):
    engine_name = override.upper() if override else environment.get("AIPS_SEARCH_ENGINE", "SOLR")
    return engine_type_map[engine_name]

def set_engine(engine_name):
    engine_name = engine_name.upper()
    if engine_name not in engine_type_map:
        raise ValueError(f"No search engine implementation found for {engine_name}")
    else:
        environment.set("AIPS_SEARCH_ENGINE", engine_name)

def get_ltr_engine(collection):    
    ltr_engine_map = {SolrCollection: SolrLTR,
                      OpenSearchCollection: OpenSearchLTR}
    return ltr_engine_map[type(collection)](collection)

def get_semantic_knowledge_graph(collection):
    return SolrSemanticKnowledgeGraph(get_engine("solr").get_collection(collection.name))

def get_entity_extractor(collection):
    return SolrEntityExtractor(get_engine("solr").get_collection(collection.name))

def get_sparse_semantic_search():
    SSS_map = {SolrEngine: SolrSparseSemanticSearch,
               OpenSearchEngine: OpenSearchSparseSemanticSearch}
    return SSS_map[type(get_engine())]()

def healthcheck():
    try:
        if get_engine().health_check():
            print("All Systems are ready. Happy Searching!")
    except:
        print("Error! One or more containers are not responding.\nPlease follow the instructions in Appendix A.")
        
def num2str(number):
    return str(round(number,4)) #round to 4 decimal places for readibility

def vec2str(vector):
    return "[" + ", ".join(map(num2str,vector)) + "]"

def tokenize(text):
    return text.replace(".","").replace(",","").lower().split()

def get_executing_notebook_path():
    return globals().get("__vsc_ipynb_file__", #only exists during a remote vscode kernel
                         globals().get("_dh", [None])[0])

def images_directory():    
    path = get_executing_notebook_path()
    if path:
        relative = os.path.relpath(os.environ.get("HOME"), path)
    else:
        relative = "../.."
    return f"{relative}/data/retrotech/images"

def img_path_for_upc(product):
    directory = images_directory()
    file = product.get("upc", "no-upc")
    if not os.path.exists(f"data/retrotech/images/{file}.jpg"):
        file = "unavailable"
    return f"{directory}/{file}.jpg"

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
    display(HTML(rendered_html))
    
def render_search_results(query, results):
    search_results_template_file = os.path.join("data/retrotech/templates/", "search-results.html")
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

        rendered = header_template.replace("${QUERY}", query.replace('"', '\"'))
        for result in results:
            image_url = img_path_for_upc(result)
            rendered += results_template.replace("${NAME}", result.get("name", "UNKNOWN")) \
                .replace("${MANUFACTURER}", result.get("manufacturer", "UNKNOWN")) \
                .replace("${IMAGE_URL}", image_url)

            rendered += separator_template
    return rendered

def fetch_products(doc_ids):
    request = {"query": " ".join([str(id) for id in doc_ids]),
               "query_fields": ["upc"],
               "limit": len(doc_ids)}
    response = get_engine().get_collection("products").search(request)
    
    df = pandas.DataFrame(response["docs"])
    df['upc'] = df['upc'].astype('int64')
    df.insert(0, 'image', df.apply(lambda row: "<img height=\"100\" src=\"" + img_path_for_upc(row['upc']) + "\">", axis=1))
    return df

def render_judged(products, judged, grade_col='ctr', label=""):
    """ Render the computed judgments alongside the products and description data"""
    w_prods = judged.merge(products, left_on='doc_id', right_on='upc', how='left')

    style = """
        <style>img {width:125px; height:125px; }
               tr {font-size:24px; text-align:left; font-weight:normal; }
               td {padding-right:40px; text-align:left; }
               th {font-size:28px; text-align:left; }
               th:nth-child(4) {width:125px; }</style>"""
    w_prods = w_prods[[grade_col, 'upc', 'image', 'name']][:5]
    return HTML(style + 
                f"<h1>{label}</h>" + w_prods.to_html(float_format=lambda x: '%10.4f' % x,
                                                     escape=False))

#def print_s(series_data, column):
    ##pandas.set_option("display.width", 76)
    #dataframe = series_data.to_frame(name=column).sort_values(column, ascending=False)
    #merged = dataframe.merge(products, left_on='doc_id', right_on='upc', how='left')
    #print(merged.rename(columns={"upc": "doc_id"})[["doc_id", column, "name"]].set_index("doc_id"))