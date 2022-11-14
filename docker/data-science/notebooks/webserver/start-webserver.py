import sys
sys.path.append('..')
from aips import *
import threading
import webbrowser
import http.server
import requests
import json
import urllib
from staticmap import StaticMap, CircleMarker
from urllib.parse import urlparse, parse_qs
import os


FILE = 'semantic-search'
#HOST = os.getenv('HOST') or 'localhost'
#WEBSERVER_PORT = os.getenv('WEBSERVER_PORT') or 2345
#SOLR_HOST = os.getenv('SOLR_HOST') or 'aips-solr'
#SOLR_PORT = os.getenv('SOLR_PORT') or 8983
#SOLR_HOST_URL = "http://" + SOLR_HOST + ":" + str(SOLR_PORT) + "/solr"

def query_solr(collection,query):   
    response = requests.post(SOLR_URL + '/' + collection + '/select',
          {
            "type": 'POST',
            "data": json.puts(query),
            "dataType": 'json',
            "contentType": 'application/json'          
          });   

    return response

def tag_query(post_body):
    return requests.post(SOLR_URL + '/entities/tag?json.nl=map&sort=popularity%20desc&matchText=true&echoParams=all&fl=id,type,canonical_form,name,country:countrycode_s,admin_area:admin_code_1_s,popularity,*_p,command_function', post_body).text

def tag_places(post_body):
    x = json.dumps(post_body)
    return requests.post(SOLR_URL + '/reviews/select', json=post_body).text


def queryTreeToResolvedString(query_tree):
    resolved_query = ""
    for i in range(len(query_tree)):
        if (len(resolved_query) > 0):
            resolved_query += " "
        
        resolved_query += query_tree[i]['query']
        
    return resolved_query


def run_search(query_bytes):
    text = query_bytes.decode('UTF-8')
     
     #http://localhost:8983/solr/places/select?q=%2B{!edismax%20v=%22bbq^0.9191%20ribs^0.6186%20pork^0.5991%22}%20%2B{!geofilt%20d=50%20sfield=location_p%20pt=%2234.9362399,-80.8379247%22}&fl=name_s,location_p,city_s,doc_type_s,state_s&debug=true&qf=text_t
     #url = "http://localhost:8983/solr/places/select?q=%2B{!edismax%20v=%22bbq^0.9191%20ribs^0.6186%20pork^0.5991%22}%20%2B{!geofilt%20d=50%20sfield=location_p%20pt=34.9362399,-80.8379247}&qf=text_t&defType=lucene"
     #solrQuery = {"query": text, "params":{ "defType": "lucene", "qf": "name_t^100 text_t city_t^0.1 categories_t^0.01", "debug": "true"}}
     #return requests.post('http://localhost:8983/solr/places/select', json=solrQuery).text
     #solrQuery = {"query": "%2B{!edismax v=\'bbq^0.9191 ribs^0.6186 pork^0.5991'} %2B{!geofilt d=50 sfield=location_p pt=34.9362399,-80.8379247}", "params":{ "defType": "lucene", "qf": "name_t^100 text_t city_t^0.1 categories_t^0.01", "debug": "true"}}
     #return requests.post('http://localhost:8983/solr/places/select', json=json.dumps(solrQuery)).text
     #q = "%2B{!edismax%20v=%22bbq^0.9191%20ribs^0.6186%20pork^0.5991%22}%20%2B{!geofilt%20d=50%20sfield=location_p%20pt=34.9362399,-80.8379247}"
    q = urllib.parse.quote(text)
    print(q)
    #q=text.replace("+", "%2B") #so it doesn't get interpreted as space
    qf="text_t"
    defType="lucene"
     
    return requests.get(SOLR_URL + "/reviews/select?q=" + q + "&qf=" + qf + "&defType=" + defType).text
    

def process_basic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    response = {
        "resolved_query": '+{!edismax mm=100% v="' + escapeQuotesInQuery(text) + '"}'
    }
    return response


def process_semantic_query(query_bytes):
    text = query_bytes.decode('UTF-8')
    data = tag_query(query_bytes)
    tagged_response = json.loads(data)

    #loop through all documents (entities) returned
    doc_map={} # reset to empty
    if (tagged_response['response'] and tagged_response['response']['docs']):

        docs = tagged_response['response']['docs']

        for doc in docs:
            doc_map[doc['id']] = doc

        #for (d=0; d<Object.keys(docs).length; d++) {
        #  let doc = docs[d];
        #  doc_map[doc.id]=doc;
        #}
        
        #sort doc_map by popularity so first most popular always wins
        #def popularity_sort(doc_a, doc_b){
        #  return a.popularity - b.popularity;
        #}
        
        #//doc_map.sort(popularity_sort);
      #}

    query_tree = []
    tagged_query = ""
    transformed_query =""
      
    if (tagged_response['tags'] is not None):
        tags = tagged_response['tags'] 
        #//var lastStart = 0;
        lastEnd = 0
        metaData = {}
        for tag in tags:                
            #tag = tags[key]
            matchText = tag['matchText']
            

            doc_ids = tag['ids']          
            
            #pick top-ranked docid
            best_doc_id = None

            for doc_id in doc_ids:
                if (best_doc_id):
                    if (doc_map[doc_id]['popularity'] > doc_map[best_doc_id]['popularity']):
                        best_doc_id = doc_id
                else:
                    best_doc_id = doc_id


            best_doc = doc_map[best_doc_id]

            #store the unknown text as keywords
            nextText = text[lastEnd:tag['startOffset']].strip()
            if (len(nextText) > 0):  #not whitespace
                query_tree.append({ "type":"keyword", "known":False, "surface_form":nextText, "canonical_form":nextText })          
                tagged_query += " " + nextText
                transformed_query += " " + "{ type:keyword, known: false, surface_form: \"" + nextText + "\"}" 
            
            
            # store the known entity as entity
            query_tree.append(best_doc)  #this is wrong. Need the query tree to have _all_
            # interpretations available and then loop through them to resolve. TODO = fix this.

            tagged_query += " {" + matchText + "}"          
            #//transformed_query += " {type: " + best_doc.type + ", canonical_form: \"" + best_doc.canonical_form + "\"}";  
            transformed_query += json.dumps(best_doc)             
            lastEnd = tag['endOffset'] 
        

        
        if (lastEnd < len(text)):
            finalText = text[lastEnd:len(text)].strip()
            if (len(finalText) > 0):
                query_tree.append({ "type":"keyword", "known":False, "surface_form":finalText, "canonical_form":finalText })
                
                tagged_query += " " + finalText
                transformed_query += " " + "{ type:keyword, known: false, surface_form: \"" + finalText + "\"}" 
                  


    #finalquery = {"query_tree": query_tree}
    #let query = {query_tree: query_tree}; //so we can pass byref        
        
    final_query = resolveQuery(query_tree)
    #if (query != null){ //short circuit if new request has been issued
    resolved_query = queryTreeToResolvedString(query_tree)      
                    
            #UI.updateResolvedQuery(resolved_query)
        #}

    response = {
        "tagged_query": tagged_query,
        "transformed_query": transformed_query,
        "resolved_query": resolved_query,
        "tagger_data": tagged_response
    }

    return response

def resolveQuery(query_tree):
    query_tree = processCommands(query_tree)
        
    # Now process everything that is not yet resolved
    for position in range(len(query_tree)):
        item = query_tree[position];         
        if (item["type"] != "solr"): #already resolved
            if (item["type"] == "keyword"):  
                #TODO: this currently looks up ALL unknown keywords in the SKG, which isn't very smart
                #need to switch to looking up meaningful phrases in next pass. This is mostly for testing
                #at the moment, so putting up with the noise temporarily.
                categoryAndTermVector = None
                #TODO: figure out way (probably timestamp-based) to guarantee processing in order given current async nature
                solrResponse = get_category_and_term_vector_solr_response(item["surface_form"])
                categoryAndTermVector = parse_category_and_term_vector_from_solr_response(solrResponse)       
                
                #if (latestAsyncRequestID != categoryAndTermVector.asyncRequestID){
                #  return null;
                #}

                queryString = ""
                if ("term_vector" in categoryAndTermVector):
                    queryString = categoryAndTermVector["term_vector"]
                
                if ("category" in categoryAndTermVector):
                    if (len(queryString) > 0):
                        queryString += " "
                        queryString += "+doc_type:\"" + categoryAndTermVector["category"] + "\""
                    
                    
                if (len(queryString) == 0):
                    queryString = item["surface_form"] #just keep the input as a keyword

                query_tree[position] = { "type":"solr", "query": "+{!edismax v=\"" + escapeQuotesInQuery(queryString) + "\"}" }              
            elif (item["type"] == "color"):
                solrQuery = "+colors_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            elif (item["type"] == "known_item" or item["type"] == "city" or item["type"] == "event"):
                solrQuery = "+name_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            elif (item["type"] == "brand"):
                solrQuery = "+brand_s:\"" + item["canonical_form"] + "\""
                query_tree[position] = {"type":"solr", "query": solrQuery}
            else:
                query_tree[position] = {"type":"solr", "query": "+{!edismax v=\"" + escapeQuotesInQuery(item["surface_form"]) + "\"}"}
                
    return query_tree

def escapeQuotesInQuery(query):
    return query.replace('"', '\\"')

def processCommands(query_tree):
    position = 0
    while position < len(query_tree):
        item = query_tree[position]        
        # process commands. For now, going left to right and then sorting by priority when ambiguous commands occur; 
        # consider other weighting options later.
        if (item['type'] == "command"):
            commandIsResolved = False
    
            command = item['command_function']

            if (command):
                query = {"query_tree": query_tree} #pass by-ref
                commandIsResolved = eval(item['command_function']); #Careful... there is code in the docs that is being eval'd. 
                #MUST ENSURE THESE DOCS ARE SECURE, OTHERWISE THIS WILL INTRODUCE A POTENTIAL SECURITY THREAT (CODE INJECTION)
            
            #else:
                #Alert ("Error: " + query.query_tree.canonical_form + " has no command function.");
            
            if (False == commandIsResolved):
                #Bad command. Just remove for now... could alternatively keep it and run as a keyword
                query_tree.pop(position) #.splice(position,1)  

        position += 1

    return query_tree  

def cmd_popularity(query, position):
    if (len(query['query_tree']) -1 > position):
        nextEntity = query['query_tree'][position + 1]
        query['query_tree'][position] = {"type":"solr", "query": '+{!func v="mul(if(stars_i,stars_i,0),20)"}'}
        return True
    else:
        return False

def cmd_location_distance(query, position):
      
    #TODO: Notes. Need a "multi-location-hopping" resolver in here. For example,
    #hotels near bbq near haystack. This is a search for doctype=hotels, join with (doctype=restaurant AND bbq OR barbecue OR ...) filtered on distance. The first "near" requires pushing the second to a sub-query (join/graph) and then takes over as the actual location distance command.
      
    if (len(query['query_tree']) -1 > position):
        nextEntity = query['query_tree'][position + 1]
        if (nextEntity['type'] == "city"):
        
            query['query_tree'].pop(position + 1); #remove next element since we're inserting into command's position
            query['query_tree'][position] = {"type":"solr", 
                                             "query": create_geo_filter(nextEntity['location_p'], 
                                             "location_p", 50)}
            return True
         
        elif 'coordinates_pt' in nextEntity:
            query['query_tree'].pop(position + 1) #remove next element since we're inserting into command's position
            query['query_tree'][position] = {"type":"solr", 
                                             "query": create_geo_filter(nextEntity['coordinates_pt'], 
                                             "coordinates_pt", 50) }
            return True
        elif nextEntity['type'] == "event": 
            #nextEntity doesn't have coordinates on it, so try traversing to find coordinates on a parent node (i.e. a venue)
            query['query_tree'].pop(position + 1); #remove next element since we're inserting into command's position
           
            quickHack = None

            if (nextEntity['canonical_form'] == "activate conference"):
                quickHack = "activate"
           
            if (nextEntity['canonical_form'] == "haystack conference"):
                quickHack = "haystack"
           
            attemptedGraphLookup = find_location_coordinates(quickHack)

            if ('response' in attemptedGraphLookup 
               and 'docs' in attemptedGraphLookup['response']
               and len(attemptedGraphLookup['response']['docs']) > 0):
                query['query_tree'][position] = \
                    { "type":"solr", 
                      "query": create_geo_filter(
                          attemptedGraphLookup['response']['docs'][0]['coordinates_s'], 
                          "coordinates_pt", 50)
                    }
                return True

    return False 

def create_geo_filter(coordinates, field, distanceInKM):
    return "+{!geofilt d=" + str(distanceInKM) + " sfield=\"" + field + "\" pt=\"" + coordinates + "\"}"

def find_location_coordinates(keyword):      
    query =  {
        "params": {
        "qf": "name_t",
        "keywords": keyword
        },
        "query": "{!graph from=name_s to=venue_s returnOnlyLeaf=true}{!edismax v=$keywords}"
    }        
    
    return json.loads(tag_places(query))



def get_category_and_term_vector_solr_response(keyword):
    query = {
        "params": {
            "fore": keyword,
            "back": "*:*",  
            "df": "text_t",
            #"qf": "text_t",
            #"defType": "edismax",
            "echoParams": "none"
        },
        "query": "*:*",
        "limit": 0,
        "facet": {
            "term_needing_vector": {
                "type": "query",
                "query": keyword,
                "facet": {
                    "related_terms" : {
                    "type" : "terms",
                    "field" : "text_t",
                    "limit": 3,
                    "sort": { "r1": "desc" },
                    "facet" : {
                        "r1" : "relatedness($fore,$back)" 
                    }
                    },
                    "doc_type" : {
                        "type" : "terms",
                        "field" : "doc_type",
                        "limit": 1,
                        "sort": { "r2": "desc" },
                        "facet" : {
                            "r2" : "relatedness($fore,$back)" 
                        }
                    }
                }
            }
        } 
    }
    
    response = tag_places(query)
    #response.asyncRequestID = asyncRequestID; //used to guarantee order of processing
    return json.loads(response)
    
def parse_category_and_term_vector_from_solr_response(solrResponse):
    parsed = {}
    relatedTermNodes = {}

    if ('facets' in solrResponse and 'term_needing_vector' in solrResponse['facets']):
    
        if ('doc_type' in solrResponse['facets']['term_needing_vector'] 
          and 'buckets' in solrResponse['facets']['term_needing_vector']['doc_type'] 
          and len(solrResponse['facets']['term_needing_vector']['doc_type']['buckets']) > 0 ):

            parsed['category'] = solrResponse['facets']['term_needing_vector']['doc_type']['buckets'][0]['val'] #just top one for now
    
        if ('related_terms' in solrResponse['facets']['term_needing_vector'] 
          and 'buckets' in solrResponse['facets']['term_needing_vector']['related_terms'] 
          and len(solrResponse['facets']['term_needing_vector']['related_terms']['buckets']) > 0 ): #at least one entry
    
            relatedTermNodes = solrResponse['facets']['term_needing_vector']['related_terms']['buckets']
    
    termVector = ""
    for relatedTermNode in relatedTermNodes:
        if (len(termVector) > 0):  termVector += " " 
        termVector += relatedTermNode['val'] + "^" + "{:.4f}".format(relatedTermNode['r1']['relatedness'])
    
    parsed['term_vector'] = termVector
    #parsed.asyncRequestID = solrResponse.asyncRequestID; //used to guarantee order of processing
    return parsed

import os, re
import io
def render_search_results(results):
    file_path = os.path.dirname(os.path.abspath(__file__))
    search_results_template_file = os.path.join(file_path, "search-results-template.html")
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

        rendered = ""
        for result in results['response']['docs']:
            rendered += results_template.replace("${NAME}", result['name_t'] if 'name_t' in result else "UNKNOWN") \
                .replace("${CITY}", result['city_t'] + ", " + result['state_t'] if 'city_t' in result and 'state_t' in result else "UNKNOWN") \
                .replace("${DESCRIPTION}", result['text_t'] if 'text_t' in result else "") \
                .replace("${IMAGE_URL}", "/map?lat=" + str(result['latitude_d']) + "&lon=" + str(result['longitude_d'])) \
                .replace("${STARS}", "★" * int(result['stars_i']) if 'stars_i' in result else "")


            rendered += separator_template

        if rendered == "":
            rendered = "No Results for this query."

        return rendered


class SemanticSearchHandler(http.server.SimpleHTTPRequestHandler):
    """The test example handler."""

    def sendResponse(self, response):
        try:      
            self.send_response(200)
            self.end_headers()
            self.wfile.write(bytes(json.dumps(response), 'utf-8'))
        except Exception as ex:
            self.send_error(500, ex)
  

    def sendImageResponse(self, response):
        try:      
            self.send_response(200)
            self.end_headers()
            self.wfile.write(bytes(response))
        except Exception as ex:
            self.send_error(500, ex)

    def do_POST(self):
        content_len = int(self.headers.get("Content-Length"), 0)
        post_body = self.rfile.read(content_len)

        if (self.path.startswith("/tag_query")):
            self.sendResponse(tag_query(post_body))
        elif self.path.startswith("/tag_places"):
            self.sendResponse(tag_places(post_body))
        elif self.path.startswith("/process_semantic_query"):
            self.sendResponse(process_semantic_query(post_body))
        elif self.path.startswith("/process_basic_query"):
            self.sendResponse(process_basic_query(post_body))
        elif self.path.startswith("/run_search"):
            results = json.loads(run_search(post_body))
            query = post_body.decode('UTF-8')
            rendered_results = render_search_results(results)
            self.sendResponse(rendered_results)
    
    def do_GET(self):
        if self.path.startswith("/search") or self.path.startswith("/semantic-search"):
            self.path = "/search.html"
            http.server.SimpleHTTPRequestHandler.do_GET(self)
            http.server.SimpleHTTPRequestHandler.do_GET(self)
        elif self.path.startswith("/map"):
            qsVars = parse_qs(urlparse(self.path).query)
            if 'lat' in qsVars and 'lon' in qsVars:
                lat = float(qsVars["lat"][0])
                lon = float(qsVars["lon"][0])
                zoom = int(qsVars['zoom'][0]) if 'zoom' in qsVars else 10
                m = StaticMap(200, 200)
                marker_outline = CircleMarker((lon, lat), 'white', 18)
                marker = CircleMarker((lon, lat), '#0036FF', 12)
                m.add_marker(marker_outline)
                m.add_marker(marker)

                image = m.render(zoom=zoom)
                buf = io.BytesIO()
                image.save(buf, format='JPEG')
                self.sendImageResponse(buf.getvalue())
        elif self.path.startswith("/healthcheck"):
            self.send_response(200)
            self.send_header('Access-Control-Allow-Private-Network', 'true')
            self.send_header('Access-Control-Allow-Origin','*')
            self.send_header('Content-type','image/png')
            self.end_headers()
            #Open the static file requested and send it
            image = open("is-running.png", 'br')  
            self.wfile.write(image.read())
            image.close()



def open_browser():
    """Start a browser after waiting for half a second."""
    def _open_browser():
        if AIPS_WEBSERVER_HOST == "localhost":
            webbrowser.open(WEBSERVER_URL + '/%s' % FILE)
    thread = threading.Timer(0.5, _open_browser)
    thread.start()

def start_server():
    """Start the server."""
    server_address = ("0.0.0.0", int(AIPS_WEBSERVER_PORT))
    server = http.server.HTTPServer(server_address, SemanticSearchHandler)
    server.serve_forever()

if __name__ == "__main__":
    open_browser()
    start_server()