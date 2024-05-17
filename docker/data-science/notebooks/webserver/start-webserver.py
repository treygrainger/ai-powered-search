import sys

sys.path.append('..')
import http.server
import io
import json
import threading
import webbrowser

import sys
sys.path.append('..')
import urllib.parse
import json
import requests

from urllib.parse import parse_qs, urlparse

from aips import get_engine, get_entity_extractor, get_semantic_knowledge_graph, get_sparse_semantic_search
from aips.environment import AIPS_WEBSERVER_HOST, AIPS_WEBSERVER_PORT, WEBSERVER_URL
from staticmap import CircleMarker, StaticMap

from webserver.display.render_search_results import render_search_results
from semantic_search import process_semantic_query, process_basic_query

engine = get_engine()
reviews_collection = engine.get_collection("reviews")
entities_collection = engine.get_collection("entities")
entity_extractor = get_entity_extractor(entities_collection)
query_transformer = get_sparse_semantic_search()

def keyword_search(text):
    request = {"query": text,
               "query_fields": ["content"]}
    return reviews_collection.search(**request)

class SemanticSearchHandler(http.server.SimpleHTTPRequestHandler):
    """Semantic Search Handler (AI-Powered Search)"""

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
        post_body = self.rfile.read(content_len).decode('UTF-8')

        if (self.path.startswith("/tag_query")):
            self.sendResponse(entity_extractor.extract_entities(post_body))
        elif self.path.startswith("/tag_places"):
            request = {"query": post_body,
                       "query_fields": ["city", "state", "location_coordinates"]}
            response = reviews_collection.search(**request)
            self.sendResponse(response)
        elif self.path.startswith("/process_semantic_query"):
            self.sendResponse(process_semantic_query(reviews_collection,
                                                     entities_collection,
                                                     post_body))
        elif self.path.startswith("/process_basic_query"):
            self.sendResponse(process_basic_query(post_body))
        elif self.path.startswith("/run_search"):
            results = keyword_search(post_body)
            highlight_terms = post_body.split(' ')
            rendered_results = render_search_results(results, highlight_terms)
            self.sendResponse(rendered_results)
    
    def do_GET(self):
        if self.path.startswith("/search") or self.path.startswith("/semantic-search"):
            self.path = "display/search.html"
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
    FILE = "semantic-search"
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