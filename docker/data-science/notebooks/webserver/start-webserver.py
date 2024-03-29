import sys
sys.path.append('..')
from aips import *
import threading
import webbrowser
import http.server
import requests
import json
import urllib
import os
from staticmap import StaticMap, CircleMarker
from urllib.parse import urlparse, parse_qs

FILE = 'semantic-search'
AIPS_WEBSERVER_PORT = os.getenv('WEBSERVER_PORT') or 2345

from semantic_search.engine import tag_places, keyword_search
from semantic_search import process_basic_query, process_semantic_query
from display.render_search_results import *

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
            self.sendResponse(get_engine().tag_query("entities", post_body))
        elif self.path.startswith("/tag_places"):
            self.sendResponse(tag_places(post_body))
        elif self.path.startswith("/process_semantic_query"):
            self.sendResponse(process_semantic_query(get_engine().get_collection("reviews"), post_body))
        elif self.path.startswith("/process_basic_query"):
            self.sendResponse(process_basic_query(post_body))
        elif self.path.startswith("/run_search"):
            results = json.loads(keyword_search(post_body))
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