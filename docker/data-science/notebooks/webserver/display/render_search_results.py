import sys
sys.path.append('../..')
from aips import *
import requests
import os, re, io

def render_search_results(results, keywords_to_highlight):
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
            #todo: add highlighting
            rendered += results_template.replace("${NAME}", result['name_t'] if 'name_t' in result else "UNKNOWN") \
                .replace("${CITY}", result['city_t'] + ", " + result['state_t'] if 'city_t' in result and 'state_t' in result else "UNKNOWN") \
                .replace("${DESCRIPTION}", result['text_t'] if 'text_t' in result else "") \
                .replace("${IMAGE_URL}", "/map?lat=" + str(result['latitude_d']) + "&lon=" + str(result['longitude_d'])) \
                .replace("${STARS}", "★" * int(result['stars_i']) if 'stars_i' in result else "")


            rendered += separator_template

        if rendered == "":
            rendered = "No Results for this query."

        return rendered