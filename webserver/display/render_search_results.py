import sys
sys.path.append('../..')
from aips import *
import os, re

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
        for result in results["docs"]:
            #todo: add highlighting
            coordinates = result["location_coordinates"].split(",")
            rendered += results_template.replace("${NAME}", result.get("business_name", "UNKNOWN")) \
                .replace("${CITY}", result.get("city", "Anywhere") + ", " + result.get("state", "USA"))\
                .replace("${IMAGE_URL}", "/map?lat=" + coordinates[0] + "&lon=" + coordinates[1]) \
                .replace("${STARS}", "★" * int(result.get("stars_rating", 0)))
            rendered += separator_template

        if rendered == "":
            rendered = "No Results for this query."

        return rendered