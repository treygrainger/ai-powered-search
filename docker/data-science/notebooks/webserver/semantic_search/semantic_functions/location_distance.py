import sys
sys.path.append('..')
import json
from semantic_search.engine.tag_places import *

def location_distance(query, position):      
    if (len(query['query_tree']) -1 > position):
        nextEntity = query['query_tree'][position + 1]
        if (nextEntity['type'] == "city"):
        
            query['query_tree'].pop(position + 1);
            query['query_tree'][position] = {"type":"solr", 
                                             "query": create_geo_filter(nextEntity['location_p'], 
                                             "location_p", 50)}
            return True
    return False

def create_geo_filter(coordinates, field, distanceInKM):
    return "+{!geofilt d=" + str(distanceInKM) + " sfield=\"" + field + "\" pt=\"" + coordinates + "\"}"