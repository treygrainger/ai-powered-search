import sys
sys.path.append('..')
import json
from methods.tag_places import *

def location_distance(query, position):
      
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