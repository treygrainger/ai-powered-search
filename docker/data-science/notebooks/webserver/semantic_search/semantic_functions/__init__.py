def create_geo_filter(coordinates, field, distance_in_KM):
    return "+{!geofilt d=" + str(distance_in_KM) + ' sfield="' + field + '" pt="' + coordinates + '"}'

def location_distance(query, position):
    if (len(query["query_tree"]) -1 > position):
        next_entity = query["query_tree"][position + 1]
        if (next_entity["type"] == "city"):
            query["query_tree"].pop(position + 1)
            query["query_tree"][position] = {
                "type": "transformed",
                "syntax": "solr",
                "query": create_geo_filter(next_entity['location_p'],
                "location_p", 50)}
            return True
    return False

def popularity(query, position):
    if (len(query["query_tree"]) -1 > position):
        query["query_tree"][position] = {
            "type": "transformed",
            "syntax": "solr",
            "query": '+{!func v="mul(if(stars_i,stars_i,0),20)"}'}
        return True
    else:
        return False