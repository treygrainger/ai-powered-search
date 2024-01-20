def popularity(query, position):
    if (len(query['query_tree']) -1 > position):
        query['query_tree'][position] = {"type":"solr", "query": '+{!func v="mul(if(stars_i,stars_i,0),20)"}'}
        return True
    else:
        return False