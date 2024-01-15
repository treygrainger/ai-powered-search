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