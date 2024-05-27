import re

baseEsQuery = {
    "size": 5,
    "query": {
        "sltr": {
            "params": {
                "keywords": "",
            },
            "model": ""
        }
      }
}

def esLtrQuery(keywords, modelName):
    import json
    baseEsQuery['query']['sltr']['params']['keywords'] = keywords
    baseEsQuery['query']['sltr']['model'] = modelName
    print("%s" % json.dumps(baseEsQuery))
    return baseEsQuery

# TODO: Parse params and add efi dynamically instead of adding manually to query below
def solrLtrQuery(keywords, modelName):
    keywords = re.sub('([^\s\w]|_)+', '', keywords)
    fuzzy_keywords = ' '.join([x + '~' for x in keywords.split(' ')])

    return {
        'fl': '*,score',
        'rows': 5,
        'q': '{{!ltr reRankDocs=30000 model={} efi.keywords="{}" efi.fuzzy_keywords="{}"}}'.format(modelName, keywords, fuzzy_keywords)
    }


tmdbFields = {
    'title': 'title',
    'display_fields': ['release_year', 'genres', 'overview']
}



def search(client, keywords, modelName, index='tmdb', fields=tmdbFields):
    if client.name() == 'elastic':
        results = client.query(index, esLtrQuery(keywords, modelName))
    else:
        results = client.query(index, solrLtrQuery(keywords, modelName))

    ti = fields['title']

    for result in results:
         print("%s " % (result[ti] if ti in result else 'N/A'))
         print("%s " % (result['_score']))

         for df in fields['display_fields']:
            print("%s " % (result[df] if df in result else 'N/A'))

         print("---------------------------------------")
