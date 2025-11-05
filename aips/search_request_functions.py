def product_search_request(query, param_overrides={}):
    request = {"query": query,
               "query_fields": ["name", "manufacturer", "long_description"],
               "return_fields": ["upc", "name", "manufacturer",
                                 "short_description", "score"],
               "limit": 5,
               "order_by": [("score", "desc"), ("upc", "asc")]}
    return request | param_overrides

def search_for_boosts(query, collection, query_field="query"):
    boosts_request = {"query": query,
                      "query_fields": [query_field],
                      "return_fields": ["query", "doc", "boost"],
                      "limit": 20,
                      "order_by": [("boost", "desc")]}
    response = collection.search(**boosts_request)
    return response["docs"]

def create_boosts_query(boost_documents):
    print("Boost Documents:")
    print(boost_documents)
    boosts = " ".join([f'"{b["doc"]}"^{b["boost"]}' 
                       for b in boost_documents])
    print(f"\nBoost Query: \n{boosts}\n")
    return boosts

def boosted_product_search_request(query, collection, boost_field=None):
    signals_documents = search_for_boosts(query, collection)
    signals_boosts = create_boosts_query(signals_documents)
    boosted_request = product_search_request(query)
    if boost_field:
        signals_boosts = (boost_field, signals_boosts)
    boosted_request["query_boosts"] = signals_boosts
    return boosted_request

def generate_fuzzy_text(text, min_chars=3, max_chars=6):
    text = text.replace(" ", "_")
    fuzzy_text = ""
    for n in range(min_chars, max_chars + 1):
        for i in range(len(text) - n):
            fuzzy_text += text[i:i + n] + " "
    return fuzzy_text