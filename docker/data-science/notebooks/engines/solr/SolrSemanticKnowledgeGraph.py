from engines.SemanticKnowledgeGraph import SemanticKnowledgeGraph
from engines.solr.SolrCollection import SolrCollection

def generate_request_root():
    return {
        "limit": 0,
        "params": {
            "q": "*:*",
            "fore": "{!type=$defType v=$q}",
            "back": "*:*",
            "defType": "edismax"
        },
        "facet": {}
    }

def generate_facets(name=None, values=None, field=None,
                    min_occurrences=None, limit=None,
                    min_popularity=None, default_operator="AND"):
    base_facet = {"type": "query" if values else "terms",
                  "limit": limit if limit else 10,
                  "sort": { "relatedness": "desc" },
                  "facet": {
                      "relatedness": {
                          "type": "func",
                          "func": "relatedness($fore,$back)"}}}
    if min_occurrences:
        base_facet["mincount"] = min_occurrences
    if min_popularity:
        base_facet["facet"]["relatedness"]["min_popularity"] = min_popularity
    if field:
        base_facet["field"] = field
    facets = []
    if values:
        if min_occurrences: base_facet.pop("mincount")
        if not limit: base_facet.pop("limit")
        for i, _ in enumerate(values):
            facets.append(base_facet.copy())
            op = f"q.op={default_operator} " if default_operator else ""  
            facets[i]["query"] = "{" + f'!edismax {op}qf={field} v=${name}_{i}_query' + "}"
    else:
        facets = [base_facet]
    return facets

def default_node_name(i, j):
    return "f" + str(i) + (f"_{j}" if j else "")

def validate_skg_request_input(multi_node):
    if isinstance(multi_node, list):
        map(validate_skg_request_input, multi_node)
        node_names = [node["name"] for node in multi_node]
        if len(node_names) != len(set(node_names)):
            raise ValueError("Node names must be distinct on a given level.")
    if "field" not in multi_node: # and "values" in multi_node
        raise ValueError("'field' must be provided")

def generate_request(*multi_nodes):
    """Generates a faceted Solr SKG request from a set of multi-nodes. 
    A multi-node can be a single node or a collection of nodes.
    A node can contain the following params: `name`, `values`, `field`, `min_occurance` and `limit`.
    :param str name: An optional name of the node. If not provided a default will be assigned
    :param list of str value: If empty or absent, a terms facet is used. Otherwise a query facet per value is used
    :param str field: The field to query against or discover values from.
    :param int min_occurance: The mincount on the facet.
    :param int limit: The limit on the facet.
    Each subsequent node is applied as a nested facet to all parent facets."""
    map(validate_skg_request_input, multi_nodes)
    request = generate_request_root()
    parent_nodes = [request]
    for i, multi_node in enumerate(multi_nodes):
        current_facets = []
        if isinstance(multi_node, dict):
            multi_node = [multi_node]   
        for j, node in enumerate(multi_node):
            if "name" not in node:
                node["name"] = default_node_name(i, j)
            facets = generate_facets(**node)
            current_facets.extend(facets)
            for i, parent_node in enumerate(parent_nodes):
                for j, facet in enumerate(facets):
                    parent_node["facet"][f'{node["name"]}_{j}'] = facet
            if "values" in node:
                for i, value in enumerate(node["values"]):
                    request["params"][f'{node["name"]}_{i}_query'] = value
        parent_nodes = current_facets
    return request

def transform_node(node, response_params):
    relatedness = node["relatedness"]["relatedness"] if node["count"] > 0 else 0.0
    value_node = {"relatedness": relatedness}
    sub_traversals = transform_response_facet(node, response_params)
    if sub_traversals:
        value_node["traversals"] = sub_traversals
    return value_node

def transform_response_facet(node, response_params):
    ignored_keys = ["count", "relatedness", "val"]
    traversals = {}
    for full_name, data in node.items():
        if full_name in ignored_keys:
            continue
        name = full_name.removesuffix("_" + full_name.split("_")[-1])
        if name not in traversals:
            traversals[name] = {"name": name, "values": {}}
        if "buckets" in data:
            values_node = {b["val"] : transform_node(b, response_params)
                        for b in data["buckets"]}
            traversals[name]["values"] = values_node
        else:
            value_name = response_params[f"{full_name}_query"]            
            traversals[name]["values"][value_name] = transform_node(data, response_params)
    for k in traversals.keys():
        traversals[k]["values"] = sort_by_relatedness_desc(traversals[k]["values"])
    return list(traversals.values())

def sort_by_relatedness_desc(d):
    return {k: v for k, v in sorted(d.items(), key=lambda item: item[1]["relatedness"], reverse=True)}

class SolrSemanticKnowledgeGraph(SemanticKnowledgeGraph):
    def __init__(self, collection_name):
        super().__init__(collection_name)

    def traverse(self, *multi_nodes):
        request = self.generate_request(*multi_nodes)
        response = SolrCollection(self.collection_name).native_search(request)
        return {"graph": transform_response_facet(response["facets"], request["params"])}
    
    def generate_request(self, *multi_nodes):
        return generate_request(*multi_nodes)