def query_tree_to_resolved_query(query_tree):
    resolved_query = ""
    for i in range(len(query_tree)):
        if (len(resolved_query) > 0):
            resolved_query += " "
        
        resolved_query += query_tree[i]['query']
        
    return resolved_query