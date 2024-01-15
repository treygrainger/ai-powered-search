import sys
sys.path.append('..')
from semantic_search.semantic_functions.location_distance import *
from semantic_search.semantic_functions.popularity import *

def process_semantic_functions(query_tree):
    position = 0
    while position < len(query_tree):
        item = query_tree[position]        
        # process commands. For now, going left to right and then sorting by priority when ambiguous commands occur; 
        # consider other weighting options later.
        if (item['type'] == "semantic_function"):
            commandIsResolved = False
    
            command = item['semantic_function']

            if (command):
                query = {"query_tree": query_tree} #pass by-ref
                commandIsResolved = eval(item['semantic_function']); #Careful... there is code in the docs that is being eval'd. 
                #MUST ENSURE THESE DOCS ARE SECURE, OTHERWISE THIS WILL INTRODUCE A POTENTIAL SECURITY THREAT (CODE INJECTION)
            
            #else:
                #Alert ("Error: " + query.query_tree.canonical_form + " has no command function.");
            
            if (False == commandIsResolved):
                #Bad command. Just remove for now... could alternatively keep it and run as a keyword
                query_tree.pop(position) #.splice(position,1)  

        position += 1

    return query_tree 