from abc import ABC, abstractmethod

class SemanticKnowledgeGraph(ABC):
    def __init__(self, collection):
        self.collection = collection

    @abstractmethod
    def traverse(self, *nodes):
        "Traverses a semantic knowledge through each request node"
        pass

    @abstractmethod    
    def transform_request(self, *nodes):
        """
        Generates a semantic knowledge graph request from a list of nodes, or multi-nodes
        A node can contain the following params: `name`, `values`, `field`, `min_occurance` and `limit`.
        :param str name: An optional name of the node. If not provided a default will be assigned
        :param list of str value: If a value is present, this node represents a query
                                  Otherwise, this node will discover terms terms are discovered. Otherwise the query is applied.
        :param str field: The field to query against or discover values from.
        :param int min_occurance: The minimum number of occurances that a term must occur within
                                  the knowledge base to be qualify for discovery.
        :param int limit: The limit on number of terms to discover
        """
        pass