from abc import ABC, abstractmethod

class EntityExtractor(ABC):
    def __init__(self, collection_name):
        "The collection containing entities"
        self.collection_name = collection_name

    @abstractmethod
    def extract_entities(self, query):
        "Returns extracted entities and tag data for a given query"
        pass