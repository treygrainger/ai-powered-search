from abc import ABC, abstractmethod

class EntityExtractor(ABC):
    def __init__(self, collection):
        "The collection containing entities"
        self.collection = collection

    @abstractmethod
    def extract_entities(self, query):
        "Returns extracted entities and tag data for a given query"
        pass