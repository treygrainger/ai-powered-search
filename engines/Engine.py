from abc import ABC, abstractmethod

class Engine(ABC):
    def __init__(self, name):
        self.name = name
    
    @abstractmethod
    def health_check(self):
        "Checks the state of the search engine returning a boolean"
        pass
    
    @abstractmethod
    def print_status(self, response):
        "Prints the resulting status of a search engine request"
        pass

    @abstractmethod
    def create_collection(self, name):
        "Create and initialize the schema for a collection, returns the initialized collection"
        pass

    @abstractmethod
    def get_collection(self, name):
        "Returns initialized object for a given collection"
        pass