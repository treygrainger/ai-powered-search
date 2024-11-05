from abc import ABC, abstractmethod

class LTR(ABC):
    def __init__(self, collection):
        self.collection = collection
    
    @abstractmethod
    def enable_ltr(self, log=False):
        "Initializes LTR dependencies for a given collection"
        pass

    @abstractmethod
    def generate_feature(self, feature_name, params, feature_type):
        "Generates an LTR feature definition."
        pass    
        
    @abstractmethod
    def generate_query_feature(self, feature_name, field_name, constant_score=False, value="(${keywords})"): 
        "Generates an LTR query feature definition."
        pass
    
    @abstractmethod
    def generate_fuzzy_query_feature(self, feature_name, field_name):
        "Generates an LTR fuzzy query feature definition."
        pass
    
    @abstractmethod
    def generate_bigram_query_feature(self, feature_name, field_name):
        "Generates an LTR bigram query feature definition."
        pass
    
    @abstractmethod
    def generate_field_value_feature(self, feature_name, field_name):
        "Generates an LTR field value feature definition."
        pass
    
    @abstractmethod
    def generate_field_length_feature(self, feature_name, field_name):
        "Generates an LTR field length feature definition."
        pass
   
    @abstractmethod 
    def generate_model(self, model_name, feature_names, means, std_devs, weights):
        "Generate a model definition."
        pass
    
    @abstractmethod
    def delete_feature_store(self, name, log=False):
        "Deletes the feature store of the given name."
        pass
    
    @abstractmethod
    def upload_features(self, features, model_name, log=False):
        "Uploads features into the engine with a given name"
        pass

    @abstractmethod
    def delete_model(self, model_name, log=False):
        "Deletes the model from the engine."
        pass
    
    @abstractmethod
    def upload_model(self, model, log=False):
        "Upload a model to the engine."
        pass 

    @abstractmethod
    def upsert_model(self, model, log=False):
        "Deletes and uploads a model to the engine."
        pass    

    @abstractmethod
    def get_explore_candidate(self, query, explore_vector, feature_config, log=False):
        "Generates a exploration search request with the given criteria."
        pass

    @abstractmethod
    def get_logged_features(self, model_name, doc_ids, options={},
                            id_field="id", fields=None, log=False):
        "Deletes the model from the engine."
        pass

    @abstractmethod
    def search_with_model(self, model_name, **search_args):
        """Search a collection using an uploaded model.
           See engines.Collection.search() for information on parameters"""
        pass
