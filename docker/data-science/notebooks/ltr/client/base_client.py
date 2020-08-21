from abc import ABC, abstractmethod

'''
    This project demonstrates working with LTR in Elasticsearch and Solr

    The goal of this class is to abstract away the server and highlight the steps
    required to begin working with LTR.  This keeps the examples agnostic about
    which backend is being used, but the implementations of each client
    should be useful references to those getting started with LTR on
    their specific platform
'''
class BaseClient(ABC):
    @abstractmethod
    def get_host(self):
        pass

    @abstractmethod
    def name(self):
        pass

    @abstractmethod
    def delete_index(self, index):
        pass

    @abstractmethod
    def create_index(self, index):
        pass

    @abstractmethod
    def index_documents(self, index, doc_src):
        pass

    @abstractmethod
    def reset_ltr(self, index):
        pass

    @abstractmethod
    def create_featureset(self, index, name, ftr_config):
        pass

    @abstractmethod
    def query(self, index, query):
        pass

    @abstractmethod
    def get_doc(self, doc_id, fields=None):
        pass

    @abstractmethod
    def log_query(self, index, featureset, ids, params):
        pass

    @abstractmethod
    def submit_model(self, featureset, index, model_name, model_payload):
        pass

    @abstractmethod
    def submit_ranklib_model(self, featureset, index, model_name, model_payload):
        pass

    @abstractmethod
    def model_query(self, index, model, model_params, query):
        pass

    @abstractmethod
    def feature_set(self, index, name):
        """ Return a mapping of name/feature ordinal
            and the raw (search engine specific) feature list"""
        pass


