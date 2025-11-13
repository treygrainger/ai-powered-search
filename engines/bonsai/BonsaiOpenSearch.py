import os
from engines.opensearch.OpenSearchEngine import OpenSearchEngine

class BonsaiOpenSearch(OpenSearchEngine):
   def __init__(self):
        bonsai_opensearch_url = os.getenv("BONSAI_OPENSEARCH_URL")
        bonsai_access_key = os.getenv("BONSAI_ACCESS_KEY")
        bonsai_access_secret = os.getenv("BONSAI_ACCESS_SECRET")
        
        if not bonsai_opensearch_url:
            raise ValueError("BONSAI_OPENSEARCH_URL environment variable is not set")
        if not bonsai_access_key:
            raise ValueError("BONSAI_ACCESS_KEY environment variable is not set")
        if not bonsai_access_secret:
            raise ValueError("BONSAI_ACCESS_SECRET environment variable is not set")
        
        super().__init__(bonsai_opensearch_url, bonsai_access_key, bonsai_access_key)