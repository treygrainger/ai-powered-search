import os

AIPS_SOLR_HOST = os.getenv("AIPS_SOLR_HOST") or "aips-solr"
AIPS_SOLR_PORT = os.getenv("AIPS_SOLR_PORT") or "8983"
SOLR_URL = f"http://{AIPS_SOLR_HOST}:{AIPS_SOLR_PORT}/solr"