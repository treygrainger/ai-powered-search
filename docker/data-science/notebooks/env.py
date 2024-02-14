import os

AIPS_SOLR_HOST = "aips-solr"
AIPS_NOTEBOOK_HOST="aips-notebook"
AIPS_ZK_HOST="aips-zk"
#AIPS_SOLR_HOST = "localhost"
#AIPS_NOTEBOOK_HOST="localhost"
#AIPS_ZK_HOST="localhost"
AIPS_SOLR_PORT = os.getenv('AIPS_SOLR_PORT') or '8983'
AIPS_NOTEBOOK_PORT= os.getenv('AIPS_NOTEBOOK_PORT') or '8888'
AIPS_ZK_PORT= os.getenv('AIPS_ZK_PORT') or '2181'
AIPS_WEBSERVER_HOST = os.getenv('AIPS_WEBSERVER_HOST') or 'localhost'
AIPS_WEBSERVER_PORT = os.getenv('AIPS_WEBSERVER_PORT') or '2345'

SOLR_URL = f'http://{AIPS_SOLR_HOST}:{AIPS_SOLR_PORT}/solr'
STATUS_URL = f"{SOLR_URL}/admin/zookeeper/status"
SOLR_COLLECTIONS_URL = f'{SOLR_URL}/admin/collections'
WEBSERVER_URL = f'http://{AIPS_WEBSERVER_HOST}:{AIPS_WEBSERVER_PORT}'
