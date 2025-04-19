import os

AIPS_ES_HOST = os.getenv("AIPS_ES_HOST") or "aips-elasticsearch"
AIPS_ES_PORT = os.getenv("AIPS_ES_PORT") or "9200"
ES_URL = f"http://{AIPS_ES_HOST}:{AIPS_ES_PORT}"
STATUS_URL = f"{ES_URL}/_cluster/health"
