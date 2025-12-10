import os

VESPA_HOST = os.getenv("AIPS_VESPA_HOST") or "aips-vespa"
VESPA_PORT = os.getenv("AIPS_VESPA_PORT") or "8080"
VESPA_URL = f"http://{VESPA_HOST}:{VESPA_PORT}"
DEFAULT_NAMESPACE = "aips"