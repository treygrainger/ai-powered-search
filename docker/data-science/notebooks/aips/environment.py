import json
import os

AIPS_NOTEBOOK_HOST = "aips-notebook"
AIPS_NOTEBOOK_PORT = os.getenv("AIPS_NOTEBOOK_PORT") or "8888"

AIPS_ZK_HOST = "aips-zk"
AIPS_ZK_PORT = os.getenv("AIPS_ZK_PORT") or "2181"

AIPS_SOLR_HOST = "aips-solr"
AIPS_SOLR_PORT = os.getenv("AIPS_SOLR_PORT") or "8983"
SOLR_URL = f"http://{AIPS_SOLR_HOST}:{AIPS_SOLR_PORT}/solr"
STATUS_URL = f"{SOLR_URL}/admin/zookeeper/status"
SOLR_COLLECTIONS_URL = f"{SOLR_URL}/admin/collections"

AIPS_WEBSERVER_HOST = os.getenv("AIPS_WEBSERVER_HOST") or "localhost"
AIPS_WEBSERVER_PORT = os.getenv("AIPS_WEBSERVER_PORT") or "2345"
WEBSERVER_URL = f"http://{AIPS_WEBSERVER_HOST}:{AIPS_WEBSERVER_PORT}"
DEFAULT_CONFIG = {"AIPS_SEARCH_ENGINE": "SOLR"}


def write_config(config):
    with open("../config.json", "w") as config_file:
        json.dump(DEFAULT_CONFIG, config_file)

def read_config():
    with open("../config.json", "r") as f:
        return json.load(f)

def load_config():
    try:
        config = read_config()
    except:        
        write_config(DEFAULT_CONFIG)
        config = read_config()
    return config

def set(key, value):
    config = load_config()
    config[key] = value
    with open("../config.json", "w") as config_file:
        json.dump(config, config_file)

def get(key, default=None):
    config = load_config()
    if default:
        return config.get(key, default)
    else:
        return config[key]