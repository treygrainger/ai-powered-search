import json
import os
import subprocess
import time
    
import signal

LOCAL_SOLR_PORT = 8983
LOCAL_ZK_PORT = 2181
SOLR_COMMAND = "/opt/solr/bin/solr"
ZOOKEEPER_COMMAND = "/home/jovyan/zookeeper-3.4.5/bin/zkServer.sh"

AIPS_ZK_URL = os.getenv("AIPS_ZK_HOST") or "aips-zk:2181"

AIPS_WEBSERVER_HOST = os.getenv("AIPS_WEBSERVER_HOST") or "localhost"
AIPS_WEBSERVER_PORT = os.getenv("AIPS_WEBSERVER_PORT") or "2345"
WEBSERVER_URL = f"http://{AIPS_WEBSERVER_HOST}:{AIPS_WEBSERVER_PORT}"
DEFAULT_CONFIG = {"AIPS_SEARCH_ENGINE": "SOLR",
                  "PRINT_REQUESTS": False}

CONFIG_FILE_PATH = os.path.abspath(os.path.join(os.path.join(os.path.dirname(__file__) , './../data/'), 'system.config'))

def write_config(config):
    with open(CONFIG_FILE_PATH, "w") as config_file:
        json.dump(config, config_file)

def read_config():
    with open(CONFIG_FILE_PATH, "r") as f:        
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
    with open(CONFIG_FILE_PATH, "w") as config_file:
        json.dump(config, config_file)

def get(key, default=None):
    config = load_config()
    if default:
        return config.get(key, default)
    else:
        return config[key]

def kill_process_using_port(port, log=False):
    if log: print(f"Shutting down process on port {port}")
    process = subprocess.Popen(["lsof", "-i", ":{0}".format(port)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = process.communicate()
    for process in str(stdout.decode("utf-8")).split("\n")[1:]:       
        data = [x for x in process.split(" ") if x != '']
        if (len(data) <= 1):
            continue
        os.kill(int(data[1]), signal.SIGKILL)

def shutdown_semantic_engine(log=False):
    kill_process_using_port(LOCAL_SOLR_PORT, log)
    kill_process_using_port(LOCAL_ZK_PORT, log)

def initialize_embedded_semantic_engine(log=False):
    if log: print("Initializing server")
    zk_process = subprocess.Popen([ZOOKEEPER_COMMAND, "start"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if log: [print(l) for l in zk_process.stdout]
    solr_process = subprocess.Popen([SOLR_COMMAND, "start", "-z", f"localhost:{LOCAL_ZK_PORT}"],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if log: [print(l) for l in solr_process.stdout]