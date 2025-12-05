import json
import os
import subprocess
import time
    
import signal

AIPS_ZK_HOST = os.getenv("AIPS_ZK_HOST") or "aips-zk:2181"

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
    return config.get(key, default) if default else config[key]

def kill_process_using_port(port, log=False):
    if log: print(f"Shutting down process on port {port}")
    process = subprocess.Popen(["lsof", "-i", ":{0}".format(port)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, _ = process.communicate()
    for process in str(stdout.decode("utf-8")).split("\n")[1:]:       
        data = [x for x in process.split(" ") if x]
        if len(data) <= 1:
            continue
        os.kill(int(data[1]), signal.SIGKILL)

def shutdown_semantic_engine(log=False, local_solr_port=8983, local_zk_port=2181):
    kill_process_using_port(local_solr_port, log)
    kill_process_using_port(local_zk_port, log)

def initialize_local_semantic_engine(log=False, solr_command="/opt/solr/bin/solr",
                                     local_zk_port=2181, zk_command="/opt/zk/zookeeper-3.4.5/bin/zkServer.sh"):
    if log: print("Initializing server")
    zk_process = subprocess.Popen([zk_command, "start -m 512m"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if log: [print(l) for l in zk_process.stdout]
    solr_process = subprocess.Popen([solr_command, "start", "-z", f"localhost:{local_zk_port}"],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if log: [print(l) for l in solr_process.stdout]
    if log: print("Localized engine initialized")
