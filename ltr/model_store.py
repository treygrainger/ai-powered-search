import json
import os

DEFAULT_LTR_CONFIG = {"models": {}, "features": {}}

class ModelStore:        
    def __init__(self, file_name="ltr_storage.cfg"):
        self.config_file_path = os.path.abspath(os.path.join(os.path.join(
            os.path.dirname(__file__) , "./../data/"), file_name))
    
    def write_ltr_config(self, config):
        with open(self.config_file_path, "w") as config_file:
            json.dump(config, config_file)

    def read_ltr_config(self):
        with open(self.config_file_path, "r") as f:
            return json.load(f)

    def load_ltr_config(self):
        try:
            config = self.read_ltr_config()
        except:        
            self.write_ltr_config(DEFAULT_LTR_CONFIG)
            config = self.read_ltr_config()
        return config
    
    def delete_feature_store(self, name, log=False):
        ltr_config = self.load_ltr_config()
        if "features" in ltr_config and name in ltr_config["features"]:
            ltr_config["features"].pop(name)
        self.write_ltr_config(ltr_config)
    
    def upload_features(self, features, model_name, log=False):
        ltr_config = self.load_ltr_config()
        if "features" not in ltr_config:
            ltr_config["features"] = {}
        ltr_config["features"][model_name] = features
        self.write_ltr_config(ltr_config)
    
    def load_features_for_model(self, model_name, log=False):
        ltr_config = self.load_ltr_config()
        if model_name not in ltr_config["features"]:
            raise Exception(f"Feature set for model {model_name} not found.")
        return ltr_config["features"][model_name]
    
    def delete_model(self, model_name, log=False):
        ltr_config = self.load_ltr_config()
        if "models" in ltr_config and model_name in ltr_config["models"]:
            ltr_config["models"].pop(model_name) 
        self.write_ltr_config(ltr_config)
    
    def upload_model(self, model, log=False):
        ltr_config = self.load_ltr_config()
        if "models" not in ltr_config:
            ltr_config["models"] = {}
        ltr_config["models"][model["name"]] = model
        self.write_ltr_config(ltr_config)
    
    def load_model(self, model_name, log=False):
        ltr_config = self.load_ltr_config()
        if model_name not in ltr_config["models"]:
            raise Exception(f"Model {model_name} not found.")
        return ltr_config["models"][model_name]
    
