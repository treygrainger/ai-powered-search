import tarfile
from aips import get_local_engine, get_engine, get_ltr_engine
from git import Repo
import os
import shutil
from engines.Engine import AdvancedFeatures

import tqdm
from aips.data_loaders import movies, outdoors, reviews, products, cities
from aips.spark.dataframe import from_csv
from git.remote import RemoteProgress

class Progress(RemoteProgress):
    def __init__(self):
        super().__init__()
        self.progress_bar = None

    def update(self, op_code, cur_count, max_count=None, message=""):
        if not self.progress_bar:
            self.progress_bar = tqdm.tqdm()
        self.progress_bar.total = max_count
        self.progress_bar.n = cur_count
        self.progress_bar.refresh()

# The following dataset definitions contain the necessary information
# for populating collections and/or pulling data dependencies from github
# 
#             url: The github url containing the file(s) for populating the collection
#           count: The number of documents  expected in the collection
#       loader_fn: The function to call to process the data file(s)
#  data_file_name: The name of the data file. Defaults to f"{dataset_name}.csv"
#     loader_args: The arguments passed to the loader function. Defaults to no arguments.
# source_datasets: A list of datasets to pull/populate the collection with. The
#                  source dataset for a collection is typically just the dataset being
#                  invoked (default). Data from every source dataset will be used to
#                  populate the collection.

def promo_products_fn(csv_file):
    return products.load_dataframe(csv_file, with_promotion=True)

dataset_info = {
    "products": {"url": "https://github.com/ai-powered-search/retrotech.git",
                 "count": 48194,
                 "loader_fn": products.load_dataframe},
    "products_with_promotions": {"url": "https://github.com/ai-powered-search/retrotech.git",
                                 "count": 48194,
                                 "loader_fn": products.load_dataframe,
                                 "data_file_name": "products.csv",
                                 "tar_file": "products.tgz",
                                 "loader_args": {"with_promotion": True},
                                 "enable_ltr": True},
    "signals": {"url": "https://github.com/ai-powered-search/retrotech.git",
                "count": 2172605,
                "loader_fn": from_csv},
               #"copy_repository": "data/"},
    "jobs": {"url": "https://github.com/ai-powered-search/jobs.git",
             "count": 30002,
             "loader_fn": from_csv,
             "loader_args": {"additional_columns": {"category": "jobs"},
                             "drop_id": True,
                             "multiline_csv": True},
             "feature_requirement": AdvancedFeatures.SKG},
    "health": {"url": "https://github.com/ai-powered-search/health.git",
               "count": 12892,
               "loader_fn": from_csv,
               "data_file_name": "posts.csv",
               "loader_args": {"additional_columns": {"category": "health"},
                               "drop_id": True},
               "feature_requirement": AdvancedFeatures.SKG},
    "cooking": {"url": "https://github.com/ai-powered-search/cooking.git",
                "count": 79324,
                "loader_fn": from_csv,
                "data_file_name": "posts.csv",
                "loader_args": {"additional_columns": {"category": "cooking"},
                                "drop_id": True},
                "feature_requirement": AdvancedFeatures.SKG},
    "scifi": {"url": "https://github.com/ai-powered-search/scifi.git",
              "count": 177547,
              "loader_fn": from_csv,
              "data_file_name": "posts.csv",
              "loader_args": {"additional_columns": {"category": "scifi"},
                              "drop_id": True},
               "feature_requirement": AdvancedFeatures.SKG},
    "travel": {"url": "https://github.com/ai-powered-search/travel.git",
               "count": 111130,
               "loader_fn": from_csv,
               "data_file_name": "posts.csv",
               "loader_args": {"additional_columns": {"category": "travel"},
                               "drop_id": True},
               "feature_requirement": AdvancedFeatures.SKG},
    "devops": {"url": "https://github.com/ai-powered-search/devops.git",
               "count": 9216,
               "loader_fn": from_csv,
               "data_file_name": "posts.csv",
               "loader_args": {"additional_columns": {"category": "devops"},
                               "drop_id": True},
               "feature_requirement": AdvancedFeatures.SKG},
    "stackexchange": {"source_datasets": ["health", "cooking", "scifi", "travel", "devops"],
                      "feature_requirement": AdvancedFeatures.SKG},
    "reviews": {"url": "https://github.com/ai-powered-search/reviews.git",
                "count": 192138,
                "loader_fn": reviews.load_dataframe},
    "entities": {"url": "https://github.com/ai-powered-search/reviews.git",
                 "source_datasets": ["entities", "cities"],
                 "count": 21,
                 "loader_fn": from_csv,
                 "feature_requirement": AdvancedFeatures.TEXT_TAGGING},
    "cities": {"url": "https://github.com/ai-powered-search/reviews.git",
               "count": 137581,
               "loader_fn": cities.load_dataframe},
    "tmdb": {"url": "https://github.com/ai-powered-search/tmdb.git",
             "count": 65616,
             "destination": ".",
             "tar_file": "movies.tgz",
             "data_file_name": "tmdb.json",
             "loader_fn": movies.load_dataframe,
             "enable_ltr": True},
    "outdoors": {"url": "https://github.com/ai-powered-search/outdoors.git",
                 "count": 18456,
                 "loader_fn": outdoors.load_dataframe,
                 "data_file_name": "posts.csv"},
    "outdoors-embeddings": {"url": "https://github.com/ai-powered-search/outdoors-embeddings.git",
             "destination": "embeddings",
             "tar_file": "outdoors_mrl_normed.tgz",
             "data_file_name": "outdoors_mrl_normed.pickle"},                        
    "judgments": {"url": "https://github.com/ai-powered-search/tmdb.git",
                  "data_file_name": "ai_pow_search_judgments.txt",
                  "destination": "."},
    "question-answering": {"url": "https://github.com/ai-powered-search/question-answering.git",
                           "repository_destination": "data"},
    "movies_with_image_embeddings": {"url": "https://github.com/ai-powered-search/tmdb.git",
                                     "data_file_name": "movies_with_image_embeddings.pickle"}}

def is_feature_supported(engine, dataset):
    required_feature = dataset_info[dataset].get("feature_requirement", None)
    return not required_feature or required_feature in engine.get_supported_advanced_features()

def build_collection(engine, dataset, force_rebuild=False, log=False):
    """
    Attempts to build a collection in the search engine if necessary.

    Parameters:
    - engine: The search engine instance.
    - dataset: The dataset key from `dataset_info`.
    - force_rebuild: Boolean to force rebuild the collection.
    - log: Boolean to enable logging.
    
    Returns:
    - collection: The built or retrieved collection from the engine.
    """
    source_datasets = dataset_info[dataset].get("source_datasets", [dataset])
    expected_count = sum([dataset_info[r]["count"] for r in source_datasets])
    if force_rebuild or not engine.is_collection_healthy(dataset, expected_count, log=log):
        if log: print(f"Reindexing [{dataset}] collection")
        collection = engine.create_collection(dataset, log=log)
        alternate_feature_collection = None
        if not is_feature_supported(engine, dataset):
            alternate_feature_collection = get_local_engine(log=log).create_collection(dataset, log=log)
        overwrite = len(source_datasets) == 1
        for dataset in source_datasets:
            csv_file_path = download_data_files(dataset, log=log)
            loader_args = dataset_info[dataset].get("loader_args", {})
            dataframe = dataset_info[dataset]["loader_fn"](csv_file_path, **loader_args)
            if dataset_info[dataset].get("enable_ltr", False):
                get_ltr_engine(collection).enable_ltr()
            collection.write(dataframe, overwrite=overwrite)
            if alternate_feature_collection:
                alternate_feature_collection.write(dataframe, overwrite=overwrite)
    else:
        if log: print(f"Collection [{dataset}] is healthy")
        collection = engine.get_collection(dataset)
    return collection

def copy_repository(dataset, log=False):
    if "copy_repository" in dataset_info[dataset]:
        target_dir = dataset_info[dataset]["copy_repository"]
        repo_path = get_repo_path(dataset)
        if log: print(f"Copying [{repo_path}] to [{target_dir}]")
        for file_name in os.listdir(repo_path):
            full_file_name = os.path.join(repo_path, file_name)
            if os.path.isfile(full_file_name):
                shutil.copy(full_file_name, target_dir)
            else:
                shutil.copytree(full_file_name, f"{target_dir}/{file_name}", dirs_exist_ok=True)

def join_split_tar_file(tar_file_path):
    if not os.path.isfile(f"{tar_file_path}") and os.path.isfile(f"{tar_file_path}.part_a"):
        char_i = 97 
        with open(tar_file_path, "wb") as merged_file:
            while True:
                part_file_path = f"{tar_file_path}.part_{chr(char_i)}"
                if not os.path.isfile(part_file_path):
                    break    
                with open(part_file_path, 'rb') as part_file:
                    content = part_file.read()
                    merged_file.write(content)
                char_i += 1

def untar_file(dataset, log=False):
    tar_file_name = dataset_info[dataset].get("tar_file", f"{dataset}.tgz")
    tar_file = f"{get_repo_path(dataset)}/{tar_file_name}"
    join_split_tar_file(tar_file)
    with tarfile.open(tar_file, 'r:gz') as tar:
        directory = dataset_info[dataset].get("destination", get_repository_name(dataset))
        target_dir = f"data/{directory}"
        if log: print(f"Extracting [{tar_file}] to [{target_dir}]")
        tar.extractall(path=target_dir)

def download_data_files(dataset, log=False):
    data_file_name = dataset_info[dataset].get("data_file_name", f"{dataset}.csv")
    directory = dataset_info[dataset].get("destination", get_repository_name(dataset))
    file_path = f"data/{directory}/{data_file_name}"
    file_exists = os.path.isfile(file_path)
    if log: print(f"File [{file_path}] exists? {file_exists}")
    if not file_exists:
        pull_github_repository(dataset, log=log)
        untar_file(dataset, log=log)
    return file_path

def pull_github_repository(dataset, log=True):
    repo_path = get_repo_path(dataset)
    progress_logger = Progress() if log else None
    if os.path.isdir(repo_path):
        if log: print(f"Pulling existing repo [{repo_path}]")
        repo = Repo(repo_path)
        repo.remotes.origin.pull(progress=progress_logger)
    else:
        if log: print(f"Cloning non-existant repo [{repo_path}]")
        repo_url = dataset_info[dataset]["url"]
        repo = Repo.clone_from(repo_url, repo_path, progress=progress_logger)
        copy_repository(dataset, log=log)
    return repo

def get_repository_name(dataset):    
    repo_url = dataset_info[dataset]["url"]
    return repo_url.split("/")[-1].split(".")[0]

def get_repo_path(dataset):
    repository_directory = dataset_info[dataset].get("repository_destination", "data/repositories")
    return f"{repository_directory}/{get_repository_name(dataset)}"