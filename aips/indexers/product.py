from ltr.download import download, extract_tgz 
from git import Repo  # pip install gitpython


#Get datasets
![ ! -d 'retrotech' ] && git clone --depth 1 https://github.com/ai-powered-search/retrotech.git
! cd retrotech && git pull
! cd retrotech && mkdir -p '../data/retrotech/' && tar -xvf products.tgz -C '../data/retrotech/' && tar -xvf signals.tgz -C '../data/retrotech/'


dataset = ["https://github.com/ai-powered-search/tmdb/raw/main/judgments.tgz", 
           "https://github.com/ai-powered-search/tmdb/raw/main/movies.tgz"]
download(dataset, dest="")
extract_tgz("data/movies.tgz", "data/") # -> Holds "tmdb.json", big json dict with corpus
extract_tgz("data/judgments.tgz", "data/") # -> Holds "ai_pow_search_judgments.txt", 
                                  # which is our labeled judgment list
Repo.clone_from("https://github.com/ai-powered-search/retrotech.git", "data/retrotech/")

from aips.data_loaders.products import load_dataframe

products_collection = engine.create_collection("products")
products_dataframe = load_dataframe("data/retrotech/products.csv")
products_collection.write(products_dataframe)

signals_collection = engine.create_collection("signals")
signals_collection.write(from_csv("data/retrotech/signals.csv"))