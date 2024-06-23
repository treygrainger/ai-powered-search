# Supported Search Engines and Vector Databases

All the algorithms in the code base are designed to work with a wide variety of search engines and vector databases. To that end, other than cases where engine-specific syntax is required to demonstrate a point, we have implemented search functionality using a generic `engine` interface throughout the codebase that allows you to easily swap in your favorite alternative search engine or vector database.

## Supported engines (tentative)

The list of supported engines will continue to grow over time, with the following engines currently being investigated for support:

**Currently Supported:**
* `solr` - Apache Solr

**Pending Support:**
* `opensearch` - OpenSearch (pending vendor support)
* `elasticsearch` - Elasticsearch (pending vendor support)
* `weaviate` - Weaviate (pending vendor support)
* `mongo` - MongoDB / Atlas Search (pending vendor support)
* `pinecone` - Pinecone (pending vendor support)
* `qdrant` - QDrant (pending vendor support)
* `astradb` - Datastax Astra DB offering (pending vendor support)
* `lucidworks-fusion` - Lucidworks Fusion (pending vendor support)
* `bonsai-opensearch` - Bonsai's managed OpenSearch platform (pending vendor support)
* `bonsai-elasticseach` - Bonsai's managed Elasticsearch platform (pending vendor support)
* `websolr` - Websolr's managed Apache Solr platform (pending vendor support)
* `searchstax` - Searchstax's managed Apache Solr platform (pending vendor support)
* `redis` - Redis (pending vendor support)
* `algolia` - Algolia Search (pending vendor support)
* `vespa` - Vespa (pending vendor support)

**Note**: If you represent another search engine, vector database, or associated hosting provider and would like to be added to this list, please reach out to trey@searchkernel.com to discuss.

## Swapping out the engine

Normally, you'll only be working with one search engine or vector database at a time when running the book's code examples. To use any particular `engine`, you just need to specify the engine's name (as `enumerated` above) when starting up Docker:

To launch `elasticsearch`, for example:
```
docker compose up elasticsearch
```

This will both start any necessary Docker containers needed to run `elasticsearch` (or the engine you specify), as well as set this engine as active in the book's Jupyter notebooks for use in all the code examples. Note that some engines, such as managed search and API-based services, do not require any additional local Docker containers since their services are hosted elsewhere. 

If you want to use a different engine at any time, you can just restart the Docker container and specify the new engine you want to use.
```
docker compose up opensearch
```

If you want to launch more than one `engine` at a time to experiment, you can provide a list at the end of the `docker compose up` command of all the engines you wish to start:
```
docker compose up elasticsearch solr opensearch
```

The first `engine` you reference in your `docker compose up` command will be set as your active engine in the Jupyter notebooks, with the others available on standby.

If you would like to switch to one of the standby engines within your live Jupyter notebooks (for example, to `opensearch`), you can do this at any time by simply running the following command in any notebook:
```
import aips
aips.set_engine("opensearch")
```

Keep in mind that if you call `set_engine` for an engine that is not currently running, this will later result in errors if that engine is still unavailable when you try to use it.

You can also check the currently set `engine` at any time by running:
```
aips.get_engine().name
```


## The engine and collection abstractions

The search engine industry is full of different terminology and concepts, and we have tried to abstract away as much of that as possible in the codebase. Most search engines started with keyword search and have since added support for vector search, and many vector databases started with vector search and have since added keyword search. For our purposes, we just think of all of these systems as matching and ranking engines, and we use the term `engine` to refer to all of them.

Likewise, each engine has a concept of one or more logical partitions or containers for adding data. In Solr and Weaviate these containers are called `collections`, whereas in OpenSearch, Elasticsearch, and Redis these are called `indexes`, and in Vespa they are called `applications`. In MongoDB, the original data is stored in a `collection`, but it is then copied into an `index` for search purposes in Atlas Search. Naming also varies further among other engines.

For proper abstraction, we always use the term `collection` in the code base, so every `engine` has a `collection` interface through which you can query or add documents.

Common public methods on the `engine` interface include:

* `engine.create_collection(collection_name)`: Creates a new collection
* `engine.get_collection(collection_name)`: Returns an existing collection

Common public methods on the `collection` interface include:

* `collection.search(request)`: Runs a lexical or vector search (depending on the type of query) and returns results
* `collection.hybrid_search(lexical_request, vector_request, algorithm)`: Runs a hybrid search leveraging both lexical and vector-based queries.

* `collection.add_documents(docs)`: Adds a list of documents to the collection
* `collection.write(dataframe)`: Writes each row from a Spark dataframe to the collection as a document
* `collection.commit()`: Ensures recently added documents are persisted and available for searching

The `engine` and `collection` interfaces also internally implement schema definitions and management for all the datasets used in the book.

Because the `collection.write` method takes a dataframe, we utilize helpers as needed when loading data from additional data sources, such as CSV or SQL:
* `collection.write(from_csv(csv_file))`: Writes each row in the  csv file to the collection as a document
* `collection.write(from_sql(sql_query))`: Runs the SQL query and writes each returned row to the collection as a document

No additional engine-specific implementation is needed for loading from these additional data sources, as any data source that can be mapped to a Spark dataframe is implicitly supported.  

## Adding support for additional engines

While we intend to support most major search engines and vector databases, you may find that your favorite engine is not currently supported. If that is the case, we encourage you to add support for it and submit a pull request to the codebase. The `engine` and `collection` interfaces are designed to be easy to implement, and you can use the default `solr` implementation or any other already implemented engines as a reference.

### Required abstractions to implement for a new engine
There are 6 main abstractions (located in [engines/](./)).

**Core**: Must be implemented for every engine:
* `Engine`: This class is responsible for setting up collections with their appropriate schemas and configurations. Most of the complexity in the engine is the configuration of 15 different collections that support the system's functionality.

* `Collection`: This class is responsible for populating and searching a search index. `search` and `hybrid_search` are some of the more complex functions within this class to implement.

**Delegatable**: Must be implemented, but can be delegated to external libraries or engines if not directly supported inside the engine
* `LTR` (Learning to Rank): The `LTR` abstraction contains all functionality for creating models, handling features, and searching with a model.

* `SparseSemanticSearch`: Defines query transformation logic and semantic functions used in lexical / sparse-vector-based Semantic Search.


**Optional**: Can be implemented, but not required (will be delegated by default to other libraries or engines):
* `EntityExtractor`: Semantic Search requires an Entity Extractor to identify entities and tag queries.

* `SemanticKnowledgeGraph`: This class contains functionality to generate requests and to traverse a semantic knowledge graph.

If your `engine` doesn't natively support one or more features used in the [_AI-Powered Search_](https://aipowerersearch.com) book and codebase, you have three options:
1. (Recommended) Implement the missing functionality in Python outside of the search engine, pushing down what you can to the engine.
2. (Good Enough) Push that particular feature to another library or engine, relying on the already implemented way to handle the functionality. This option may be useful for specific capabilities like the `EntityExtractor` and the `SemanticKnowledgeGraph` implementations, where another engine (Solr in this case) is fairly unique in its native support of these capabilities and they are treated like an external model.
3. (Worst Case) Throw a "Not Implemented" exception for a handful of unimplemented capabilities. This should only be used as a last resort.

We hope that the `engine` and `collection` abstractions will make it easy for you to add support for your favorite engine and also potentially contribute it to the book's codebase to benefit the larger community of _AI-Powered Search_ readers and practitioners. Happy searching!
