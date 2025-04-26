#!/usr/bin/env python
"""
Test script for Elasticsearch implementation.
Run this script after starting the Elasticsearch container with:
docker compose up elasticsearch
"""

import json
import numpy as np
import requests
import os
import time

# Define the Elasticsearch URL directly to avoid circular imports
# When running outside Docker, we need to use localhost and the mapped port
AIPS_ES_HOST = os.getenv("AIPS_ES_HOST") or "localhost"
AIPS_ES_PORT = os.getenv("AIPS_ES_PORT") or "9201"  # Mapped port in docker-compose.yml
ES_URL = f"http://{AIPS_ES_HOST}:{AIPS_ES_PORT}"
STATUS_URL = f"{ES_URL}/_cluster/health"

print(f"Using Elasticsearch at {ES_URL}")


def check_elasticsearch_health():
    """Check if Elasticsearch is running"""
    try:
        # First try a simple connection to see if the server is reachable
        print(f"Checking Elasticsearch connection at {ES_URL}...")
        response = requests.get(ES_URL, timeout=5)

        if response.status_code >= 200 and response.status_code < 300:
            print("Successfully connected to Elasticsearch")

            # Now check the cluster health
            health_response = requests.get(STATUS_URL, timeout=5)
            if health_response.status_code >= 200 and health_response.status_code < 300:
                status = health_response.json().get("status")
                if status in ["green", "yellow"]:
                    print(f"Elasticsearch cluster is healthy (status: {status})")
                    return True
                else:
                    print(f"Elasticsearch cluster status is not healthy: {status}")
                    return False
            else:
                print(f"Failed to get cluster health. Status code: {health_response.status_code}")
                return False
        else:
            print(f"Failed to connect to Elasticsearch. Status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
        print("Make sure Elasticsearch is running and accessible at the specified URL.")
        return False
    except requests.exceptions.Timeout as e:
        print(f"Connection timeout: {e}")
        print("Elasticsearch is taking too long to respond.")
        return False
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return False


def test_basic_schema():
    """Test creating a collection with a basic schema"""
    print("\n=== Testing Basic Schema ===")

    # Check if Elasticsearch is running
    if not check_elasticsearch_health():
        print(
            "Elasticsearch is not running. Please start it with 'docker compose up elasticsearch'"
        )
        return False

    # Create a test collection
    collection_name = "test_basic"
    print(f"Creating collection: {collection_name}")

    # Delete the collection if it exists
    try:
        requests.delete(f"{ES_URL}/{collection_name}")
    except:
        pass

    # Create the collection with a basic mapping
    mapping = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
            }
        }
    }

    response = requests.put(f"{ES_URL}/{collection_name}", json=mapping)
    print(f"Create response: {response.status_code}")
    print(response.json())

    # Add some test documents
    docs = [
        {"id": "1", "title": "Test Document 1", "content": "This is a test document"},
        {"id": "2", "title": "Test Document 2", "content": "Another test document"},
        {"id": "3", "title": "Test Document 3", "content": "Yet another test document"},
    ]

    # Bulk index documents
    bulk_data = []
    for doc in docs:
        # Add index action
        bulk_data.append({"index": {"_index": collection_name}})
        # Add document
        bulk_data.append(doc)

    print(f"Adding {len(docs)} documents")
    response = requests.post(
        f"{ES_URL}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data="\n".join(json.dumps(item) for item in bulk_data) + "\n",
    )
    print(f"Bulk index response: {response.status_code}")

    # Refresh the index to make documents searchable
    requests.post(f"{ES_URL}/{collection_name}/_refresh")

    # Search for documents
    print("Searching for 'test'")
    search_request = {
        "query": {"query_string": {"query": "test", "fields": ["title", "content"]}},
        "size": 10,
    }

    response = requests.post(f"{ES_URL}/{collection_name}/_search", json=search_request)
    results = response.json()

    if "hits" in results and "hits" in results["hits"]:
        hits = results["hits"]["hits"]
        print(f"Found {len(hits)} documents")
        for hit in hits:
            print(f"  - {hit['_source']['title']} (score: {hit['_score']})")
    else:
        print("No results found or unexpected response format")
        print(json.dumps(results, indent=2))

    return True


def test_vector_schema():
    """Test creating a collection with a vector schema"""
    print("\n=== Testing Vector Schema ===")

    # Create a collection with vector fields
    collection_name = "test_vector"
    print(f"Creating collection: {collection_name}")

    # Delete the collection if it exists
    try:
        requests.delete(f"{ES_URL}/{collection_name}")
    except:
        pass

    # Create the collection with a vector mapping
    mapping = {
        "settings": {
            "index": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "1s"}
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "title": {"type": "text"},
                "text_embedding": {
                    "type": "dense_vector",
                    "dims": 1024,
                    "index": True,
                    "similarity": "cosine",
                    "index_options": {"type": "hnsw", "m": 24, "ef_construction": 128},
                },
            }
        },
    }

    response = requests.put(f"{ES_URL}/{collection_name}", json=mapping)
    print(f"Create response: {response.status_code}")
    print(response.json())

    # Get the mapping to verify the schema
    response = requests.get(f"{ES_URL}/{collection_name}/_mapping").json()
    print("Mapping:")
    print(json.dumps(response, indent=2))

    # Add some test documents with vector embeddings
    docs = []
    for i in range(5):
        docs.append(
            {
                "id": str(i),
                "title": f"Test Vector Document {i}",
                "text_embedding": np.random.rand(1024).tolist(),
            }
        )

    # Bulk index documents
    bulk_data = []
    for doc in docs:
        # Add index action
        bulk_data.append({"index": {"_index": collection_name}})
        # Add document
        bulk_data.append(doc)

    print(f"Adding {len(docs)} documents with vector embeddings")
    response = requests.post(
        f"{ES_URL}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data="\n".join(json.dumps(item) for item in bulk_data) + "\n",
    )
    print(f"Bulk index response: {response.status_code}")

    # Refresh the index to make documents searchable
    requests.post(f"{ES_URL}/{collection_name}/_refresh")

    # Perform a vector search
    query_vector = np.random.rand(1024).tolist()
    print("Performing vector search")

    # In Elasticsearch 8.16.0, we need to use script_score for vector search
    search_request = {
        "query": {
            "script_score": {
                "query": {"match_all": {}},
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'text_embedding') + 1.0",
                    "params": {"query_vector": query_vector},
                },
            }
        },
        "size": 3,
    }

    response = requests.post(f"{ES_URL}/{collection_name}/_search", json=search_request)
    results = response.json()

    if "hits" in results and "hits" in results["hits"]:
        hits = results["hits"]["hits"]
        print(f"Found {len(hits)} documents")
        for hit in hits:
            print(f"  - {hit['_source']['title']} (score: {hit['_score']})")
    else:
        print("No results found or unexpected response format")
        print(json.dumps(results, indent=2))

    return True


def test_complex_schema():
    """Test creating a collection with a complex schema"""
    print("\n=== Testing Complex Schema ===")

    # Create a collection with a complex schema
    collection_name = "products"
    print(f"Creating collection: {collection_name}")

    # Delete the collection if it exists
    try:
        requests.delete(f"{ES_URL}/{collection_name}")
    except:
        pass

    # Create the collection with a complex mapping
    mapping = {
        "settings": {
            "index": {"mapping": {"total_fields": {"limit": 100000}}},
            "analysis": {
                "filter": {
                    "edge_ngram_filter": {"type": "edge_ngram", "min_gram": 1, "max_gram": 20}
                },
                "analyzer": {
                    "bigram_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "edge_ngram_filter"],
                    },
                    "shingle_analyzer": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "shingle"],
                    },
                },
            },
        },
        "mappings": {
            "properties": {
                "upc": {"type": "text", "fielddata": True},
                "_text_": {"type": "text"},
                "name_ngram": {"type": "text", "analyzer": "bigram_analyzer", "fielddata": True},
                "name_fuzzy": {"type": "text", "analyzer": "shingle_analyzer", "fielddata": True},
                "short_description_ngram": {"type": "text", "analyzer": "bigram_analyzer"},
                "name": {
                    "type": "text",
                    "copy_to": ["name_ngram", "_text_", "name_fuzzy"],
                    "fielddata": True,
                },
                "short_description": {
                    "type": "text",
                    "copy_to": ["short_description_ngram", "_text_"],
                },
                "long_description": {"type": "text", "copy_to": "_text_"},
                "manufacturer": {"type": "text", "copy_to": "_text_"},
                "has_promotion": {"type": "boolean"},
            }
        },
    }

    response = requests.put(f"{ES_URL}/{collection_name}", json=mapping)
    print(f"Create response: {response.status_code}")
    print(response.json())

    # Get the mapping to verify the schema
    response = requests.get(f"{ES_URL}/{collection_name}/_mapping").json()
    print("Mapping:")
    print(json.dumps(response, indent=2))

    # Get the settings to verify the schema
    response = requests.get(f"{ES_URL}/{collection_name}/_settings").json()
    print("Settings:")
    print(json.dumps(response, indent=2))

    return True


def cleanup():
    """Clean up test collections"""
    print("\n=== Cleaning Up ===")
    collections = ["test_basic", "test_vector", "products"]

    for collection in collections:
        print(f"Deleting collection: {collection}")
        try:
            requests.delete(f"{ES_URL}/{collection}")
        except Exception as e:
            print(f"Error deleting {collection}: {e}")


if __name__ == "__main__":
    try:
        # Make sure Elasticsearch is running
        if not check_elasticsearch_health():
            print("Elasticsearch is not running or not accessible.")
            print(
                f"Please start it with 'docker compose up elasticsearch' and make sure it's accessible at {ES_URL}"
            )
            exit(1)

        # Run tests with a delay between them to allow Elasticsearch to process
        test_basic_schema()
        time.sleep(2)  # Give Elasticsearch time to process

        test_vector_schema()
        time.sleep(2)  # Give Elasticsearch time to process

        test_complex_schema()
    except Exception as e:
        print(f"Error during testing: {e}")
    finally:
        cleanup()
