import os
import requests
import json

# Set environment variables for local testing
os.environ["AIPS_ES_HOST"] = "localhost"
os.environ["AIPS_ES_PORT"] = "9201"  # This should match the host port in docker-compose.yml

# Import after setting environment variables
from aips import get_engine, set_engine

# Test Elasticsearch connection
def test_elasticsearch_connection():
    es_url = f"http://{os.environ['AIPS_ES_HOST']}:{os.environ['AIPS_ES_PORT']}"
    try:
        response = requests.get(f"{es_url}/_cluster/health")
        if response.status_code == 200:
            print(f"✅ Successfully connected to Elasticsearch at {es_url}")
            print(f"Cluster status: {response.json()['status']}")
            return True
        else:
            print(f"❌ Failed to connect to Elasticsearch at {es_url}")
            print(f"Status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error connecting to Elasticsearch at {es_url}: {e}")
        return False

# Test creating a collection
def test_create_collection():
    try:
        # Set the engine to Elasticsearch
        set_engine("elasticsearch")
        engine = get_engine()
        
        # Create a test collection
        collection = engine.create_collection("test_collection")
        print("✅ Successfully created test_collection")
        
        # Add a test document
        doc = {"id": "1", "title": "Test Document", "content": "This is a test document"}
        collection.add_documents([doc])
        print("✅ Successfully added test document")
        
        # Search for the document
        response = collection.search(query="test")
        print(f"✅ Search results: {json.dumps(response, indent=2)}")
        
        return True
    except Exception as e:
        print(f"❌ Error testing collection: {e}")
        return False

if __name__ == "__main__":
    print("Testing Elasticsearch connection...")
    if test_elasticsearch_connection():
        print("\nTesting collection creation and search...")
        test_create_collection()
    else:
        print("\nSkipping collection tests due to connection failure.")
