import uuid
import weaviate
import pytest
import sys

sys.path.insert(0, "/workspaces/ai-powered-search/")

from aips import set_engine, get_engine
from engines.weaviate.WeaviateEngine import WeaviateEngine
from aips import set_engine, get_engine


@pytest.fixture
def weaviate_engine():
    set_engine("weaviate")
    yield get_engine()


def test_health_check(weaviate_engine):
    assert weaviate_engine.health_check() == True


def test_create_and_get_collection(weaviate_engine):
    collection_name = f"TestCollection_{uuid.uuid4().hex}"
    weaviate_engine.create_collection(collection_name)

    assert weaviate_engine.client.collections.exists(collection_name)

    collection = weaviate_engine.get_collection(collection_name)

    assert collection.__class__.__name__ == "WeaviateCollection"
    assert collection.name == collection_name


def test_enable_ltr(weaviate_engine):
    collection_name = f"TestCollection_{uuid.uuid4().hex}"
    weaviate_engine.create_collection(collection_name)

    collection = weaviate_engine.get_collection(collection_name)
    with pytest.raises(NotImplementedError):
        weaviate_engine.enable_ltr(collection)
