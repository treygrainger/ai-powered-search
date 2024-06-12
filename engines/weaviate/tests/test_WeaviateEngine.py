import pytest
import sys
sys.path.insert(0, '/workspaces/ai-powered-search/')

from aips import set_engine, get_engine
from engines.weaviate.WeaviateEngine import WeaviateEngine
from aips import set_engine, get_engine


@pytest.fixture
def weaviate_engine():
    set_engine("weaviate")
    yield get_engine()

def test_health_check(weaviate_engine):
    assert weaviate_engine.health_check() == True