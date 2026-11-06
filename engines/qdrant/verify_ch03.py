"""Verify Qdrant adapter against chapter 3 lab behavior.

Ch1/Ch2 have no notebooks. Ch3.1 is pure vector math helpers.
Ch3.2 indexes cat_in_the_hat and ranks \"the cat in the hat\".
"""
from __future__ import annotations

import os
import random
import sys

import numpy
from numpy import dot
from numpy.linalg import norm
from qdrant_client import models


def cosine_similarity(v1, v2):
    return dot(v1, v2) / (norm(v1) * norm(v2))


def main():
    # Prefer explicit host for in-docker vs host-port access.
    os.environ.setdefault("AIPS_QDRANT_HOST", os.environ.get("AIPS_QDRANT_HOST", "aips-qdrant"))
    os.environ.setdefault("AIPS_QDRANT_PORT", os.environ.get("AIPS_QDRANT_PORT", "6333"))

    # Ensure bind-mounted repo is first on path when running in notebooks container.
    repo = os.environ.get("AIPS_REPO", "/tmp/notebooks")
    if os.path.isdir(repo) and repo not in sys.path:
        sys.path.insert(0, repo)

    from aips import tokenize
    from engines.qdrant.config import QDRANT_URL
    from engines.qdrant.QdrantEngine import QdrantEngine

    print("QDRANT_URL", QDRANT_URL)

    # --- Ch03.1 helpers ---
    q = numpy.array([1.0, 1.0, 0.0, 0.0])
    assert cosine_similarity(q, numpy.array([1.0, 1.0, 0.0, 0.0])) > cosine_similarity(
        q, numpy.array([0.0, 1.0, 1.0, 0.0])
    )
    assert tokenize("the cat in the hat") == ["the", "cat", "in", "the", "hat"]
    print("CH03.1 OK")

    # --- Ch03.2 controlling relevance ---
    engine = QdrantEngine()
    assert engine.health_check(log=True, retries=5), "Qdrant health check failed"
    collection = engine.create_collection("cat_in_the_hat", force_rebuild=True, log=True)
    docs = [
        {
            "id": "doc1",
            "title": "Worst",
            "description": (
                "The interesting thing is that the person in the wrong "
                "made the right decision in the end."
            ),
        },
        {
            "id": "doc2",
            "title": "Best",
            "description": (
                "My favorite book is the cat in the hat, which is about a crazy cat "
                "who breaks into a house and creates a crazy afternoon for two kids."
            ),
        },
        {
            "id": "doc3",
            "title": "Okay",
            "description": (
                "My neighbors let the stray cat stay in their garage, which resulted "
                "in my favorite hat that I let them borrow being ruined."
            ),
        },
    ]
    engine.print_status(collection.add_documents(docs))
    count = collection.get_document_count()
    print("count", count)
    assert count == 3, count

    resp = collection.search(
        query="the cat in the hat",
        query_fields=["description"],
        return_fields=["id", "title", "description", "score"],
        limit=10,
    )
    titles = [d["title"] for d in resp["docs"]]
    print("SEARCH1", titles, [d.get("score") for d in resp["docs"]])
    assert titles[0] == "Best", titles
    assert set(titles) == {"Best", "Okay", "Worst"}

    q2 = (
        '{!func}query("the") {!func}query("cat") {!func}query("in") '
        '{!func}query("the") {!func}query("hat")'
    )
    resp2 = collection.search(
        query=q2,
        query_fields="description",
        return_fields=["id", "title", "score"],
        limit=10,
    )
    titles2 = [d["title"] for d in resp2["docs"]]
    print("SEARCH2", titles2)
    assert titles2[0] == "Best", titles2

    # Dense vector sanity (used from ch13+)
    vc = engine.create_collection("outdoors_with_embeddings", force_rebuild=True)
    vec = [random.random() for _ in range(768)]
    engine.client.upsert(
        "outdoors_with_embeddings",
        points=[
            models.PointStruct(
                id=1,
                vector={"title_embedding": vec},
                payload={"id": "1", "title": "camping tent", "_text_": "camping tent"},
            )
        ],
        wait=True,
    )
    vres = vc.search(query=vec, query_fields=["title_embedding"], limit=1)
    assert vres["docs"][0]["title"] == "camping tent"
    print("VECTOR OK", vres["docs"][0].get("score"))
    print("ALL_CH03_VERIFICATION_OK")


if __name__ == "__main__":
    main()
