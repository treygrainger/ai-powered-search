# Qdrant engine adapter

Local Docker option for AI-Powered Search notebooks.

## Start

```bash
docker compose up qdrant
# or with notebooks already running:
docker compose up -d --no-deps qdrant
```

- In-compose hostname: `aips-qdrant:6333`
- Host port mapping: `16333 -> 6333` (avoids clashes with other local Qdrant processes on 6333)

```python
import aips
aips.set_engine("qdrant")
aips.healthcheck()
```

## Capabilities

| Feature | Support |
| --- | --- |
| Lexical keyword search | Client-side TF-IDF-style ranking over payload text |
| Dense vector search | Named vectors from schema |
| Hybrid RRF | Application-side fusion (`Collection.hybrid_search`) |
| LTR | Local `ModelStore` (same pattern as Weaviate) |
| SKG / entity tagging | Delegated to Solr via `get_semantic_engine` |

## Verify chapter 3 labs

```bash
docker exec -e PYTHONPATH=/tmp/notebooks \
  -e AIPS_QDRANT_HOST=aips-qdrant \
  aips-notebooks python /tmp/notebooks/engines/qdrant/verify_ch03.py
```

Chapters 1–2 have no notebooks. Chapter 3 lab 1 is pure vector math; lab 2 uses `cat_in_the_hat` create/index/search.

## Qdrant skills check

Official agent skills from [qdrant/skills](https://github.com/qdrant/skills) are mirrored under `skills-reference/` with an `AUDIT.md` comparing guidance to this adapter.

High level: deployment/SDK/named-vectors/payload-indexes match; lexical ranking is intentionally client-side for lab simplicity (skills prefer server BM25 / sparse BM25). Native Query-API hybrid prefetch is a planned upgrade.
