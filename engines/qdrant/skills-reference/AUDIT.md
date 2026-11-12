# Qdrant skills audit vs AIPS adapter

Source: [qdrant/skills](https://github.com/qdrant/skills) (fetched raw SKILL.md files into this folder).

## What matches

| Skill guidance | Our implementation |
| --- | --- |
| Docker for local/prototype server | `docker-compose` service `qdrant` (`aips-qdrant`) |
| Official Python SDK | `qdrant-client==1.13.3` |
| REST for prototypes | `prefer_grpc=False`, HTTP URL |
| Named vectors per representation | Schema `vector_fields` → named dense vectors |
| Payload indexes right after create | `create_payload_index` in `QdrantEngine.create_collection` |
| Health probe | `GET /readyz` |
| Hybrid fusion RRF | App-side `Collection.hybrid_search` / `reciprocal_rank_fusion` |
| Idempotent upserts by id | Stable `point_id_from_value` + `upsert` |

## Gaps / deliberate tradeoffs

| Skill guidance | Gap | Why / follow-up |
| --- | --- | --- |
| Lexical BM25 full-text indexes (Qdrant ≥1.20) | **Implemented**: `TextIndexParams` + `TextScoringType.BM25`; search via `TextQuery` | Skills-aligned on 1.20; TF–IDF scroll kept as fallback |
| Hybrid via Query API `prefetch` + server RRF | Multi-field lexical uses prefetch+RRF; cross dense/lexical hybrid still mostly app-side `Collection.hybrid_search` | Partial skills alignment |
| Bulk writes | Streaming Spark iterator + batched upsert with reconnect retries | Aligned for lab scale |
| Image/client pin | **`qdrant/qdrant:v1.20.0`** + **`qdrant-client==1.20.0`** | Skills-aligned version floor |
| Dummy dense vector on text-only collections | `aips_dummy` size-1 vector | Still required by Qdrant vectors config |

## Recommended next upgrades (optional)

1. ~~Bump to 1.20 + BM25 text scoring~~ (done).
2. Full dense+lexical hybrid as single Query API call for all hybrid_search algorithms.
3. Raise Docker VM RAM for ch9–15 heavy Spark/transformers labs.

## How to re-fetch skills

```bash
# sparse: raw files from main
curl -fsSL https://raw.githubusercontent.com/qdrant/skills/main/skills/qdrant-search-quality/search-strategies/hybrid-search/SKILL.md
# or full set when disk allows:
# npx skills add qdrant/skills
```
