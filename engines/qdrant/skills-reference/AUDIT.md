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
| Lexical: BM25 full-text index w/ `scoring` (Qdrant ≥1.20) or sparse BM25 (≤1.19) | Client-side TF–IDF over scrolled candidates + `MatchText` filter | Keeps labs working without FastEmbed/Cloud Inference; ch03 ranking verified. Upgrade path: sparse `qdrant/bm25` or text BM25 when image ≥1.20 |
| Hybrid via Query API `prefetch` + server RRF | Fusion is client-side after separate searches | Matches AIPS `Collection.hybrid_search` abstraction used by all engines |
| Prefer `upload_points(parallel=…)` for bulk | `write()` uses `upload_points(parallel=2, max_retries=3)` with upsert fallback | Aligned |
| Image pin vs latest skills (1.18–1.20 features) | Compose pins `qdrant/qdrant:v1.13.6` | Compatible with client 1.13.x; bump together when enabling server BM25 |
| Dummy dense vector on text-only collections | `aips_dummy` size-1 vector | Qdrant requires a vectors config; points always carry `[1.0]` |

## Recommended next upgrades (optional)

1. Bump image + client together and switch lexical path to sparse BM25 or text BM25 scoring.
2. Implement native hybrid: dense prefetch + lexical prefetch + `Fusion.RRF` inside `native_search` when both signals exist.
3. ~~Use `upload_points` for Spark `write`~~ (done).

## How to re-fetch skills

```bash
# sparse: raw files from main
curl -fsSL https://raw.githubusercontent.com/qdrant/skills/main/skills/qdrant-search-quality/search-strategies/hybrid-search/SKILL.md
# or full set when disk allows:
# npx skills add qdrant/skills
```
