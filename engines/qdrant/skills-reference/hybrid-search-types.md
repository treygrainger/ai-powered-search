---
name: qdrant-hybrid-search-prefetches
description: "Constructing prefetch queries for hybrid retrieval, including BM25/dense and multi-field setups, and choosing a lexical retrieval method. Use when someone asks 'dense and sparse in one search?', 'how to combine multiple fields for retrieval?', 'payloads or sparse vectors for lexical?', 'BM25 on a full-text index or sparse vectors?', 'which sparse embedding model to use?', 'BM25 vs SPLADE?', or 'how to migrate BM25 sparse vectors to a text index?'"
---

# Different Searches in One Query API Request

Each `prefetch` runs exactly one search per one query. 

Understand if user wants to run several parallel searches on:
1. The same vector representations but different queries or filters.
2. Different vector representations but the same raw query.

If first, help user to design logic of constructing query or/and filters on application side and then check [Combining Searches](../combining-searches/SKILL.md). Don't forget to create [indices on filterable payload fields](https://skills.qdrant.tech/md/documentation/manage-data/indexing/?s=payload-index), immediately after collection creation, prior to building HNSW, so filterable HNSW could be constructed.

If second, use [named vectors](https://skills.qdrant.tech/md/documentation/manage-data/vectors/?s=named-vectors), which allow to store multiple vector types per point in one collection. On Qdrant 1.18 or newer, named vectors can be added to or removed from an existing collection without recreating it [Update vector schema](https://skills.qdrant.tech/md/documentation/manage-data/collections/?s=update-vector-schema); on 1.17 or older, they can be configured only at collection creation. To choose vectors, check following recommendations.

## Missed Keyword Matches

Use when: pure vector search misses exact term or keyword matches and you need lexical retrieval alongside semantic search.

Most likely you need a lexical retriever alongside the dense one. [Payload filtering doesn't provide any ranking score](https://skills.qdrant.tech/md/documentation/search/text-search/?s=filtering-versus-querying), so rank text with BM25 or a sparse vector.

### Choose a Lexical Retriever for Text
- **BM25 on a full-text index** (Qdrant 1.20 or newer, recommended default). Create a `text` payload index with `scoring` enabled on the field and query it with a `text` query, `using` set to the field name. Works on text already stored in the payload, so there are no embeddings to generate or store, existing data is indexed without re-upload, and the average document length is computed from your data. Text processing (tokenizer, stemming, stopwords, ASCII folding) comes from the index settings and defaults to no stemming and no stopwords, so configure the language. `k` and `b` are set per query. Scoring also enables phrase matching and costs extra memory and disk for document lengths and token positions. [BM25 on a full-text index](https://skills.qdrant.tech/md/documentation/search/text-search/full-text-search/?s=bm25) [BM25 scoring index](https://skills.qdrant.tech/md/documentation/manage-data/indexing/?s=bm25-scoring)
- **BM25 sparse vector model**, built into Qdrant core (computed server-side by the `qdrant/bm25` inference model or client-side with FastEmbed). Use it on Qdrant 1.19 or older, when the text isn't stored in the payload, or when you need the sparse vector itself, for example for Edge compatibility. Good baseline, works out-of-domain, usually for long texts. Can be used for non-English content with per-language configuration. More in [Sparse Vector BM25](https://skills.qdrant.tech/md/documentation/search/text-search/sparse-vector-search/?s=bm25)
- **BM42** learned sparse, based on BM25, but better for small chunks of text & with meaning understanding. Works only on English. Requires fine-tuning for domain-specific retrieval. Requires FastEmbed (Python/REST only, not available in all SDKs). Not maintained. 
- **miniCOIL** learned sparse, BM25 with additional understanding of words meaning in context. Works only on English. Requires fine-tuning for domain-specific retrieval. Requires FastEmbed. Usage shown in [FastEmbed miniCOIL documentation](https://skills.qdrant.tech/md/documentation/fastembed/fastembed-minicoil/).
- **SPLADE++** learned sparse with term expansion. Heavier inference and resources usage but better performance due to term expansion. Requires fine-tuning for domain-specific retrieval. Provided in Qdrant Cloud Inference and FastEmbed versions work only on English. To use with FastEmbed, check [FastEmbed SPLADE documentation](https://skills.qdrant.tech/md/documentation/fastembed/fastembed-splade/).
- **External learned sparse embeddings**, for example BAAI/bge-m3.

What to remember when using any lexical retriever:
- tokenization and stemming affect exact matches, especially on custom codes, terms, etc.
- On Qdrant 1.18 and older, IDF statistics are computed over the whole shard, which distorts scoring in multi-tenant collections where tenants have different vocabularies. On 1.19 or newer, scope them to one tenant with the `idf` search parameter, and index the tenant field. On 1.20 or newer this also applies to BM25 `text` queries on a full-text index. See [Per-tenant scoring statistics](https://skills.qdrant.tech/md/documentation/manage-data/multitenancy/?s=per-tenant-scoring-statistics).
- BM25 might be not good for small chunks of text, as BM25 algorithm was initially created for search on long documents; consider adjusting document statistics (TF & IDF, k, b).
- Statistics are per shard, and deleted or updated points can skew them slightly until the optimizer rebuilds segments, so scores can shift without a data change [Corpus statistics](https://skills.qdrant.tech/md/documentation/search/text-search/full-text-search/?s=corpus-statistics).

What to remember when using BM25 on a full-text index:
- Set stemmer and stopwords for the data's language on the index. Without them, BM25 matches only exact lowercased tokens. For language-neutral content (names, codes) use the `multilingual` tokenizer and leave stemming and stopwords off [Text processing](https://skills.qdrant.tech/md/documentation/search/text-search/full-text-search/?s=text-processing).
- A `text` query returns points that contain at least one query term, and repeated query terms count once. Add a full-text filter to require or exclude terms.
- If the text field is inside an array of objects (reviews, passages), create the index with `nested` so each element is scored as its own document and the point gets its best element's score [Score elements separately](https://skills.qdrant.tech/md/documentation/search/text-search/full-text-search/?s=score-elements-of-an-array-separately).
- Changing `scoring` on an existing index rebuilds it from the stored payload without re-upload, but can take a while on large collections [BM25 scoring](https://skills.qdrant.tech/md/documentation/manage-data/indexing/?s=bm25-scoring).

What to remember when using the BM25 sparse vector model and miniCOIL (based on BM25):
- Calibrate `avg_len` per field: Qdrant doesn't compute it for sparse vectors, and the default (256) assumes document-length text, so short fields (titles, tags) need a much smaller value or BM25 overweights term frequency. This is one reason to prefer a full-text index on 1.20 or newer [Sparse vector BM25](https://skills.qdrant.tech/md/documentation/search/text-search/sparse-vector-search/?s=configuring-bm25-parameters).
- Qdrant BM25 vectors are configured per language, so consider customizing stop words, stemming & tokenization when users documents mix several languages or carefully configure vectors per point when they are monolingual. To disable text processing entirely for language-neutral content: on Qdrant 1.19 or newer, use `stemmer: {"type": "none"}` plus an empty `stopwords` set explicitly (by default, BM25 applies English stemming and stopword removal); on 1.18 or older, use `language: none` (deprecated as of 1.19).

### Migrate from Sparse Vector BM25 to a Full-Text Index

Use when: the user already runs sparse vector BM25 on Qdrant 1.20 or newer and the text is stored in the payload.

- Create a `text` index with `scoring` enabled on the payload field, mirror the sparse model's text processing (the sparse model defaults to English stemming and stopwords, a full-text index doesn't, so set the `english` stemmer and stopwords explicitly, and match tokenizer and ASCII folding), then switch queries from the sparse vector to `text` queries. Drop `avg_len`. Scores will differ, so re-evaluate [Migrate from sparse vector BM25](https://skills.qdrant.tech/md/documentation/search/text-search/full-text-search/?s=migrate-from-sparse-vector-bm25).

More on [Sparse Vectors for Text Search](https://skills.qdrant.tech/md/course/essentials/day-3/sparse-retrieval-demo/)

## Need to Combine Multiple Representations of the Same Item

Use when: the same item is embedded in multiple ways (e.g. different models, languages, modalities, or different fields like title/abstract/chunk) and you want to search across different representations in one request (don't have to be all of them, can be even one).

Use multiple named vector prefetches, each prefetch covers one representation.

A representation only earns its own prefetch if it carries signal independent of the others — e.g. title vocabulary the body never repeats, or an abstract treated as a single semantic unit vs. individual chunks. Don't add a prefetch per field reflexively; verify each candidate contributes content the other vectors don't.

When a representation's signal is mostly lexical — keyword-driven titles, codes, tags, or other short fields — prefer BM25 over an additional dense embedding. On Qdrant 1.20 or newer, create a BM25 full-text index (with `scoring` enabled) per payload field and run one `text` query prefetch per field, with no extra named vector. On older versions, use a sparse named vector with BM25. Server-side BM25 in Qdrant avoids the inference cost of another dense model and stores far less per point. Skip this when the field carries paraphrase or conceptual signal that exact-term matching would miss.

- End-to-end worked example fusing title, abstract, chunk, and sparse-title named vectors with RRF and document-level grouping in one Query API call: [Multi-Representation Search tutorial](https://skills.qdrant.tech/md/documentation/tutorials-search-engineering/multi-representation-search/)
- If you have groups and subgroups of representations (document -> chunk, image -> patch), you could use [searching in groups](https://skills.qdrant.tech/md/documentation/search/search/?s=search-groups). To not store identical payloads several times, check [Lookup in Groups](https://skills.qdrant.tech/md/documentation/search/search/#lookup-in-groups). Index the grouping payload field (e.g. `document_id`) as a keyword payload index before grouping.
- When grouping chunk-level points back to documents, each prefetch only contributes the candidates it returned — so size per-prefetch `limit` well above the final document `limit` (rule of thumb: `prefetch_limit ≥ final_limit × expected_chunks_per_document`), otherwise a few documents with many chunks saturate the candidate pool and relevant documents drop silently. Validate grouped recall on a labeled sample.
- Keep per-document vectors (title, abstract) denormalized on every chunk-level point for single-call queries, unless the duplication dominates storage; then move them to a sidecar collection joined via [Lookup in Groups](https://skills.qdrant.tech/md/documentation/search/search/#lookup-in-groups).

You can also search directly on [multivectors](https://skills.qdrant.tech/md/documentation/manage-data/vectors/?s=multivectors), a matrix of dense vectors, in a prefetch.

However, it comes with several considerations, as multivectors were designed to support late interaction models using max similarity metric, so it's impossible to retrieve the list of individual max similarity scores for each query vector.

Moreover, multivectors are rarely a good pick for prefetch:
- max similarity metric is not symmetric, so [using HNSW index with it could be problematic](https://skills.qdrant.tech/md/course/multi-vector-search/module-1/maxsim-distance/#the-hnsw-challenge)
- [multivector representations are very heavy, as search process on them](https://skills.qdrant.tech/md/course/multi-vector-search/module-1/problems-multi-vector). 

There are ways to make multivector retrieval cheaper (MUVERA, pooling), you can see more in ["Evaluating Tradeoffs of Multi-stage Multi-vector Search"](https://skills.qdrant.tech/md/course/multi-vector-search/module-3/evaluating-pipelines/)

## What NOT to Do
- Choose any search method (for example, BM25) without evaluation of its quality & resources used.
- Use any search method (for example, BM25) without paying attention to the specifics of their configuration and applicability to the use case.
- On Qdrant 1.20 or newer, default to sparse vector BM25 with a guessed `avg_len` when the text is already in the payload; a full-text index with `scoring` enabled computes it from the data.
- Run BM25 on a full-text index without configuring stemmer and stopwords for non-English data, or without `scoring` enabled (a plain full-text index only filters).

