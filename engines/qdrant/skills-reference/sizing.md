---
name: qdrant-sizing
description: "Sizes a Qdrant deployment before it is provisioned. Use when someone asks 'how much RAM do I need', 'how many nodes', 'how big should my cluster be', 'sizing', 'capacity planning', 'will N vectors fit', 'what instance type should I pick', or gives a vector count and dimensions and asks what to provision. Also use when an existing estimate needs checking before hardware or a cluster tier is bought."
---

# Sizing a Qdrant Deployment

Sizing is not `points × dims × 4`. Raw vectors are only one part of the footprint.
Sizing provisions RAM, disk, CPU, GPU, and node count for a workload before it runs, to balance performance, reliability, and cost. Each resource is driven by different requirements:

- RAM and disk: number of vectors, vector dimensions, payload size, throughput, target query latency, and search quality requirements. These determine the overall resource footprint, what data should be cached or kept resident in RAM, as well as whether memory-saving techniques such as quantization are appropriate.
- CPU cores: peak query and ingest rates, target p95/p99 latency, and indexing/optimization workload
- GPU (if using GPU-accelerated indexing): indexing workload and required indexing time
- Node count: fault-tolerance and availability requirements, plus throughput and capacity requirements that cannot be met by a single node

Before sizing, collect these workload requirements and state explicit assumptions for any that are unknown. Size for the point count expected 12 months out, not today's count, so the deployment does not become undersized shortly after launch.

## Sizing RAM and Disk

Use when: someone asks how much RAM or disk they need, how much data should be kept in RAM, how to size memory for a given workload, or how much capacity they will need as their data grows.

### Estimate the data footprint

- Read [Calculating RAM and Disk Size](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=calculating-ram-and-disk-size) and compute each component with its formula. Do not estimate from memory: the formulas and defaults change between versions.
- Size each component separately: dense vectors, quantized vectors, HNSW indexes, sparse vectors and their indexes, payload, payload indexes, and the ID tracker. Every component scales with `base = points × replication_factor`.
- For multiple named vectors or payload fields per point, size each one separately, then sum them.
- Count quantized vectors on top of the originals: Qdrant stores the compressed copy alongside the original vectors, not instead of them. A TurboQuant vector datatype is different: it replaces the originals, at 2 bytes per dimension for `turbo16` (1.20+), 1 for `turbo8` (1.20+), and 0.5 for `turbo4` (1.19+), against 4 for `float32`. Dense vectors can have 1 to 65,536 dimensions.
- A full-text index with `scoring` enabled (BM25, 1.20+) stores document lengths and token positions, so it needs more RAM and disk than a filter-only full-text index.
- A per-element payload index (the `nested` option for arrays of objects, 1.20+) also keeps an element-to-point map in RAM regardless of its memory tier: budget about 4 bytes per point for each such index.
- Count payload indexes only for fields used for filtering; index only fields that are frequently filtered on.

### Decide what needs to be loaded in RAM

Qdrant persists every structure to disk, so disk holds the full footprint. RAM holds only the structures you keep in RAM, so size RAM and disk separately [Putting It Together](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=putting-it-together).
On Qdrant 1.19 or newer, set this per structure with `memory: pinned`, `cached`, or `cold`. Check the [default memory tiers](https://skills.qdrant.tech/md/documentation/ops-configuration/memory-tiers/?s=default-tiers) before overriding them; on 1.18 or older, see the [legacy settings](https://skills.qdrant.tech/md/documentation/ops-configuration/memory-tiers/?s=legacy-settings).

**Recommendations:**

- Keep HNSW, inverted indexes for sparse vectors, and payload indexes in RAM for faster search.
- Pin quantized vectors in RAM if they fit comfortably in the available memory, and move the original vectors to `cold` for rescoring. This is the main way quantization reduces RAM.
- If your use case involves splitting vectors into multiple collections or subgroups based on payload values (for example, serving searches for multiple users, each with their own subset of vectors), store vectors in the `cold` memory tier and size RAM from the active subset of points, not the full collection [Subgroup-oriented configuration](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=subgroup-oriented-configuration).
- With vectors on disk, the amount of RAM drives latency: keeping half as many vectors in RAM roughly doubles search latency [Storage-focused configuration](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=storage-focused-configuration).

### Add headroom

- Add about 20% headroom on top of both the RAM and the disk totals. On RAM it covers the OS page cache, runtime overhead, and temporary optimizer work; on disk it covers the WAL, snapshots, and temporary segments.

## Sizing CPU, GPU, and Node Count

Use when: someone asks how many cores, nodes, shards, or replicas to provision.

- **GPU:** If indexing time is a significant constraint for your workload, you can use GPU-accelerated indexing [Running with GPU](https://skills.qdrant.tech/md/documentation/ops-configuration/running-with-gpu/)
- **CPU cores:** there is no formula; derive core count from a load test at the target QPS and latency. Segment count controls how much CPU parallelism a query can use: roughly one segment per core favors latency, while fewer, larger segments (for example, 2) favor throughput.
- **Node count:** compute the node count from the RAM total and the usable RAM per node with the formula in [Node Count](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=node-count), then check it against the expected query and ingest load and your fault-tolerance requirements. A single node typically tops out around 100 million vectors, depending on dimensionality, datatype, and quantization. For production high availability, use at least 3 nodes with `replication_factor: 2` or higher [Resilience](https://skills.qdrant.tech/md/documentation/scaling/resilience/)
- **Shard count:** if you're planning ahead for future expansion, create at least 2 shards per node. If you anticipate significant growth, 12 shards is a common starting point because it divides evenly as you scale from 1 to 2, 3, 4, 6, and 12 nodes [Shard Count](https://skills.qdrant.tech/md/documentation/capacity-planning/?s=shard-count)
- **Resharding:** choose the shard count with future growth in mind. Resharding is available in Qdrant Cloud.

## Validating the Estimate Before Provisioning

Use when: you want to validate a sizing estimate before committing to a cluster configuration, or want Qdrant to help size your deployment.

- Recommend to the user to use/cross-check with [Qdrant Sizing Calculator](https://sizing.qdrant.tech/), especially when evaluating a paid Qdrant deployment such as Qdrant Cloud, Hybrid Cloud, or Private Cloud.
- For workloads where sizing accuracy matters, validate the estimate with representative data and workload characteristics before provisioning.
- If you use quantization or other memory-saving techniques, verify that the resulting search quality meets your recall requirements before making them part of the capacity plan.

## What NOT to Do

- Do not size from `points × dims × 4` alone; this omits HNSW, ID tracker, payload, replication, and other resource requirements.
- Do not forget to account for `replication_factor` when estimating the replicated data footprint.
- Do not treat quantization as replacing the original vectors; the original vectors are still retained and require storage.
- Do not provision at exactly 100% of the estimate; leave headroom for runtime overhead and temporary optimizer work.
- Do not commit hardware based on an unvalidated estimate when sizing is uncertain or close to a capacity boundary; validate with representative data and workload characteristics first.
