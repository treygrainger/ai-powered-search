---
name: qdrant-deployment-options
description: "Guides Qdrant deployment selection. Use when someone asks 'how to deploy Qdrant', 'Docker vs Cloud', 'local mode', 'embedded Qdrant', 'Qdrant EDGE', 'which deployment option', 'self-hosted vs cloud', or 'need lowest latency deployment'. Also use when choosing between deployment types for a new project."
---

# Which Qdrant Deployment Do I Need?

Start with what you need: managed ops or full control? Network latency acceptable or not? Production or prototyping? The answer narrows to one of four options.


## Getting Started or Prototyping

Use when: building a prototype, running tests, CI/CD pipelines, or learning Qdrant.

- Use local mode (Python only): zero-dependency, in-memory or disk-persisted, no server needed [Local mode](https://skills.qdrant.tech/md/documentation/quickstart/)
- Local mode data format is NOT compatible with server. Do not use for production or benchmarking.
- For a real server locally, use Docker [Quick start](https://skills.qdrant.tech/md/documentation/quickstart/?s=download-and-run)


## Going to Production (Self-Hosted, You Own Ops)

Use when: you need full control over infrastructure or custom configuration, and are prepared to own operations (upgrades, backups, scaling, monitoring) yourself.

- Docker is the standard self-hosted deployment. Full Qdrant Open Source feature set, minimal setup. [Quick start](https://skills.qdrant.tech/md/documentation/quickstart/?s=download-and-run)
- You own operations: upgrades, backups, scaling, monitoring
- Must set up distributed mode manually for multi-node clusters [Distributed deployment](https://skills.qdrant.tech/md/documentation/scaling/distributed_deployment/)
- Have a data-residency or compliance requirement but don't want to own that ops burden? That combination is Hybrid Cloud (next section), not self-hosted Docker.


## Going to Production (Zero-Ops)

Use when: you want managed infrastructure with zero-downtime updates, automatic backups, and resharding without operating clusters yourself — including when data-residency or compliance rules mean the data can't sit on Qdrant-operated infrastructure.

- Qdrant Cloud handles upgrades, scaling, backups, and monitoring [Qdrant Cloud](https://skills.qdrant.tech/md/documentation/cloud-quickstart/)
- Chose Qdrant Cloud? See the [Managed Cloud quickstart skill](../qdrant-managed-cloud-quickstart/SKILL.md) to create and connect your first cluster.
- Hybrid Cloud: the same managed control plane, deployed on your own infrastructure/VPC. Use this when data residency or compliance requirements rule out Qdrant Cloud but you still don't want to operate clusters yourself [Hybrid Cloud](https://skills.qdrant.tech/md/documentation/hybrid-cloud/)
- Supports multi-version upgrades automatically
- Provides features not available in self-hosted: `/sys_metrics`, managed resharding, pre-configured alerts


## Need Lowest Possible Latency

Use when: network round-trip to a server is unacceptable. Edge devices, in-process search, or latency-critical applications.

- Qdrant EDGE: in-process bindings to Qdrant shard-level functions, no network overhead [Qdrant EDGE](https://skills.qdrant.tech/md/documentation/edge/edge-quickstart/)
- Same data format as server. Can sync with server via shard snapshots.
- Single-node feature set only. No distributed mode.
- Chose EDGE and want to build on it? See the `qdrant-edge` skill (BM25, snapshot sync, hybrid queries).


## What NOT to Do

- Use local mode for production or benchmarking (not optimized, incompatible data format)
- Self-host without monitoring and backup strategy (you will lose data or miss outages)
- Recommend self-managed Docker as the production target when the user says they don't want to operate clusters — that combination needs Qdrant Hybrid Cloud or Qdrant Managed Cloud, not self-hosted
- Choose EDGE when you need distributed search (single-node only)
- Pick Hybrid Cloud unless you have data residency requirements (unnecessary Kubernetes complexity when Qdrant Cloud works)
