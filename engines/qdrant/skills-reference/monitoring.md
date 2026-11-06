---
name: qdrant-monitoring
description: "Guides Qdrant monitoring and observability setup. Use when someone asks 'how to monitor Qdrant', 'what metrics to track', 'is Qdrant healthy', 'optimizer stuck', 'why is memory growing', 'requests are slow', 'set up alerts', 'cluster health check', or needs to set up Prometheus, Grafana, health checks, or log centralization. Also use when debugging production issues that require metric analysis."
allowed-tools:
  - Read
  - Grep
  - Glob
---

# Qdrant Monitoring

Route first, then answer. Match the user's symptom in the table, `Read` that file, and answer from it.
Do not answer from this page alone: it contains routing only, not the guidance. If two rows match, read both.

| The user says | Read |
|---|---|
| Want to set up Prometheus, Grafana, or health checks | `setup/SKILL.md` |
| Need to know which metrics to track | `setup/SKILL.md` |
| Setting up alerting, log centralization, or Hybrid Cloud metrics | `setup/SKILL.md` |
| Want to send Qdrant metrics to Datadog | `setup/SKILL.md` |
| Optimizer stuck or running forever | `debugging/SKILL.md` |
| Memory keeps growing in production | `debugging/SKILL.md` |
| Requests are slow and I need to find out why | `debugging/SKILL.md` |
| Is Qdrant healthy right now | `debugging/SKILL.md` |
| Want to find which queries are slow (slow request log) | `debugging/SKILL.md` |
| Need a per-collection memory or vector breakdown | `debugging/SKILL.md` |

Qdrant monitoring tracks performance and health, and catches issues before they become outages. See the metric reference before tuning anything:
[Monitoring docs](https://skills.qdrant.tech/md/documentation/ops-monitoring/monitoring/)
