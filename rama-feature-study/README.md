# Rama Feature Study for LiveStore

This study analyzes patterns from Red Planet Labs' Rama platform and the Twitter-scale Mastodon implementation to identify feature opportunities for LiveStore.

## Quick Reference

| # | Feature | Impact | Complexity | Sync Impact | Prerequisite |
|---|---------|--------|------------|-------------|--------------|
| 1 | [Aggregator Primitives](#1-aggregator-primitives) | High | Medium | None | - |
| 2 | [Time Bucketing](#2-time-bucketing) | High | Medium | None | #1 |
| 3 | [Cross-Partition Transactions](#3-cross-partition-transactions) | High | High | **Major** | #8 |
| 4 | [Idempotent Migrations](#4-idempotent-migrations) | Medium | Medium | Minor | - |
| 5 | [Task Globals](#5-task-globals) | Medium | Low | None | - |
| 6 | [Subindexed Collections](#6-subindexed-collections) | Medium | Medium | Minor | - |
| 7 | [Ack Returns](#7-ack-returns) | Medium | Low | **Moderate** | - |
| 8 | [Partitioning Strategy](#8-partitioning-strategy) | High | Medium | **Major** | - |
| 9 | [Compaction Generators](#9-compaction-generators) | High | High | **Major** | #1 |
| 10 | [Fanout Patterns](#10-fanout-patterns) | Medium | High | **Major** | #8 |

## Feature Dependency Graph

```
                    ┌─────────────────────────────────────────┐
                    │         Independent Features            │
                    └─────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ #1 Aggregator │   │ #5 Task Globals │   │ #4 Idempotent   │
│   Primitives  │   │    (Effect)     │   │   Migrations    │
└───────┬───────┘   └─────────────────┘   └─────────────────┘
        │
        ├───────────────────┐
        │                   │
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│ #2 Time       │   │ #9 Compaction │◄────── Requires aggregation
│   Bucketing   │   │   Generators  │        for rollup logic
└───────────────┘   └───────────────┘
                            │
                            │ Compaction affects
                            ▼ sync protocol
                    ┌───────────────┐
                    │ #8 Partition  │◄────── Foundation for
                    │   Strategy    │        multi-eventlog
                    └───────┬───────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ #3 Cross-     │   │ #10 Fanout    │   │ #6 Subindexed │
│   Partition   │   │    Patterns   │   │  Collections  │
│   Transactions│   │               │   │               │
└───────────────┘   └───────────────┘   └───────────────┘
        │
        │ Requires ack
        ▼ for feedback
┌───────────────┐
│ #7 Ack        │
│   Returns     │
└───────────────┘
```

## Reading Order

### Phase 1: Foundations (No sync changes)
Start here—these features can be implemented without protocol changes:

1. **[Aggregator Primitives](./feature-ideas.md#1-aggregator-primitives)** - Declarative combining logic
2. **[Task Globals](./task-globals-examples.md)** - Managed resource lifecycle via Effect
3. **[Idempotent Migrations](./feature-ideas.md#4-idempotent-migration-functions)** - Safe schema evolution

### Phase 2: Analytics Layer (Minimal sync changes)
Build on aggregators for time-series and analytics:

4. **[Time Bucketing](./time-bucketing-examples.md)** - Multi-granularity stats
5. **[Subindexed Collections](./subindexed-collections-examples.md)** - Hierarchical data modeling

### Phase 3: Sync Protocol Extensions (Major changes)
These require sync protocol evolution:

6. **[Ack Returns](./ack-returns-examples.md)** - Synchronous materializer feedback
7. **[Partitioning Strategy](./partitioning-examples.md)** - Multi-eventlog architecture
8. **[Compaction Generators](./compaction-generators-examples.md)** - Eventlog size management

### Phase 4: Scale Patterns (Requires partitioning)
Advanced patterns requiring multi-partition support:

9. **[Cross-Partition Transactions](./cross-partition-transactions-examples.md)** - Atomic multi-entity ops
10. **[Fanout Patterns](./fanout-examples.md)** - Twitter-scale delivery

## Files in This Study

| File | Lines | Description |
|------|-------|-------------|
| [`feature-ideas.md`](./feature-ideas.md) | ~1,200 | Main document with all 10 features |
| [`cross-partition-transactions-examples.md`](./cross-partition-transactions-examples.md) | ~1,400 | Fund transfers, escrow, inventory |
| [`task-globals-examples.md`](./task-globals-examples.md) | ~1,200 | HTTP, AI, DB, cache resources |
| [`subindexed-collections-examples.md`](./subindexed-collections-examples.md) | ~1,400 | Followers, notifications, threads |
| [`time-bucketing-examples.md`](./time-bucketing-examples.md) | ~450 | API latency, DAU/MAU, revenue |
| [`partitioning-examples.md`](./partitioning-examples.md) | ~800 | Workspace, document, geographic |
| [`compaction-generators-examples.md`](./compaction-generators-examples.md) | ~800 | Purchase rollup, GDPR, snapshots |
| [`fanout-examples.md`](./fanout-examples.md) | ~1,000 | Activity feeds, presence, cursors |
| [`ack-returns-examples.md`](./ack-returns-examples.md) | ~500 | Generated IDs, validation, conflicts |
| [`architecture.md`](./architecture.md) | ~600 | Sync protocol implications |

## Rama vs LiveStore Architecture Comparison

| Concept | Rama | LiveStore | Gap |
|---------|------|-----------|-----|
| **Event Log** | Depot (partitioned, append-only) | Eventlog (single, append-only) | Partitioning |
| **Processing** | ETL (Stream/Microbatch topologies) | Materializers (sync, per-event) | Batch semantics |
| **State** | PState (replicated, subindexed) | SQLite (local, indexed) | Replication |
| **Partitioning** | `(hash-by :field)` depot partitioner | N/A (single eventlog) | Multi-eventlog |
| **Aggregation** | `+sum`, `+top-monotonic`, combiner | Manual in materializers | Primitives |
| **Ack** | `ack-return>` in stream topology | N/A (async only) | Sync feedback |
| **Compaction** | `defgenerator` with `batch<-` | N/A (append-only) | Size management |

## Quick Wins (Implement First)

These provide immediate value with minimal complexity:

1. **Task Globals** - Use Effect's `Layer.scoped` for managed resources
   - No sync changes required
   - Enables webhook delivery, AI integration, external APIs
   - See: [`task-globals-examples.md`](./task-globals-examples.md)

2. **Aggregator Primitives** - Helper functions for common patterns
   - No sync changes required
   - Enables time-bucketing, leaderboards, analytics
   - Foundation for compaction

## High-Value Investments

These unlock significant capabilities but require more work:

1. **Compaction** (#136) - Essential for long-running apps
   - Requires sync protocol changes (compaction events)
   - Enables 96-99% eventlog size reduction
   - See: [`compaction-generators-examples.md`](./compaction-generators-examples.md)

2. **Partitioning** (#255) - Enables horizontal scaling
   - Requires multi-eventlog architecture
   - Foundation for fanout and cross-partition transactions
   - See: [`partitioning-examples.md`](./partitioning-examples.md)

## Related GitHub Issues

- [#136](https://github.com/livestorejs/livestore/issues/136) - Eventlog compaction
- [#255](https://github.com/livestorejs/livestore/issues/255) - Multi-eventlog architecture

## Source Material

- **Rama Demo Gallery**: `/.context/rama-demo-gallery/` (Clojure examples)
- **Twitter Clone**: `/.context/twitter-scale-mastodon/` (Java, production-scale patterns)
- **LiveStore Docs**: `/docs/src/content/docs/` (current architecture)
