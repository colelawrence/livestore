# Architecture Analysis: Sync Protocol Implications

This document analyzes how each Rama-inspired feature would impact LiveStore's sync protocol and core architecture.

## Current LiveStore Sync Model

```
┌─────────────┐                              ┌─────────────┐
│   Client    │                              │   Leader    │
│             │                              │             │
│ ┌─────────┐ │    Push (events)             │ ┌─────────┐ │
│ │Eventlog │ │ ─────────────────────────────▶│ │Eventlog │ │
│ └─────────┘ │                              │ └─────────┘ │
│      │      │    Pull (events + cursor)    │      │      │
│      ▼      │ ◀─────────────────────────── │      ▼      │
│ ┌─────────┐ │                              │ ┌─────────┐ │
│ │ SQLite  │ │                              │ │ SQLite  │ │
│ └─────────┘ │                              │ └─────────┘ │
└─────────────┘                              └─────────────┘
```

**Key Properties:**
- Single eventlog per store (1:1 storeId ↔ eventlog)
- Append-only, immutable events
- Deterministic materialization (same events → same state)
- Cursor-based sync (client tracks last-seen event)
- No server-to-client feedback channel during commit

---

## Feature-by-Feature Protocol Analysis

### 1. Aggregator Primitives

**Sync Impact: None**

Aggregators are purely client-side abstractions for materializer logic. They don't affect the sync protocol.

```typescript
// No protocol change - just helper functions
const sumAgg = (curr: number, val: number) => curr + val
```

**Architecture Changes:**
- None required
- Can ship as utility library

---

### 2. Multi-Granularity Time Bucketing

**Sync Impact: None**

Time bucketing is a materializer pattern that writes to multiple SQLite rows per event. No protocol changes needed.

```typescript
// Single event → multiple table rows (already supported)
'v1.ApiRequest': (args) => [
  tables.stats.upsert({ granularity: 'm', bucket: minuteBucket, ... }),
  tables.stats.upsert({ granularity: 'h', bucket: hourBucket, ... }),
  tables.stats.upsert({ granularity: 'd', bucket: dayBucket, ... }),
]
```

**Architecture Changes:**
- None required
- Document as pattern in guides

---

### 3. Cross-Partition Transactions

**Sync Impact: Major**

Cross-partition transactions require coordinating writes across multiple eventlogs with atomic semantics.

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│ Partition A │         │ Coordinator │         │ Partition B │
│  (sender)   │         │   (leader)  │         │ (receiver)  │
└──────┬──────┘         └──────┬──────┘         └──────┬──────┘
       │                       │                       │
       │  1. TransferRequest   │                       │
       │──────────────────────▶│                       │
       │                       │                       │
       │  2. Validate & Lock   │  3. Validate & Lock   │
       │◀──────────────────────│──────────────────────▶│
       │                       │                       │
       │  4. Commit/Rollback   │  4. Commit/Rollback   │
       │◀──────────────────────│──────────────────────▶│
       │                       │                       │
```

**Protocol Changes Required:**

1. **Transaction Coordinator Role**
   - Leader must coordinate 2PC across partitions
   - New message types: `PrepareTransaction`, `CommitTransaction`, `AbortTransaction`

2. **Cross-Partition Event References**
   ```typescript
   interface CrossPartitionEvent {
     transactionId: string
     sourcePartition: string
     targetPartitions: string[]
     phase: 'prepare' | 'commit' | 'abort'
   }
   ```

3. **Distributed Lock Protocol**
   - Pessimistic: Lock entities before modification
   - Optimistic: Version vectors with conflict detection

4. **Rollback Mechanism**
   - Compensation events for failed transactions
   - Saga pattern support

**Prerequisites:**
- #8 Partitioning Strategy must be implemented first

**Complexity: High** - This is essentially building a distributed transaction coordinator.

---

### 4. Idempotent Migration Functions

**Sync Impact: Minor**

Migrations run during rematerialization. The only sync consideration is tracking which migrations have been applied.

**Option A: Migration in Materializer (No protocol change)**
```typescript
// Idempotent check in materializer - no protocol change
'v1.UserCreated': (args) => {
  const migratedArgs = migrateUserSchema(args) // Idempotent transform
  return tables.users.insert(migratedArgs)
}
```

**Option B: Migration Metadata (Minor protocol change)**
```typescript
// Store migration state in eventlog metadata
interface EventlogMetadata {
  appliedMigrations: string[]  // ['migration-001', 'migration-002']
  schemaVersion: number
}
```

**Protocol Changes (Option B):**
- Eventlog metadata sync (small addition to cursor response)
- Migration event type for tracking

**Architecture Changes:**
- Migration registry in schema definition
- Rematerialization hooks for migration tracking

---

### 5. Task Globals for IO Resources

**Sync Impact: None**

Task globals are purely client-side resource management using Effect's Layer system.

```typescript
// No protocol change - Effect Layer lifecycle
const store = yield* createStore({
  schema,
  layer: Layer.mergeAll(httpClientLayer, aiClientLayer),
})
```

**Architecture Changes:**
- Document Effect Layer integration pattern
- Possibly expose hook for store lifecycle events

---

### 6. Subindexed Collections

**Sync Impact: Minor**

Subindexed collections are a storage optimization. The sync protocol doesn't change, but the SQLite schema does.

**Option A: Virtual Table Pattern (No protocol change)**
```sql
-- Client-side only: use JSON columns with generated indexes
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  followers TEXT,  -- JSON: {"follower_id": {...}, ...}
);
CREATE INDEX idx_followers ON users(json_each(followers, '$.key'));
```

**Option B: Automatic Join Tables (Minor schema change)**
```typescript
// Schema declares subindexed collection
followers: State.SQLite.subindexedMap({
  keySchema: Schema.String,
  valueSchema: FollowerSchema,
})

// Materializes to separate table automatically
// CREATE TABLE users_followers (parent_id, key, value)
```

**Protocol Changes:**
- None for data sync
- Schema negotiation might need extension for subindex hints

---

### 7. Ack Returns for Event Commits

**Sync Impact: Moderate**

Ack returns require a response channel from materializer back to the committing client.

```
┌─────────────┐                              ┌─────────────┐
│   Client    │                              │   Leader    │
│             │  1. Commit(event)            │             │
│             │ ─────────────────────────────▶             │
│             │                              │ Materialize │
│             │  2. AckResponse(value)       │             │
│             │ ◀───────────────────────────│             │
│             │                              │             │
└─────────────┘                              └─────────────┘
```

**Protocol Changes Required:**

1. **Commit Response Enhancement**
   ```typescript
   // Current: Commit returns success/failure only
   interface CommitResponse {
     success: boolean
     cursor: string
   }

   // New: Include ack return value
   interface CommitResponseWithAck<T> {
     success: boolean
     cursor: string
     ackValue?: T  // Materializer return value
   }
   ```

2. **Materializer Context Extension**
   ```typescript
   type MaterializerContext = {
     query: QueryContext
     ackReturn: <T>(value: T) => void  // NEW
   }
   ```

3. **Client Identification**
   - Need to track which client originated which event
   - Only originating client receives ack (not broadcast)

**Key Design Decisions:**

| Question | Option A | Option B |
|----------|----------|----------|
| When does ack fire? | After local materialization | After leader confirmation |
| Offline behavior? | Fail immediately | Queue for later |
| Determinism? | Ack values must be deterministic | Allow non-deterministic |

**Complexity: Moderate** - Requires response channel but doesn't change core sync model.

---

### 8. Declarative Partitioning Strategy

**Sync Impact: Major**

This is the foundational change that enables multi-eventlog architecture.

```
┌─────────────────────────────────────────────────────────────┐
│                         Client                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Eventlog A   │  │ Eventlog B   │  │ Eventlog C   │      │
│  │ (workspace-1)│  │ (workspace-2)│  │ (workspace-3)│      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                 │               │
│         ▼                 ▼                 ▼               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   SQLite A   │  │   SQLite B   │  │   SQLite C   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Sync (per partition)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         Leader                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Eventlog A   │  │ Eventlog B   │  │ Eventlog C   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

**Protocol Changes Required:**

1. **Partition Discovery**
   ```typescript
   // Client needs to know which partitions exist
   interface PartitionDiscoveryRequest {
     baseStoreId: string
     partitionKeyPrefix?: string  // Optional filter
   }

   interface PartitionDiscoveryResponse {
     partitions: Array<{
       partitionKey: string
       eventlogId: string
       lastCursor: string
     }>
   }
   ```

2. **Multi-Cursor Sync**
   ```typescript
   // Current: Single cursor per store
   interface SyncState {
     cursor: string
   }

   // New: Cursor per partition
   interface MultiPartitionSyncState {
     partitions: Map<string, {
       eventlogId: string
       cursor: string
       syncStatus: 'synced' | 'syncing' | 'stale'
     }>
   }
   ```

3. **Event Routing**
   ```typescript
   // Schema declares partition key extraction
   partition: {
     strategy: 'hash',
     keyExtractor: (event) => event.args.workspaceId,
   }

   // Commit routes to correct eventlog
   store.commit(event) // → routes to eventlog-workspace-{workspaceId}
   ```

4. **Selective Sync**
   ```typescript
   // Client subscribes to subset of partitions
   interface SyncSubscription {
     partitionKeys: string[]  // Only sync these workspaces
   }
   ```

**Architecture Changes:**

| Component | Change |
|-----------|--------|
| Schema | Add `partition` configuration |
| Store | Manage multiple eventlogs |
| Sync Client | Multi-cursor tracking |
| Sync Server | Partition discovery endpoint |
| Adapter | Partition-aware storage |

**Complexity: High** - This is the biggest architectural change.

---

### 9. Compaction Generators

**Sync Impact: Major**

Compaction fundamentally changes the append-only nature of eventlogs by replacing events with summaries.

```
Before Compaction:
┌────────────────────────────────────────────────────────────┐
│ E1 │ E2 │ E3 │ E4 │ E5 │ E6 │ ... │ E999 │ E1000 │       │
└────────────────────────────────────────────────────────────┘

After Compaction:
┌────────────────────────────────────────────────────────────┐
│ CompactionEvent(E1-E500) │ E501 │ E502 │ ... │ E1000 │    │
└────────────────────────────────────────────────────────────┘
```

**Protocol Changes Required:**

1. **Compaction Event Type**
   ```typescript
   interface CompactionEvent {
     type: 'system.Compaction'
     args: {
       compactionId: string
       strategy: string  // 'daily-rollup', 'state-snapshot', etc.
       compactedRange: {
         fromCursor: string
         toCursor: string
         eventCount: number
       }
       summaryData: unknown  // Aggregated data
     }
   }
   ```

2. **Cursor Semantics Change**
   ```typescript
   // Current: Cursor is monotonic position in eventlog
   // New: Cursor must handle compaction boundaries

   interface CursorWithCompaction {
     position: string
     compactionEpoch: number  // Increments on each compaction
   }
   ```

3. **Sync Recovery After Compaction**
   ```
   Client cursor: E250 (in compacted range)
   Server state: [CompactionEvent(E1-E500), E501, ...]

   Options:
   A) Force full rematerialization (simple, slow)
   B) Send CompactionEvent + remaining events (complex, fast)
   C) Reject sync, require client upgrade (breaking)
   ```

4. **Compaction Trigger Protocol**
   ```typescript
   // Leader-initiated compaction
   interface CompactionTrigger {
     strategy: string
     targetEventlogId: string
     parameters: {
       maxEventsToCompact: number
       retentionPolicy: 'delete' | 'archive'
     }
   }
   ```

**Key Design Decisions:**

| Question | Option A | Option B |
|----------|----------|----------|
| Who triggers compaction? | Leader only | Client can request |
| Original events? | Delete after compaction | Archive to cold storage |
| Client with old cursor? | Force rematerialize | Send compaction delta |
| Compaction determinism? | Must be deterministic | Can vary by client |

**Complexity: High** - Changes fundamental eventlog semantics.

---

### 10. Fanout Patterns

**Sync Impact: Major (if server-side)**

Fanout patterns determine how updates reach subscribers. Two approaches:

**Option A: Client-Side Fanout (No protocol change)**
```typescript
// Client queries followers and updates locally
const followers = await store.query(tables.followers.where({ authorId }))
for (const follower of followers) {
  await store.commit(events.timelineUpdated({ userId: follower.id, postId }))
}
```

**Option B: Server-Side Fanout (Major protocol change)**
```typescript
// Server handles fanout after receiving post event
// Requires server-side materializer execution
```

**Protocol Changes (Option B):**

1. **Server-Side Materializers**
   ```typescript
   // Materializer runs on leader, not client
   interface ServerMaterializer {
     event: string
     handler: (args, ctx: ServerContext) => Promise<void>
   }

   interface ServerContext {
     query: QueryContext
     emit: (event: Event) => Promise<void>  // Emit to other partitions
     fanout: (recipients: string[], event: Event) => Promise<void>
   }
   ```

2. **Subscription Model**
   ```typescript
   // Client subscribes to receive fanout events
   interface FanoutSubscription {
     userId: string
     channels: string[]  // 'timeline', 'notifications', 'presence'
   }
   ```

3. **Delivery Tracking**
   ```typescript
   interface FanoutDelivery {
     eventId: string
     recipientId: string
     status: 'pending' | 'delivered' | 'failed'
     attempts: number
   }
   ```

**Complexity: High** - Requires server-side materializer execution model.

---

## Summary: Protocol Impact Matrix

| Feature | Eventlog | Cursor | Commit | Sync | New Messages |
|---------|----------|--------|--------|------|--------------|
| #1 Aggregators | - | - | - | - | - |
| #2 Time Bucketing | - | - | - | - | - |
| #3 Cross-Partition | Multi | Multi | Coord | Multi | 3 |
| #4 Migrations | Meta | - | - | Meta | 1 |
| #5 Task Globals | - | - | - | - | - |
| #6 Subindexed | - | - | - | - | - |
| #7 Ack Returns | - | - | Response | - | 1 |
| #8 Partitioning | Multi | Multi | Route | Multi | 2 |
| #9 Compaction | Replace | Epoch | - | Recovery | 2 |
| #10 Fanout | - | - | - | Server | 3 |

Legend:
- `-` = No change
- `Multi` = Multiple instances
- `Meta` = Metadata addition
- `Coord` = Coordination required
- `Route` = Routing logic
- `Replace` = Event replacement
- `Epoch` = Epoch tracking
- `Response` = Response enhancement
- `Recovery` = Recovery mechanism
- `Server` = Server-side execution

---

## Recommended Implementation Order

### Phase 1: No Protocol Changes
1. Aggregator Primitives (utility library)
2. Task Globals (Effect patterns)
3. Time Bucketing (materializer patterns)
4. Idempotent Migrations (Option A - materializer only)

### Phase 2: Minor Protocol Extensions
5. Ack Returns (commit response enhancement)
6. Subindexed Collections (schema extension)

### Phase 3: Major Protocol Evolution
7. Partitioning Strategy (multi-eventlog foundation)
8. Compaction Generators (eventlog size management)

### Phase 4: Advanced Patterns (Requires Phase 3)
9. Cross-Partition Transactions (2PC coordinator)
10. Fanout Patterns (server-side materializers)

---

## Risk Assessment

| Feature | Breaking Change | Migration Path | Rollback |
|---------|-----------------|----------------|----------|
| Aggregators | No | N/A | N/A |
| Time Bucketing | No | N/A | N/A |
| Cross-Partition | Yes | Version negotiation | Complex |
| Migrations | No | Additive | Easy |
| Task Globals | No | N/A | N/A |
| Subindexed | No | Schema migration | Medium |
| Ack Returns | No | Feature flag | Easy |
| Partitioning | Yes | Store migration | Complex |
| Compaction | Yes | Epoch versioning | Complex |
| Fanout | Yes | Feature flag | Medium |

---

## Open Questions

1. **Compaction + Offline**: How do offline clients handle catching up after compaction?
2. **Partition Discovery**: Push (server notifies) vs Pull (client polls)?
3. **Cross-Partition Consistency**: Eventual vs Strong consistency model?
4. **Ack Determinism**: Must ack values be deterministic for replay?
5. **Server Materializers**: Who pays for server-side compute (fanout)?
