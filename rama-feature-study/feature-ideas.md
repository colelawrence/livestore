# LiveStore Feature Ideas Inspired by Rama

This document catalogs feature ideas for LiveStore inspired by patterns observed in Rama's demo gallery modules and the Twitter-scale Mastodon implementation.

## Table of Contents

1. [Aggregator Primitives](#1-aggregator-primitives)
2. [Multi-Granularity Time Bucketing](#2-multi-granularity-time-bucketing)
3. [Cross-Partition Transactions](#3-cross-partition-transactions)
4. [Idempotent Migration Functions](#4-idempotent-migration-functions)
5. [Task Globals for IO Resources](#5-task-globals-for-io-resources)
6. [Subindexed Collections](#6-subindexed-collections)
7. [Ack Returns for Event Commits](#7-ack-returns-for-event-commits)
8. [Declarative Partitioning Strategy](#8-declarative-partitioning-strategy)
9. [Compaction Generators](#9-compaction-generators)
10. [Fanout Patterns for Scalable Delivery](#10-fanout-patterns-for-scalable-delivery)

---

## 1. Aggregator Primitives

**Rama Sources:**
- `time_series_module.clj`: `+combine-measurements` combiner aggregator
- `top_users_module.clj`: `+top-monotonic` aggregator for leaderboards
- `bank_transfer_module.clj`: `+sum` aggregator for funds

**Rama Pattern:**
Rama provides declarative aggregators that specify how to combine values incrementally. The `combiner` aggregator defines a combine function and an init function, enabling stateless merging of partial results.

```clojure
;; From time_series_module.clj
(def +combine-measurements
  (combiner
    (fn [window-stats1 window-stats2]
      (->WindowStats
        (+ (:cardinality window-stats1) (:cardinality window-stats2))
        (+ (:total window-stats1) (:total window-stats2))
        (or (:last-millis window-stats2) (:last-millis window-stats1))
        (min-nullable (:min-latency-millis window-stats1) (:min-latency-millis window-stats2))
        (max-nullable (:max-latency-millis window-stats1) (:max-latency-millis window-stats2))))
    :init-fn (fn [] (->WindowStats 0 0 nil nil nil))))
```

**LiveStore Opportunity:**
Provide built-in aggregator primitives for common analytics patterns that work within materializers.

### Concrete Examples

Aggregator primitives enable declarative, incremental computation of statistics. Key patterns include:

**1. Counter Aggregator** - Simple incrementing counts (pageviews, clicks)
```typescript
const counterAgg = (curr: number, delta: number) => curr + delta
```

**2. Sum Aggregator** - Running totals (revenue, usage)
```typescript
const sumAgg = (curr: number, value: number) => curr + value
```

**3. Min/Max Aggregators** - Extremes tracking (latency bounds)
```typescript
const minAgg = (curr: number | null, value: number) =>
  curr === null ? value : Math.min(curr, value)
```

**4. Top-N Aggregator** - Leaderboards with bounded memory (like Rama's `+top-monotonic`)
```typescript
const topNAgg = (curr: Item[], newItem: Item, n: number) =>
  [...curr, newItem].sort((a, b) => b.score - a.score).slice(0, n)
```

**5. Set Union** - Unique value tracking (unique visitors per day)
```typescript
const setUnionAgg = (curr: Set<string>, value: string) => curr.add(value)
```

**6. Window Statistics** - Combined stats (count, total, min, max, avg)
```typescript
interface WindowStats {
  count: number; total: number; min: number | null; max: number | null;
}
const windowStatsAgg = (curr: WindowStats, value: number): WindowStats => ({
  count: curr.count + 1,
  total: curr.total + value,
  min: curr.min === null ? value : Math.min(curr.min, value),
  max: curr.max === null ? value : Math.max(curr.max, value),
})
```

**7. Custom Combiner** - Merge partial aggregations (matches Rama's `combiner` pattern)
```typescript
// Two-phase: aggregate locally, then combine across partitions
const combineWindowStats = (a: WindowStats, b: WindowStats): WindowStats => ({
  count: a.count + b.count,
  total: a.total + b.total,
  min: minNullable(a.min, b.min),
  max: maxNullable(a.max, b.max),
})
```

**LiveStore Implementation Opportunity:** Provide `State.SQLite.aggregatedColumn()` or `Schema.Aggregator()` primitives that declare combining logic declaratively.

---

## 2. Multi-Granularity Time Bucketing

**Rama Sources:**
- `time_series_module.clj`: `emit-index-granularities` custom operation
- Multi-level bucketing (minute/hour/day/month)
- Colocated query topology for efficient range queries

**Rama Pattern:**
```clojure
;; Emit buckets at all granularities for a single timestamp
(deframaop emit-index-granularities [*timestamp-millis]
  (long (/ *timestamp-millis (* 1000 60)) :> *minute-bucket)
  (long (/ *minute-bucket 60) :> *hour-bucket)
  (long (/ *hour-bucket 24) :> *day-bucket)
  (long (/ *day-bucket 30) :> *thirty-day-bucket)
  (:> :m *minute-bucket)
  (:> :d *day-bucket)
  (:> :h *hour-bucket)
  (:> :td *thirty-day-bucket))
```

**LiveStore Opportunity:**
Built-in time-series table helpers that automatically maintain multiple granularity indexes.

### Concrete Examples

> See complete code examples in [`time-bucketing-examples.md`](./time-bucketing-examples.md) for full implementations.

Multi-granularity time bucketing enables efficient time-series analytics by storing aggregated statistics at multiple time scales (minute/hour/day/month). This dramatically reduces query costs: querying 1 year of data reads ~12 month buckets instead of 525,600 minute buckets.

#### Pattern Overview

**1. API Latency Tracking** - Track request latencies at minute/hour/day/month for performance monitoring
```typescript
export const tables = {
  latencyStats: State.SQLite.table({
    name: 'latencyStats',
    columns: {
      endpoint: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'm' | 'h' | 'd' | 'mo'
      bucket: State.SQLite.integer(),
      count: State.SQLite.integer({ default: 0 }),
      totalMs: State.SQLite.integer({ default: 0 }),
      minMs: State.SQLite.integer({ nullable: true }),
      maxMs: State.SQLite.integer({ nullable: true }),
    },
    primaryKey: ['endpoint', 'granularity', 'bucket'],
  }),
}
```

**2. User Activity Metrics (DAU/WAU/MAU)** - Track daily/weekly/monthly active users with automatic rollup

**3. Revenue Analytics** - Track revenue at hourly/daily/monthly for financial dashboards

**4. Error Rate Monitoring** - Track error counts at minute/hour/day for alerting

**5. Usage Metering for Billing** - API call counts for usage-based billing

**6. Advanced Query Patterns** - Automatic granularity selection and batch multi-endpoint queries

#### Query Efficiency Gains

| Time Range | Naive (minutes only) | Optimized (multi-granularity) | Savings |
|------------|---------------------|-------------------------------|---------|
| 1 hour | 60 buckets | 60 buckets | 1x |
| 1 day | 1,440 buckets | 48 buckets | 30x |
| 30 days | 43,200 buckets | 60 buckets | 720x |

See [`time-bucketing-examples.md`](./time-bucketing-examples.md) for full implementations.

---

## 3. Cross-Partition Transactions

**Rama Sources:**
- `bank_transfer_module.clj`: Exactly-once microbatch semantics
- Cross-partition fund transfers with atomic guarantees
- `|hash` partition hopping for coordinated updates

**Rama Pattern:**
```clojure
;; Deduct from sender
(<<if *success?
  (local-transform> [(keypath *from-user-id) (term %deduct)] $funds))
;; Partition hop to receiver
(|hash *to-user-id)
;; Credit to receiver (exactly-once across the whole microbatch)
(<<if *success?
  (+compound $funds {*to-user-id (aggs/+sum *amt)}))
```

**LiveStore Opportunity:**
Formalized multi-table transactional events with rollback guarantees.

### Concrete Examples

#### Overview

Cross-partition transactions enable atomic operations across multiple entities that may be stored on different partitions. This is critical for maintaining consistency in distributed scenarios like fund transfers, inventory moves, or role reassignments.

**Key Requirements:**
- Exactly-once semantics: Each transaction executes once, even if retried due to failures
- Atomic visibility: All updates succeed together or none succeed
- Validation before commit: Check preconditions (e.g., sufficient funds) before applying changes
- Consistent failure states: Both sides of the transaction record the same outcome
- Audit trail: Both sides record the transaction in their history

**See the full detailed examples with complete code in:** [`cross-partition-transactions-examples.md`](./cross-partition-transactions-examples.md)

#### Example Patterns Covered

1. **Fund Transfer Between Accounts** - Transfer money with insufficient funds protection
   - Validation: Check sender has sufficient funds before deducting
   - Atomic: Deduct from sender, credit to receiver in single transaction
   - Audit: Record in both `outgoingTransfers` and `incomingTransfers` tables
   - Failure handling: Both sides record `success: false` with consistent `failureReason`

2. **Inventory Transfer Between Warehouses** - Move physical inventory with two-phase commit
   - Phase 1: Reserve inventory at source location
   - Phase 2: Complete transfer (deduct source, add to destination)
   - Cancellation: Release reservations if transfer cancelled
   - Status tracking: `pending` -> `completed` or `failed`

3. **Team Member Reassignment** - Move user between teams atomically
   - Remove from source team, add to destination team
   - Update member counts on both teams
   - Validation: Ensure user is on source team before transfer
   - History: Complete audit trail of all membership changes

4. **Order Fulfillment** - Multi-table coordination across products, orders, and customers
   - Validate inventory for all items in order
   - Validate customer credit if paying with credit
   - Deduct inventory, create order items, update customer balance
   - All-or-nothing: Either all items fulfilled or order fails

5. **Permission Delegation / Ownership Transfer** - Transfer admin rights between users
   - Validate current owner before transfer
   - Atomic revoke + grant prevents ownership gaps
   - Optional downgrade: Previous owner becomes admin instead of removed
   - Permission history for security audit

6. **Escrow Pattern** - Hold funds until conditions met, then release or refund
   - Three-state lifecycle: `held` -> `released` or `refunded`
   - Funds locked (unavailable) but not transferred until release
   - Atomic release prevents double-spending
   - Support both happy path (release) and failure path (refund)

#### Common Implementation Patterns

**1. Validation-First Approach:**
```typescript
// Always validate before making any state changes
const isValid = checkPreconditions()
const failureReason = isValid ? null : 'reason_code'

if (isValid) {
  // Make state changes
}

// Always record outcome (success or failure)
recordTransaction({ success: isValid, failureReason })
```

**2. Atomic Multi-Entity Updates:**
```typescript
// All operations in array execute atomically
return [
  table1.update({ ... }),
  table2.update({ ... }),
  table3.insert({ ... }),
  historyTable.insert({ ... })
]
```

**3. Idempotency via Status Checks:**
```typescript
const record = tables.transfers.select().where({ id }).one()
if (!record || record.status !== 'pending') {
  return [] // Already processed or invalid
}
```

**4. Consistent Dual Recording:**
```typescript
// Both sides of transaction record the same outcome
operations.push(
  outgoingTable.insert({ id, success, failureReason }),
  incomingTable.insert({ id, success, failureReason })
)
```

#### LiveStore-Specific Considerations

**Current Limitations:**
1. No built-in partition hopping (unlike Rama's `|hash` operator)
2. All materializer operations execute on the same partition
3. No distributed transaction coordinator

**Workarounds:**
1. Use single eventlog to ensure serializable execution
2. Optimistic locking via version number checks
3. Compensation events for rollback of multi-step transactions

**Future Enhancements Needed:**
1. Partition-aware events that trigger materializers on multiple partitions
2. Two-phase commit support with built-in coordinator
3. Cross-partition queries spanning multiple partitions
4. Saga pattern support for long-running transactions

#### Summary of Use Cases

These patterns are essential for:
- **Financial applications**: Payments, transfers, escrow, credits
- **Inventory systems**: Stock transfers, reservations, order fulfillment
- **Multi-tenant apps**: Team assignments, permission transfers
- **Workflow orchestration**: Multi-step processes with rollback support

---

## 4. Idempotent Migration Functions

**Rama Sources:**
- `migrations_music_catalog_modules.clj`: `migrated` schema wrapper
- Migration ID for tracking which migrations have run
- Idempotent migration functions that handle both old and new data

**Rama Pattern:**
```clojure
(migrated
  (fixed-keys-schema
    {:name String
     :songs (vector-schema Song)})
  "parse-song-data"  ; migration ID
  migrate-songs)     ; idempotent function

(defn- migrate-songs [album]
  (if (some-> album :songs first string?)
    (update album :songs (partial mapv parse-song))
    album))  ; Already migrated, return unchanged
```

**LiveStore Opportunity:**
Per-column idempotent migrations that can run incrementally during rematerialization.

### Concrete Examples

Idempotent migrations enable safe schema evolution without data loss. Key patterns:

**1. String to Array Migration** - Convert comma-separated to proper array
```typescript
function migrateTagsField(record: { tags: string | string[] }) {
  if (typeof record.tags === 'string') {
    return { ...record, tags: record.tags.split(',').map(t => t.trim()) }
  }
  return record // Already migrated
}
```

**2. Computed Field Backfill** - Add derived column to existing records
```typescript
function migrateWithSlug(record: { title: string; slug?: string }) {
  if (record.slug === undefined) {
    return { ...record, slug: slugify(record.title) }
  }
  return record // Already has slug
}
```

**3. Enum Migration** - Update status values
```typescript
function migrateStatusEnum(record: { status: string }) {
  const mapping = { 'active': 'published', 'inactive': 'draft' }
  return { ...record, status: mapping[record.status] ?? record.status }
}
```

**4. Nested Structure Restructure** - Flatten or nest fields
```typescript
function migrateAddressFields(record: { city?: string; address?: { city: string } }) {
  if (record.city && !record.address) {
    return { address: { city: record.city, country: 'unknown' } }
  }
  return record // Already nested
}
```

**5. Default Value Backfill** - Add required fields with defaults
```typescript
function migrateWithDefaults(record: { priority?: number }) {
  return { priority: 0, ...record } // Spread preserves existing values
}
```

**6. Type Narrowing** - Parse loose types into structured data
```typescript
function migrateMetadataField(record: { metadata: string | object }) {
  if (typeof record.metadata === 'string') {
    return { ...record, metadata: JSON.parse(record.metadata) }
  }
  return record
}
```

**Rama Pattern:** Wrap schema with `(migrated schema "migration-id" migration-fn)` - the migration ID ensures each migration runs exactly once per record.

**LiveStore Opportunity:** Provide `State.SQLite.migratedColumn()` that runs idempotent transforms during rematerialization, with migration tracking to avoid re-running.

---

## 5. Task Globals for IO Resources

**Rama Sources:**
- `rest_api_integration_module.clj`: `TaskGlobalObject` interface
- HTTP client lifecycle management per task
- `completable-future>` for async integration

**Rama Pattern:**
```clojure
(deftype AsyncHttpClientTaskGlobal
  [^{:unsynchronized-mutable true :tag AsyncHttpClient} client]
  TaskGlobalObject
  (prepareForTask [this task-id task-global-context]
    (set! client (Dsl/asyncHttpClient)))
  (close [this]
    (.close client)))
```

**LiveStore Opportunity:**
Managed resource lifecycle for external service integrations (webhooks, AI APIs, etc.).

### Concrete Examples

**Complete detailed examples available in**: [`task-globals-examples.md`](./task-globals-examples.md)

This document provides 6 production-ready TypeScript examples demonstrating managed resource lifecycle patterns using Effect's Layer system:

#### 1. HTTP Client Pool for Webhooks
- Reusable fetch client with connection pooling
- Fire-and-forget webhook delivery with retry logic
- Exponential backoff and timeout protection
- Example: Send notifications on order creation

#### 2. AI/LLM Client (OpenAI/Anthropic)
- Managed AI SDK clients with connection pooling
- Concurrent API calls with concurrency control
- Generate embeddings and summaries for content
- Example: Article publishing with AI-generated metadata

#### 3. Database Connection Pool (PostgreSQL)
- External database connection for data enrichment
- Connection pool with health checks
- Graceful fallback on query failures
- Example: Enrich user signups with CRM data

#### 4. Message Queue Client (Kafka/Redis)
- Kafka producer and Redis pub/sub clients
- Fire-and-forget event publishing
- Automatic reconnection handling
- Example: Publish order shipment events to external systems

#### 5. File System Handles
- Managed file handles for audit logs and exports
- Batched writes with automatic flushing
- Background workers for async I/O
- Example: Append to audit log files

#### 6. Cache Client (Redis)
- Redis cache with connection pooling
- Cache-aside pattern with automatic fallback
- Two-tier caching (memory + Redis)
- Cache invalidation on updates
- Example: Cache expensive user profile enrichment

Each example demonstrates:
- **Resource definition**: Using Effect's `Layer.scoped` with lifecycle management
- **Accessing resources**: Via `Context.Tag` in materializers
- **Error handling**: Retry logic, timeouts, graceful degradation
- **Cleanup**: Automatic via `Scope.addFinalizer`
- **Usage patterns**: Sync (blocking) vs async (fire-and-forget) operations

**Key architectural patterns**:
```typescript
// Resource definition with lifecycle
const makeResourceLayer = Layer.scoped(
  ResourceTag,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope
    const resource = yield* initializeResource()
    yield* Scope.addFinalizer(scope, cleanup(resource))
    return { _tag: 'ResourceTag', resource }
  })
)

// Usage in materializer
'v1.EventName': (args, { query }) =>
  Effect.gen(function* () {
    const { resource } = yield* ResourceTag
    yield* useResource(resource)
    return tables.foo.insert(args)
  }).pipe(
    Effect.provide(makeResourceLayer),
    Effect.timeout('10 seconds'),
  )

// Store integration
const store = yield* createStore({
  schema,
  layer: Layer.mergeAll(
    makeHttpClientLayer,
    makeAIClientLayer,
    makeDatabasePoolLayer,
  ),
})
```

**Benefits**:
- **Performance**: Connection pooling eliminates per-event setup overhead
- **Resource safety**: Automatic cleanup prevents leaks
- **Error resilience**: Graceful degradation when external services fail
- **Observability**: Centralized logging and error tracking
- **Testability**: Easy mocking via Layer substitution
- **Type safety**: Full TypeScript inference for all resources

**Comparison to Rama**:
- Rama uses `TaskGlobalObject` interface with `prepareForTask`/`close` lifecycle
- LiveStore can use Effect's `Layer.scoped` with `Scope.addFinalizer`
- Rama's `completable-future>` maps to `Effect.tryPromise` + `Effect.fork`
- Both provide thread-safe resource management with cleanup guarantees

See [`task-globals-examples.md`](./task-globals-examples.md) for full implementation details, error handling patterns, and production-ready code.

---

## 6. Subindexed Collections

**Rama Sources:**
- All modules use `{:subindex? true}` for large nested maps
- Twitter clone: `$$partitionedFollowers`, `$$hashtagToFollowers`
- Efficient storage/querying of millions of elements

**Rama Pattern:**
```clojure
;; From bank_transfer_module.clj - transfers can be huge per user
(declare-pstate mb $outgoing-transfers
  {Long ; user-id
   (map-schema String ; transfer-id
               (fixed-keys-schema {:to-user-id Long :amt Long :success? Boolean})
               {:subindex? true})})
```

**LiveStore Opportunity:**
Better modeling for hierarchical data without separate join tables.

### Concrete Examples

See [subindexed-collections-examples.md](./subindexed-collections-examples.md) for comprehensive TypeScript/LiveStore examples covering:

1. **User Transactions (Banking)** - Thousands of transfers per user with efficient pagination
   - Current: Separate `transactions` table with composite indexes
   - Hypothetical: Subindexed `outgoingTransactions` and `incomingTransactions` maps
   - Operations: Single entry insertion, cursor pagination, O(1) size queries, individual deletion

2. **Project Tasks with Status Filtering** - Tasks organized by status buckets
   - Current: Single `tasks` table with multi-column index on `(projectId, status, priority)`
   - Hypothetical: Separate subindexed maps per status (`todoTasks`, `inProgressTasks`, `doneTasks`)
   - Operations: Status transitions (move between buckets), filtered queries, priority sorting

3. **Social Graph (Followers/Following)** - Millions of follow relationships
   - Current: `follows` join table with denormalized counters
   - Hypothetical: Subindexed `followers` and `following` maps on users table
   - Operations: O(1) relationship checks, follower counts without scans, paginated follower lists

4. **Notification Timeline** - Bounded activity feed per user
   - Current: `notifications` table with user index
   - Hypothetical: Bounded subindexed map with automatic eviction (like Rama's `KeyToFixedItemsPStateGroup`)
   - Operations: Range queries by timestamp, mark individual as read, clear all, automatic LRU eviction

5. **Comment Threads with Nested Replies** - Tree-structured discussions
   - Current: Adjacency list with `parentCommentId` and denormalized counters
   - Hypothetical: Nested subindexed maps (comments contain replies map)
   - Operations: Top-level pagination, nested reply access, hierarchical counts

6. **Activity Feed with Time-Based Queries** - User activity timeline
   - Current: `activities` table with timestamp index
   - Hypothetical: Sorted subindexed map keyed by timestamp
   - Operations: Time range queries (last 24h, between dates), type filtering, cursor pagination

#### Key Insights from Examples

**When Subindexed Collections Shine:**
- One-to-many with high cardinality (thousands to millions of children)
- Strong parent-based access patterns (always query by parent ID)
- Frequent pagination and range queries (timelines, feeds)
- Individual entry operations (add/remove/update single items)
- O(1) size/existence queries needed

**Current LiveStore Strengths:**
- Join tables with indexes work well for most use cases
- SQL-native, flexible for ad-hoc queries
- Established patterns, familiar to developers
- Good enough for moderate cardinalities (< 10k children per parent)

**Trade-offs to Consider:**
- Locality vs Flexibility: Subindexed = better locality, joins = better flexibility
- Schema complexity: Nested structures vs flat normalized tables
- Migration difficulty: Changing nested schemas harder than adding columns
- Query patterns: Subindexed optimized for parent-scoped access, joins better for cross-cutting queries

**Potential LiveStore Implementation:**
```typescript
// Hypothetical API surface
State.SQLite.subindexedMap({
  keySchema: Schema.String,
  valueSchema: Schema.Struct({ ... }),
  sortBy: 'timestamp', // or 'priority', 'key'
  sortOrder: 'desc',
  maxSize: 5000, // Optional: auto-evict oldest
})

// Query operations
table.selectSubindexedMap('column')
  .where({ id: parentId })
  .rangeFrom(cursor)
  .limit(50)

// Update operations
table.updateSubindexedMap('column')
  .set(key, value)
  .delete(key)
  .where({ id: parentId })
```

See full examples with code snippets in [subindexed-collections-examples.md](./subindexed-collections-examples.md).

---

## 7. Ack Returns for Event Commits

**Rama Sources:**
- `profile_module.clj`: `ack-return>` for returning generated IDs
- Stream topology returns data to depot append clients

**Rama Pattern:**
```clojure
;; Return generated user ID to the client that appended the registration
(ack-return> *user-id)
```

**LiveStore Opportunity:**
Synchronous feedback from materializers without polling.

### Concrete Examples

This feature would enable materializers to return values synchronously to the client that committed an event. Critical use cases include:

#### 1. Generated ID Return
When the server generates IDs (nanoid, UUID), clients need immediate access for navigation/routing:

```typescript
const result = await store.commit(
  events.postCreated({ title, content, authorId })
)

if (result.ok) {
  const { postId } = result.value // Server-generated ID
  navigate(`/posts/${postId}`)
}
```

**Pattern:** Materializer generates ID with `nanoid()`, calls `ackReturn({ postId })`, client awaits commit result.

#### 2. Validation Result Return
Server-side validation (uniqueness checks, business rules) with immediate feedback:

```typescript
const result = await store.commit(
  events.userRegistered({ username, email, password })
)

if (result.ok && !result.value.success) {
  // Validation failed
  if (result.value.error === 'USERNAME_TAKEN') {
    setErrors({ username: result.value.message })
  }
}
```

**Pattern:** Materializer queries existing data, returns `{ success: boolean, error?: string }`, client handles validation errors without polling.

#### 3. Computed Field Return
Server computes derived values (slugs, hashes, normalized data) and returns them:

```typescript
const result = await store.commit(
  events.articleCreated({ title, content })
)

if (result.ok) {
  const { slug } = result.value // Server-computed URL slug
  navigate(`/articles/${slug}`)
  toast.success(`Published: ${location.origin}/articles/${slug}`)
}
```

**Pattern:** Materializer computes slug with uniqueness check, returns computed value for immediate use in UI.

#### 4. Conflict Detection
Optimistic concurrency control with version checks:

```typescript
const result = await store.commit(
  events.documentUpdated({
    id: docId,
    content: newContent,
    expectedVersion: currentVersion,
  })
)

if (result.ok && !result.value.success) {
  if (result.value.conflict === 'VERSION_MISMATCH') {
    showConflictResolutionUI(result.value.currentContent)
  }
}
```

**Pattern:** Materializer checks version, returns conflict info if mismatch, client handles merge/overwrite decision.

#### 5. Created Entity Return
Server applies defaults and returns fully-materialized entity:

```typescript
const result = await store.commit(
  events.taskCreated({ title, assigneeId })
)

if (result.ok) {
  const { task } = result.value
  // task includes server-computed defaults:
  // { id, title, assigneeId, priority: 'medium', status: 'open', createdAt, ... }

  toast.success(`Task created with ${task.priority} priority`)
}
```

**Pattern:** Materializer computes all defaults (priority based on workload, timestamps, etc.), returns complete entity for optimistic UI updates.

#### 6. Batch Result Return
Bulk operations return per-item success/failure details:

```typescript
const result = await store.commit(
  events.contactsBatchImported({ contacts: [...] })
)

if (result.ok) {
  const { total, succeeded, failed, results } = result.value
  // results: [{ index: 0, success: true, contactId: '...' }, { index: 1, success: false, error: 'Duplicate email' }, ...]

  toast.warning(`Imported ${succeeded} of ${total} contacts`)
  showFailedImports(results.filter(r => !r.success))
}
```

**Pattern:** Materializer processes array, validates each item, returns detailed per-item results for user feedback.

### Implementation Design

**Type Safety:**
```typescript
// Materializer context gains ackReturn function
type MaterializerContext<TEventDef> = {
  query: MaterializerContextQuery
  ackReturn: <TAck>(value: TAck) => void
  // ...existing fields
}

// Store.commit return type inferred from ackReturn calls
store.commit: <TEvent>(event: TEvent) =>
  Promise<Result<AckReturnType<TEvent>, MaterializeError>>
```

**Error Handling:**
- Materializer exceptions: `Result.error(MaterializeError)`
- Validation failures: `Result.ok({ success: false, error: '...' })`
- Success: `Result.ok({ success: true, ... })`

**Key Design Questions:**

1. **When does ack fire?**
   - After local materialization? (instant, no server round-trip)
   - After leader materialization? (consistent, but slower)
   - Both? (local first, leader confirmation later?)

2. **Offline behavior:**
   - Queue acks for later delivery?
   - Fail immediately with timeout error?
   - Return partial result (local only)?

3. **Multi-client consistency:**
   - Only originating client receives ack
   - Other clients' materializers run but don't send acks
   - How to distinguish originating client in distributed system?

4. **Determinism:**
   - Ack values must be deterministic (same event → same ack)
   - No side effects (HTTP calls, random numbers outside of event args)
   - Must work correctly during replay/rematerialization

**See `ack-returns-examples.md` for detailed TypeScript examples of all 6 patterns.**

---

## 8. Declarative Partitioning Strategy

**Rama Sources:**
- All modules: `(hash-by :field)` depot partitioners
- Twitter clone: Explicit control over where data lands
- `$$partitionedFollowersControl` for balanced fanout

**Rama Pattern:**
```clojure
;; Partition depot by user-id so processing starts on correct task
(declare-depot setup *transfer-depot (hash-by :from-user-id))

;; Twitter clone: Spread followers across tasks for balanced fanout
stream.pstate("$$partitionedFollowers",
  PState.mapSchema(Long.class, PState.mapSchema(Long.class, Follower.class).subindexed()));
```

**LiveStore Opportunity:**
Explicit partitioning for multi-eventlog scenarios (per-workspace, per-document).

### Concrete Examples

See full detailed examples in [`partitioning-examples.md`](./partitioning-examples.md) covering 6 production-ready patterns.

#### Overview

In Rama, all depots declare partitioning strategies using `(hash-by :field)` to ensure related data lands on the same task for collocated processing. This enables:
- **Data locality**: Related events processed on same worker
- **Scalable fanout**: Spread followers across tasks (every 1000 triggers new task assignment)
- **Isolation**: Multi-tenant data separation

**Current LiveStore limitation:** Assumes 1:1 mapping between eventlog and SQLite database ([#255](https://github.com/livestorejs/livestore/issues/255)).

#### Patterns Covered

**1. Workspace Partitioning (Multi-Tenant)** - SaaS apps with isolated workspace data (Slack, Notion)
```typescript
partition: {
  strategy: 'hash',
  keyExtractor: (event) => event.args.workspaceId,
  eventlogIdTemplate: (baseId, key) => `${baseId}-workspace-${key}`,
}
```

**2. Document Partitioning (Collaborative Editing)** - Each document has own eventlog (Google Docs, Figma)
```typescript
partition: {
  strategy: 'hash',
  keyExtractor: (event) => event.args.documentId,
}
```

**3. User Partitioning (Personal Data)** - User-scoped data isolation (notes, bookmarks)
```typescript
partition: {
  strategy: 'hash',
  keyExtractor: (event) => event.args.userId,
}
```

**4. Geographic Partitioning** - Regional data residency compliance
```typescript
partition: {
  strategy: 'range',
  keyExtractor: (event) => event.args.region,
  ranges: ['us-east', 'us-west', 'eu-west', 'ap-south'],
}
```

**5. Tenant Partitioning (B2B SaaS)** - Enterprise customer isolation
```typescript
partition: {
  strategy: 'hash',
  keyExtractor: (event) => event.args.tenantId,
}
```

**6. Time-Based Partitioning** - Historical data archival and hot/cold tiering
```typescript
partition: {
  strategy: 'time',
  keyExtractor: (event) => getMonthBucket(event.args.timestamp),
}
```

#### Benefits

| Benefit | Description |
|---------|-------------|
| **Isolation** | Workspace A's events never mix with Workspace B |
| **Sync Performance** | Only sync relevant partition to each client |
| **Horizontal Scaling** | Add workers per partition independently |
| **Compliance** | Geographic data residency requirements |

See [`partitioning-examples.md`](./partitioning-examples.md) for full TypeScript implementations.

---

## 9. Compaction Generators

**Rama Sources:**
- `top_users_module.clj`: `defgenerator` for batch processing
- Subbatch pattern for aggregation before final computation

**Rama Pattern:**
```clojure
(defgenerator user-spend-subbatch [microbatch]
  (batch<- [*user-id *total-spend-cents]
    (microbatch :> {:keys [*user-id *purchase-cents]})
    (|hash *user-id)
    (+compound $user-total-spend
      {*user-id (aggs/+sum *purchase-cents :new-val> *total-spend-cents)})))
```

**LiveStore Opportunity:**
Structured approach to eventlog compaction addressing eventlog size management (GitHub issue #136) and long-running application requirements.

### Concrete Examples

See full detailed examples in [compaction-generators-examples.md](./compaction-generators-examples.md) covering 6 production-ready patterns with TypeScript code.

#### Quick Summary

**Pattern 1: Purchase Event Summarization** - Daily rollup of e-commerce transactions
- Input: 1000 events/day × 200 bytes = 200 KB/day
- Output: 50 daily summaries × 150 bytes = 7.5 KB/day
- **Savings: 96%** | Trade-off: Daily totals preserved, individual receipts lost

**Pattern 2: Activity Log Compaction** - Hourly digests of user interactions
- Input: 1M events/week × 100 bytes = 100 MB/week
- Output: 10k hourly digests × 120 bytes = 1.2 MB/week
- **Savings: 99%** | Trade-off: Hourly patterns preserved, click-stream lost

**Pattern 3: Metric Rollup** - Multi-tier time-series compaction (minute→hour→day)
- Input: 1.5M minute-level events/month = ~220 MB
- Output: 15k daily summaries/month = 2.2 MB
- **Savings: 99%** | Trade-off: Statistical summaries only, raw data points lost

**Pattern 4: State Snapshot Compaction** - Replace edit chains with snapshots
- Input: 142 edit events × 120 bytes = 17 KB per document
- Output: 1 snapshot event ≈ (content + 200 bytes)
- **Savings: 90%** | Trade-off: Final state preserved, edit history lost

**Pattern 5: CRUD Collapse** - Consolidate entity lifecycle into final state
- Input: 6 lifecycle events × 150 bytes = 900 bytes per entity
- Output: 1 final state event × 180 bytes = 180 bytes
- **Savings: 80%** | Trade-off: Creation + final state, intermediate changes lost

**Pattern 6: GDPR-Compliant Retention** - Multi-tier with automatic PII anonymization
- Hot tier (0-30d): Full detail (100 MB)
- Warm tier (30d-7y): Daily summaries, no PII (5 MB)
- Cold tier (>7y): Deleted with audit event
- **Savings: 95%** | Compliance: Right-to-erasure, retention policies

Each pattern includes:
- Compaction trigger (time-based, count-based, per-entity)
- Generator/aggregation logic (inspired by Rama's `batch<-` pattern)
- Input/output event examples with size calculations
- Audit trail trade-offs and compliance considerations
- Implementation patterns (transactional safety, rematerialization, GDPR hooks)

---

## 10. Fanout Patterns for Scalable Delivery

**Rama Sources:**
- Twitter clone Core module: Timeline fanout to millions of followers
- `$$statusIdToLocalFollowerFanouts` for resumable fanout
- Bloom filters for efficient reply filtering
- Partitioned followers for balanced processing

**Rama Pattern:**
```java
// From Core.java - balanced follower fanout across partitions
stream.pstate("$$partitionedFollowersControl",
  PState.mapSchema(Long.class, List.class));  // account ID -> list of task IDs
stream.pstate("$$partitionedFollowers",
  PState.mapSchema(Long.class, PState.mapSchema(Long.class, Follower.class).subindexed()));

// Fanout continuation for fault tolerance
fan.pstate("$$statusIdToLocalFollowerFanouts",
  PState.mapSchema(Long.class, List.class)); // List<FollowerFanout>
```

**LiveStore Opportunity:**
Patterns for efficiently delivering updates to many subscribers (notifications, presence, activity feeds).

### Concrete Examples

See [fanout-examples.md](./fanout-examples.md) for detailed TypeScript implementations.

#### 1. Activity Feed Delivery (Twitter-style)
When a user posts, fan out to millions of followers:

```typescript
// Partitioned followers across workers for balanced processing
const schema = {
  partitionedFollowersControl: {
    [authorId: string]: number[];  // which workers handle this author
  },
  partitionedFollowers: {
    [authorId: string]: {
      [followerId: string]: { showBoosts: boolean; languages?: string[] };
    };
  },
  // Resumable fanout state for fault tolerance
  postIdToFollowerFanouts: {
    [postId: string]: Array<{
      authorId: string;
      nextFollowerIndex: number;  // resume from here
      status: Post;
      partitionId: number;
    }>;
  },
  // Bloom filters for efficient reply filtering
  followerBloomFilters: {
    [userId: string]: BloomFilterData;
  }
};

// Fanout with batching and continuation
async function fanoutPost(event: PostCreatedEvent, ctx) {
  const partitions = await ctx.select(['partitionedFollowersControl', event.authorId]);

  for (const partitionId of partitions) {
    await ctx.directPartition(partitionId);
    const followers = await fetchFollowersBatched(ctx, event.authorId, 0, 1000);

    // Filter: skip if boost disabled, wrong language, or reply to non-followed user
    const eligible = followers.filter(f =>
      (event.type !== 'boost' || f.showBoosts) &&
      (!event.isReply || await checkFollowsParent(f.id, event.parentAuthorId, ctx))
    );

    // Add to home timelines
    for (const follower of eligible) {
      await ctx.hashPartition(follower.id);
      await ctx.localTransform(['homeTimelines', follower.id],
        timeline => [...timeline, event.postId].slice(0, 600)
      );
    }

    // Save continuation if more followers remain
    if (followers.nextIndex) {
      await ctx.localTransform(['postIdToFollowerFanouts', event.postId],
        state => [...state, { authorId: event.authorId, nextFollowerIndex: followers.nextIndex, partitionId }]
      );
    }
  }
}
```

**Key patterns:** Partitioned recipients, bloom filters for filtering, resumable state, batched fetches

#### 2. Notification Broadcast
Deliver notifications to many users with delivery tracking:

```typescript
// Poll completion: notify author + all voters
async function notifyPollCompletion(pollId: string, authorId: string, ctx) {
  const voters = await ctx.select(['pollVotes', pollId, 'allVoters']);
  const recipients = new Set([authorId, ...Object.keys(voters)]);

  const batchSize = 20000;  // Rama's fanoutLimit
  for (const batch of chunks(Array.from(recipients), batchSize)) {
    for (const userId of batch) {
      if (await isSuppressed(ctx, userId, authorId)) continue;

      await ctx.hashPartition(userId);
      await ctx.localTransform(['userNotifications', userId],
        notifs => ({
          items: [{ type: 'poll_complete', pollId, timestamp: Date.now() }, ...notifs.items],
          unreadCount: notifs.unreadCount + 1
        })
      );

      if (batch.indexOf(userId) % 100 === 0) await ctx.yield();
    }
  }
}
```

**Key patterns:** Batch processing with rate limiting, delivery tracking, suppression checks, periodic yields

#### 3. Presence Updates
Fan out online/offline status to contacts:

```typescript
async function fanoutPresenceUpdate(event: PresenceEvent, ctx) {
  await ctx.hashPartition(event.userId);
  await ctx.localTransform(['userPresence', event.userId],
    presence => {
      if (event.status === 'offline' && event.deviceId) {
        presence.devices.delete(event.deviceId);
        return presence.devices.size > 0 ? presence : { status: 'offline', lastSeen: Date.now() };
      }
      return { status: event.status, lastSeen: Date.now() };
    }
  );

  const subscribers = await ctx.select(['presenceSubscriptions', event.userId]);
  for (const subscriberId of subscribers) {
    await ctx.hashPartition(subscriberId);
    await ctx.emit('presence-update', { userId: event.userId, status: event.status });
  }
}
```

**Key patterns:** Subscription model, multi-device tracking, real-time event emission

#### 4. Collaborative Cursor Sync
Broadcast cursor positions to document viewers with throttling:

```typescript
async function broadcastCursor(event: CursorMoveEvent, ctx) {
  await ctx.hashPartition(event.documentId);

  // Throttle: only broadcast every 50ms per user
  const shouldThrottle = await ctx.localTransform(
    ['cursorBroadcastThrottles', event.documentId, event.userId],
    throttle => {
      const elapsed = Date.now() - (throttle?.lastBroadcast ?? 0);
      return elapsed < 50
        ? { ...throttle, pendingUpdate: event }
        : { lastBroadcast: Date.now() };
    }
  );

  if (shouldThrottle?.pendingUpdate) return;

  const viewers = await ctx.select(['documentViewers', event.documentId]);
  for (const [viewerId, viewer] of viewers.entries()) {
    if (viewerId === event.userId) continue;
    await ctx.emitToConnection(viewer.connectionId, 'cursor-update', event);
  }
}
```

**Key patterns:** Throttling, no partition hopping, direct connection emission

#### 5. Mention Fanout
Deliver @mention notifications:

```typescript
async function fanoutMentions(event: MentionEvent, ctx) {
  const mentionedIds = await Promise.all(
    event.mentionedUsernames.map(u => ctx.select(['usernameToUserId', u]))
  );

  for (const userId of new Set(mentionedIds.filter(Boolean))) {
    if (userId === event.authorId) continue;

    const suppressions = await ctx.select(['userSuppressions', userId]);
    if (suppressions?.blocked.has(event.authorId)) continue;

    // Reply filter: only notify if recipient follows parent author
    if (event.isReply) {
      const follows = await ctx.select(['followerToFollowees', userId, event.parentAuthorId]);
      if (!follows) continue;
    }

    await ctx.hashPartition(userId);
    await ctx.localTransform(['userMentions', userId],
      mentions => [{ postId: event.postId, authorId: event.authorId }, ...mentions]
    );
  }
}
```

**Key patterns:** Username resolution, reply filtering, suppression checks

#### 6. Channel Message Delivery
Adaptive fanout strategy for small vs large channels:

```typescript
async function broadcastMessage(event: ChannelMessageEvent, ctx) {
  const partitionInfo = await ctx.select(['partitionedChannelMembers', event.channelId]);

  if (partitionInfo?.partitions) {
    // Large channel: partitioned fanout across workers
    for (const partitionId of partitionInfo.partitions) {
      await ctx.directPartition(partitionId);
      const members = await ctx.localSelect(['partitionedChannelMembers', event.channelId, 'members', partitionId]);

      for (const [memberId, member] of Object.entries(members)) {
        if (member.notificationPreference === 'none') continue;
        if (member.notificationPreference === 'mentions' && !isMentioned(event.content, memberId)) continue;

        await deliverMessage(ctx, memberId, event);
      }
    }
  } else {
    // Small channel: direct fanout
    const members = await ctx.select(['channelMembers', event.channelId]);
    for (const [memberId, member] of Object.entries(members)) {
      if (member.notificationPreference !== 'none') {
        await deliverMessage(ctx, memberId, event);
      }
    }
  }
}
```

**Key patterns:** Adaptive strategy, notification preferences, unread tracking

### Summary: Core Fanout Techniques

| Pattern | Purpose | Example Use Case |
|---------|---------|------------------|
| **Partitioned recipients** | Balance load across workers | Spread followers across partitions |
| **Resumable fanout** | Fault tolerance | Save `nextIndex`, resume after crash |
| **Bloom filters** | Fast membership checks | Reply filtering (is following parent?) |
| **Batching & rate limiting** | Prevent worker overload | 1k-20k recipients per batch |
| **Delivery tracking** | Observability & retry | Track delivered/pending/failed |
| **Suppression filtering** | User preferences | Check blocked/muted before delivery |
| **Throttling** | Network optimization | Cursor updates max every 50ms |

**LiveStore applicability:** Essential for social apps (feeds, notifications), collaboration tools (presence, cursors), and messaging platforms (channels, DMs) scaling to millions of users.

---

## Summary: Priority Matrix

| Feature | Impact | Complexity | LiveStore Issue |
|---------|--------|------------|-----------------|
| Aggregator primitives | High | Medium | New |
| Multi-granularity time bucketing | High | Medium | New |
| Cross-partition transactions | High | High | New |
| Idempotent migrations | Medium | Medium | Related to schema evolution |
| Task globals for IO | Medium | Low | New |
| Subindexed collections | Medium | Medium | New |
| Ack returns | Medium | Low | New |
| Partitioning strategy | High | Medium | #255 |
| Compaction generators | High | High | #136 |
| Fanout patterns | Medium | High | New (notifications/presence) |

## Quick Wins (Low complexity, Medium-High impact)
1. **Ack returns** - Immediate commit feedback without polling
2. **Task globals for IO** - Clean external service integration

## High-Value Investments
1. **Compaction generators** - Essential for eventlog size management
2. **Aggregator primitives** - Simplifies analytics materializers dramatically
3. **Partitioning strategy** - Enables horizontal scaling
