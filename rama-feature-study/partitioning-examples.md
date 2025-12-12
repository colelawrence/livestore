## 8. Declarative Partitioning Strategy - Concrete Examples

### Overview

In Rama, all depots declare partitioning strategies using `(hash-by :field)` to ensure related data lands on the same task for collocated processing. The Twitter clone demonstrates advanced partitioning with `$$partitionedFollowersControl` that spreads followers across tasks - every 1000 followers triggers assignment to a new task for balanced fanout.

**Current LiveStore limitation:** Currently assumes 1:1 mapping between eventlog and SQLite database (see [#255](https://github.com/livestorejs/livestore/issues/255)). Each `storeId` maps to a single eventlog/DB pair.

**Opportunity:** Enable multi-eventlog architectures where partition key derivation is declarative and schema-driven, improving isolation, scaling, and sync performance.

---

### 1. Workspace Partitioning (Multi-Tenant Collaboration)

**Use Case:** SaaS application where each workspace (organization/team) has isolated data. Think Slack, Notion, Linear.

**Current Approach:** One store per workspace using multi-store API:

```typescript
// Current: Manual store instantiation per workspace
const workspaceStoreOptions = (workspaceId: string) =>
  storeOptions({
    storeId: `workspace-${workspaceId}`,  // Manual partition key
    schema: workspaceSchema,
    adapter,
  })
```

**Declarative Partitioning Approach:**

```typescript
// Schema with declarative partitioning
import { Events, makeSchema, Schema, State, Partitioning } from '@livestore/livestore'

const events = {
  workspaceCreated: Events.synced({
    name: 'v1.WorkspaceCreated',
    schema: Schema.Struct({
      workspaceId: Schema.String,
      name: Schema.String,
      ownerId: Schema.String,
    }),
  }),

  taskCreated: Events.synced({
    name: 'v1.TaskCreated',
    schema: Schema.Struct({
      workspaceId: Schema.String,  // Partition key field
      taskId: Schema.String,
      title: Schema.String,
    }),
  }),

  taskUpdated: Events.synced({
    name: 'v1.TaskUpdated',
    schema: Schema.Struct({
      workspaceId: Schema.String,  // Partition key field
      taskId: Schema.String,
      completed: Schema.Boolean,
    }),
  }),
}

const tables = {
  workspace: State.SQLite.table({
    name: 'workspace',
    columns: {
      workspaceId: State.SQLite.text({ primaryKey: true }),
      name: State.SQLite.text(),
    },
  }),
  task: State.SQLite.table({
    name: 'task',
    columns: {
      taskId: State.SQLite.text({ primaryKey: true }),
      workspaceId: State.SQLite.text(),  // Foreign key to partition
      title: State.SQLite.text(),
      completed: State.SQLite.boolean({ default: false }),
    },
  }),
}

const state = State.SQLite.makeState({ tables, materializers })

// Declarative partition strategy
export const schema = makeSchema({
  events,
  state,
  // NEW: Partition configuration
  partition: {
    strategy: 'hash',
    // All events must have this field
    keyExtractor: (event) => event.args.workspaceId,
    // Eventlog naming: `${baseEventlogId}-${partitionKey}`
    eventlogIdTemplate: (baseId, partitionKey) => `${baseId}-workspace-${partitionKey}`,
  }
})
```

**How Queries Work Across Partitions:**

```typescript
// Within partition - normal queries work as expected
const tasks = store.query((db) =>
  db.select(tables.task)
    .where({ workspaceId: 'ws-abc' })
    .all()
)

// Cross-partition aggregation requires explicit multi-store query
import { MultiStoreQuery } from '@livestore/livestore'

const allUserWorkspaceTasks = MultiStoreQuery.aggregate({
  stores: userWorkspaceIds.map(wsId =>
    registry.getStore({ storeId: `app-workspace-${wsId}` })
  ),
  query: (db) => db.select(tables.task).where({ completed: false }).all(),
  combine: (results) => results.flat(),
})
```

**Migration Strategy:**

```typescript
// Re-partitioning when scaling (e.g., splitting busy workspace)
const repartitionWorkspace = async (
  oldWorkspaceId: string,
  newWorkspaceIds: string[]
) => {
  // 1. Read all events from old partition
  const events = await store.exportEventlog({ workspaceId: oldWorkspaceId })

  // 2. Transform events with new partition keys
  const repartitionedEvents = events.map(event => ({
    ...event,
    args: {
      ...event.args,
      workspaceId: assignNewWorkspace(event.args, newWorkspaceIds),
    }
  }))

  // 3. Import into new partitions (atomic per partition)
  for (const newId of newWorkspaceIds) {
    const partitionEvents = repartitionedEvents.filter(
      e => e.args.workspaceId === newId
    )
    await store.importEventlog({
      workspaceId: newId,
      events: partitionEvents
    })
  }

  // 4. Archive old partition
  await store.archivePartition({ workspaceId: oldWorkspaceId })
}
```

**Sync Performance Benefits:**

- **Isolation:** Each workspace syncs independently - user in workspace A doesn't pull workspace B events
- **Smaller eventlogs:** 10K events per workspace vs 10M events in monolithic log
- **Parallel sync:** Multiple workspaces can sync concurrently from different Durable Objects
- **Selective sync:** Only sync workspaces user has accessed recently

**Architecture with Cloudflare:**

```typescript
// Worker routes by partition key
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const workspaceId = url.searchParams.get('workspaceId')

    if (!workspaceId) {
      return new Response('workspaceId required', { status: 400 })
    }

    // Each workspace gets its own Durable Object instance
    const doId = env.SYNC_DO.idFromName(`workspace-${workspaceId}`)
    const stub = env.SYNC_DO.get(doId)

    return stub.fetch(request)
  }
}
```

---

### 2. Document Partitioning (Collaborative Editing)

**Use Case:** Google Docs-style collaborative document editor. Each document has rich edit history.

**Schema with Document-Level Partitioning:**

```typescript
const events = {
  documentCreated: Events.synced({
    name: 'v1.DocumentCreated',
    schema: Schema.Struct({
      documentId: Schema.String,  // Partition key
      title: Schema.String,
      ownerId: Schema.String,
    }),
  }),

  blockInserted: Events.synced({
    name: 'v1.BlockInserted',
    schema: Schema.Struct({
      documentId: Schema.String,  // Partition key
      blockId: Schema.String,
      content: Schema.String,
      position: Schema.Number,
    }),
  }),

  blockContentEdited: Events.synced({
    name: 'v1.BlockContentEdited',
    schema: Schema.Struct({
      documentId: Schema.String,  // Partition key
      blockId: Schema.String,
      newContent: Schema.String,
      version: Schema.Number,  // For OT/CRDT
    }),
  }),

  cursorMoved: Events.clientOnly({  // Not synced to backend
    name: 'CursorMoved',
    schema: Schema.Struct({
      documentId: Schema.String,
      userId: Schema.String,
      position: Schema.Number,
    }),
  }),
}

export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'hash',
    keyExtractor: (event) => event.args.documentId,
    eventlogIdTemplate: (baseId, docId) => `${baseId}-doc-${docId}`,
  }
})
```

**Multi-Level Partitioning (Documents within Workspaces):**

```typescript
// Composite partition key: workspace + document
export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'composite',
    keyExtractor: (event) => ({
      workspaceId: event.args.workspaceId,
      documentId: event.args.documentId,
    }),
    // Hierarchical eventlog naming
    eventlogIdTemplate: (baseId, { workspaceId, documentId }) =>
      `${baseId}-ws-${workspaceId}-doc-${documentId}`,
  }
})
```

**Sync Performance for Real-Time Collaboration:**

```typescript
// Only sync the actively edited document
const activeDocStore = useStore({
  storeId: `docs-doc-${activeDocumentId}`,
  schema: documentSchema,
  sync: {
    mode: 'realtime',  // WebSocket with live-pull
    debounce: 0,       // Immediate sync for cursors/edits
  }
})

// Background sync for recently viewed docs (lower priority)
const recentDocStores = recentDocIds.map(docId =>
  useStore({
    storeId: `docs-doc-${docId}`,
    schema: documentSchema,
    sync: {
      mode: 'polling',
      interval: 30000,  // 30 second polling
    }
  })
)
```

**Benefits:**

- **Real-time focus:** Only active document gets real-time sync (WebSocket), others poll
- **Smaller history:** 100K edits per doc vs 100M edits across all docs
- **Fast initial load:** Only download active document's eventlog
- **Offline editing:** Conflicts isolated to document boundary

---

### 3. User Data Partitioning (User Isolation)

**Use Case:** Personal data apps (finance, health, notes) where user data must be strictly isolated.

**Schema:**

```typescript
const events = {
  noteCreated: Events.synced({
    name: 'v1.NoteCreated',
    schema: Schema.Struct({
      userId: Schema.String,  // Partition key (from auth)
      noteId: Schema.String,
      title: Schema.String,
      encryptedContent: Schema.String,  // E2E encrypted
    }),
  }),

  noteUpdated: Events.synced({
    name: 'v1.NoteUpdated',
    schema: Schema.Struct({
      userId: Schema.String,
      noteId: Schema.String,
      encryptedContent: Schema.String,
    }),
  }),
}

export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'hash',
    // Extract from authenticated context
    keyExtractor: (event, context) => context.userId,
    eventlogIdTemplate: (baseId, userId) => `${baseId}-user-${userId}`,
    // Enforce partition key matches auth
    validate: (event, context) => {
      if (event.args.userId !== context.userId) {
        throw new Error('User ID mismatch - security violation')
      }
    },
  }
})
```

**Auth-Enforced Partitioning:**

```typescript
// Worker validates user can only access their partition
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const authHeader = request.headers.get('Authorization')
    const userId = await validateTokenAndGetUserId(authHeader)

    if (!userId) {
      return new Response('Unauthorized', { status: 401 })
    }

    // Extract partition key from request
    const url = new URL(request.url)
    const requestedUserId = url.searchParams.get('userId')

    // Enforce partition boundary matches auth
    if (requestedUserId !== userId) {
      return new Response('Forbidden - cross-user access denied', { status: 403 })
    }

    // Route to user-specific Durable Object
    const doId = env.SYNC_DO.idFromName(`user-${userId}`)
    const stub = env.SYNC_DO.get(doId)

    return stub.fetch(request)
  }
}
```

**Benefits:**

- **Security:** Physical isolation at DB level, no cross-user queries possible
- **Compliance:** GDPR deletion = delete user's eventlog partition
- **Performance:** User only syncs their data, not entire app's data
- **Cost:** Can archive inactive users to cold storage

---

### 4. Geographic Partitioning (Edge Deployment)

**Use Case:** Global app with region-specific data residency requirements (GDPR, data sovereignty).

**Schema:**

```typescript
const events = {
  orderPlaced: Events.synced({
    name: 'v1.OrderPlaced',
    schema: Schema.Struct({
      orderId: Schema.String,
      region: Schema.Enum(['us-east', 'eu-west', 'ap-south']),  // Partition key
      customerId: Schema.String,
      items: Schema.Array(Schema.String),
    }),
  }),
}

export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'hash',
    keyExtractor: (event) => event.args.region,
    eventlogIdTemplate: (baseId, region) => `${baseId}-region-${region}`,
  }
})
```

**Cloudflare Deployment by Region:**

```typescript
// wrangler.toml
[[durable_objects.bindings]]
name = "SYNC_DO_US"
class_name = "SyncDurableObject"
script_name = "sync-worker-us"

[[durable_objects.bindings]]
name = "SYNC_DO_EU"
class_name = "SyncDurableObject"
script_name = "sync-worker-eu"
```

```typescript
// Worker routes by region
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const region = url.searchParams.get('region')

    // Route to region-specific DO binding
    const doBinding = region === 'eu-west' ? env.SYNC_DO_EU : env.SYNC_DO_US
    const doId = doBinding.idFromName(`orders-${region}`)
    const stub = doBinding.get(doId)

    return stub.fetch(request)
  }
}
```

**Cross-Region Queries (When Permitted):**

```typescript
// Aggregate orders across regions (with permission)
const globalOrders = await MultiStoreQuery.aggregate({
  stores: ['us-east', 'eu-west', 'ap-south'].map(region =>
    registry.getStore({ storeId: `orders-region-${region}` })
  ),
  query: (db) => db.select(tables.order)
    .where('status', 'pending')
    .all(),
  combine: (results) => results.flat(),
})
```

**Benefits:**

- **Compliance:** EU data stays in EU, satisfies GDPR
- **Latency:** Data stored close to users
- **Resilience:** Regional outage doesn't affect other regions

---

### 5. Tenant Partitioning with Sub-Tenants (B2B SaaS)

**Use Case:** Enterprise SaaS where tenants can have departments/teams with hierarchical data.

**Schema with Tenant Hierarchy:**

```typescript
const events = {
  tenantCreated: Events.synced({
    name: 'v1.TenantCreated',
    schema: Schema.Struct({
      tenantId: Schema.String,  // Top-level partition
      name: Schema.String,
      tier: Schema.Enum(['free', 'pro', 'enterprise']),
    }),
  }),

  departmentCreated: Events.synced({
    name: 'v1.DepartmentCreated',
    schema: Schema.Struct({
      tenantId: Schema.String,     // Top-level partition
      departmentId: Schema.String, // Sub-partition (optional)
      name: Schema.String,
    }),
  }),

  recordCreated: Events.synced({
    name: 'v1.RecordCreated',
    schema: Schema.Struct({
      tenantId: Schema.String,
      departmentId: Schema.String,
      recordId: Schema.String,
      data: Schema.Unknown,
    }),
  }),
}

export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'composite',
    keyExtractor: (event) => ({
      tenantId: event.args.tenantId,
      // Optional sub-partition for large tenants
      departmentId: event.args.departmentId,
    }),
    eventlogIdTemplate: (baseId, { tenantId, departmentId }) =>
      departmentId
        ? `${baseId}-tenant-${tenantId}-dept-${departmentId}`
        : `${baseId}-tenant-${tenantId}`,
    // Dynamically decide partitioning based on tenant tier
    shouldSubPartition: async (tenantId, context) => {
      const tenant = await getTenant(tenantId)
      return tenant.tier === 'enterprise' && tenant.recordCount > 100000
    },
  }
})
```

**Dynamic Re-Partitioning on Tier Upgrade:**

```typescript
// When tenant upgrades from Pro to Enterprise
const onTenantUpgrade = async (tenantId: string) => {
  const tenant = await getTenant(tenantId)

  if (tenant.tier === 'enterprise' && tenant.recordCount > 100000) {
    // Trigger re-partitioning by department
    await repartitionTenantByDepartment(tenantId)
  }
}

const repartitionTenantByDepartment = async (tenantId: string) => {
  // 1. Read monolithic tenant eventlog
  const events = await store.exportEventlog({ tenantId })

  // 2. Group events by department
  const eventsByDept = groupBy(events, e => e.args.departmentId)

  // 3. Create per-department eventlogs
  for (const [deptId, deptEvents] of Object.entries(eventsByDept)) {
    await store.importEventlog({
      tenantId,
      departmentId: deptId,
      events: deptEvents,
    })
  }

  // 4. Archive old monolithic eventlog
  await store.archivePartition({ tenantId })
}
```

**Benefits:**

- **Flexible isolation:** Small tenants share infrastructure, large tenants get dedicated partitions
- **Cost optimization:** Don't over-partition small tenants
- **Scale-up:** Automatically sub-partition when tenant grows

---

### 6. Time-Based Partitioning (Archival/Analytics)

**Use Case:** High-volume event streams (analytics, logs, IoT) where old data is archived.

**Schema:**

```typescript
const events = {
  pageViewed: Events.synced({
    name: 'v1.PageViewed',
    schema: Schema.Struct({
      sessionId: Schema.String,
      timestamp: Schema.Date,
      url: Schema.String,
      userId: Schema.String.optional(),
    }),
  }),

  eventTracked: Events.synced({
    name: 'v1.EventTracked',
    schema: Schema.Struct({
      sessionId: Schema.String,
      timestamp: Schema.Date,
      eventName: Schema.String,
      properties: Schema.Unknown,
    }),
  }),
}

export const schema = makeSchema({
  events,
  state,
  partition: {
    strategy: 'time-range',
    // Partition by month
    keyExtractor: (event) => {
      const date = new Date(event.args.timestamp)
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`
    },
    eventlogIdTemplate: (baseId, monthKey) => `${baseId}-month-${monthKey}`,
    // Archive partitions older than 6 months
    archivalPolicy: {
      retentionMonths: 6,
      archiveDestination: 'r2://analytics-archive',
    },
  }
})
```

**Hot/Warm/Cold Storage Strategy:**

```typescript
// Query pattern for time-range analytics
const getPageViews = async (startDate: Date, endDate: Date) => {
  const monthKeys = getMonthsBetween(startDate, endDate)

  const stores = monthKeys.map(monthKey => {
    const age = getMonthAge(monthKey)

    return {
      storeId: `analytics-month-${monthKey}`,
      schema: analyticsSchema,
      // Hot: last 2 months - in-memory DO
      // Warm: 2-6 months - DO with disk
      // Cold: 6+ months - archived to R2
      storage: age < 2 ? 'hot' : age < 6 ? 'warm' : 'cold',
    }
  })

  return MultiStoreQuery.aggregate({
    stores,
    query: (db) => db.select(tables.pageView)
      .where('timestamp', '>=', startDate)
      .where('timestamp', '<=', endDate)
      .all(),
    combine: (results) => results.flat(),
  })
}
```

**Automatic Compaction on Archive:**

```typescript
// When archiving old partition, compact events
const archivePartition = async (monthKey: string) => {
  const events = await store.exportEventlog({ monthKey })

  // Compact events (e.g., merge session events)
  const compactedEvents = compactAnalyticsEvents(events)

  // Write to R2 in parquet format for analytics
  await writeToR2({
    bucket: 'analytics-archive',
    key: `analytics-month-${monthKey}.parquet`,
    events: compactedEvents,
  })

  // Delete local partition
  await store.deletePartition({ monthKey })
}
```

**Benefits:**

- **Cost:** Old data in cheap R2 storage vs expensive DO storage
- **Performance:** Queries only scan relevant time partitions
- **Compliance:** Automatic data retention policies
- **Analytics:** Archived data in columnar format for DuckDB/ClickHouse

---

### Summary: Partitioning Patterns

| Pattern | Partition Key | Use Case | Sync Benefit | Migration Complexity |
|---------|---------------|----------|--------------|---------------------|
| **Workspace** | `workspaceId` | Multi-tenant SaaS | Isolated sync per workspace | Medium (tenant splitting) |
| **Document** | `documentId` | Collaborative editing | Real-time for active doc only | Low (immutable docs) |
| **User** | `userId` | Personal data apps | Complete user isolation | Low (users independent) |
| **Geographic** | `region` | Data residency | Edge locality | High (regional migration) |
| **Tenant + Sub** | `tenantId` + `deptId` | Enterprise B2B SaaS | Flexible granularity | High (dynamic re-partition) |
| **Time-Based** | `YYYY-MM` | Analytics/Logs | Archive old data | Low (append-only) |

---

### Implementation Considerations

**1. Schema Validation:**
```typescript
// Ensure all events have partition key
const validatePartitionKey = (schema: LiveStoreSchema) => {
  for (const [name, eventDef] of Object.entries(schema.events)) {
    if (eventDef.clientOnly) continue  // Client-only events exempt

    const hasPartitionKey = schema.partition.keyExtractor(
      { name, args: {} } as any
    )

    if (!hasPartitionKey) {
      throw new Error(
        `Event ${name} missing partition key field ${schema.partition.keyField}`
      )
    }
  }
}
```

**2. Cross-Partition Transactions:**
```typescript
// For now: Disallow cross-partition transactions
// Future: Use saga pattern or 2PC
const transferBetweenWorkspaces = async (
  fromWorkspace: string,
  toWorkspace: string,
  taskId: string
) => {
  // This would require distributed transaction
  throw new Error(
    'Cross-partition transactions not yet supported. ' +
    'Use application-level saga pattern or create compensating events.'
  )
}
```

**3. Partition Discovery:**
```typescript
// How clients discover which partitions to sync
interface PartitionRegistry {
  // Get all partitions user has access to
  getUserPartitions(userId: string): Promise<string[]>

  // Subscribe to partition changes
  onPartitionAdded(callback: (partitionId: string) => void): void
}

// Example: User's workspace list drives partition sync
const userStore = useStore({ storeId: `user-${userId}`, schema: userSchema })
const workspaceIds = userStore.query(db =>
  db.select(tables.workspace).all().map(w => w.workspaceId)
)

// Dynamically sync all user's workspaces
const workspaceStores = workspaceIds.map(wsId =>
  useStore({ storeId: `app-workspace-${wsId}`, schema: workspaceSchema })
)
```

**4. Monitoring & Observability:**
```typescript
// Track partition health
interface PartitionMetrics {
  partitionKey: string
  eventCount: number
  sizeBytes: number
  lastSyncTimestamp: Date
  activeConnections: number
}

// Alert when partition grows too large
const PARTITION_SIZE_LIMIT = 100_000_000  // 100MB
const onPartitionMetrics = (metrics: PartitionMetrics) => {
  if (metrics.sizeBytes > PARTITION_SIZE_LIMIT) {
    alertOps({
      message: `Partition ${metrics.partitionKey} exceeds size limit`,
      action: 'Consider sub-partitioning or archival',
    })
  }
}
```

---

### Next Steps for LiveStore

1. **Schema API:** Add `partition` configuration to `makeSchema()`
2. **Multi-eventlog support:** Core changes to support multiple eventlogs per store
3. **Partition-aware sync:** Modify sync protocol to handle partition routing
4. **Migration tooling:** Built-in tools for re-partitioning eventlogs
5. **Cross-partition queries:** Higher-level API for aggregating across partitions
6. **Cloudflare DO integration:** Automatic DO routing by partition key
