## 9. Compaction Generators - Concrete Examples

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

---

### Pattern 1: Purchase Event Summarization (E-Commerce Analytics)

**Problem:** An e-commerce app generates thousands of purchase events daily. Over months, the eventlog grows to hundreds of MB, slowing sync and rematerialization.

**Compaction Strategy:** Daily rollup of individual purchase events into daily summary events.

**Trigger:** Time-based (daily at 00:00 UTC)

**Input Events (to be compacted):**
```typescript
// Throughout the day, many individual purchase events
const purchaseEvents = [
  events.purchaseCompleted({
    id: 'pur_001',
    userId: 'usr_123',
    amount: 4999, // $49.99 in cents
    timestamp: new Date('2025-12-12T10:30:00Z'),
    items: ['item_a', 'item_b'],
  }),
  events.purchaseCompleted({
    id: 'pur_002',
    userId: 'usr_123',
    amount: 2999,
    timestamp: new Date('2025-12-12T14:45:00Z'),
    items: ['item_c'],
  }),
  events.purchaseCompleted({
    id: 'pur_003',
    userId: 'usr_456',
    amount: 15999,
    timestamp: new Date('2025-12-12T18:20:00Z'),
    items: ['item_d', 'item_e', 'item_f'],
  }),
  // ... hundreds more throughout the day
]
```

**Compaction Generator (TypeScript/LiveStore-style):**
```typescript
/**
 * Daily purchase compaction generator
 * Runs daily to compact previous day's purchase events into summary events
 */
const dailyPurchaseCompaction = Compaction.generator({
  name: 'daily-purchase-rollup',

  // Trigger: Run daily at 00:00 UTC
  schedule: Compaction.cron('0 0 * * *'),

  // Select events to compact: purchases from previous day
  selectEvents: ({ eventlog, date }) =>
    eventlog
      .events('purchase.completed')
      .where((e) => {
        const eventDate = new Date(e.timestamp)
        const yesterday = new Date(date)
        yesterday.setDate(yesterday.getDate() - 1)
        return eventDate.toDateString() === yesterday.toDateString()
      }),

  // Aggregation logic (inspired by Rama's batch<- pattern)
  aggregate: ({ events }) => {
    const summaryByUser = new Map<string, {
      userId: string
      totalAmount: number
      purchaseCount: number
      itemsPurchased: number
      firstPurchaseTime: Date
      lastPurchaseTime: Date
    }>()

    for (const event of events) {
      const existing = summaryByUser.get(event.userId) ?? {
        userId: event.userId,
        totalAmount: 0,
        purchaseCount: 0,
        itemsPurchased: 0,
        firstPurchaseTime: event.timestamp,
        lastPurchaseTime: event.timestamp,
      }

      summaryByUser.set(event.userId, {
        ...existing,
        totalAmount: existing.totalAmount + event.amount,
        purchaseCount: existing.purchaseCount + 1,
        itemsPurchased: existing.itemsPurchased + event.items.length,
        lastPurchaseTime: new Date(
          Math.max(existing.lastPurchaseTime.getTime(), event.timestamp.getTime())
        ),
        firstPurchaseTime: new Date(
          Math.min(existing.firstPurchaseTime.getTime(), event.timestamp.getTime())
        ),
      })
    }

    return Array.from(summaryByUser.values())
  },

  // Emit summary events
  emit: ({ summaries, date }) =>
    summaries.map((summary) =>
      events.dailyPurchaseSummary({
        date,
        userId: summary.userId,
        totalAmount: summary.totalAmount,
        purchaseCount: summary.purchaseCount,
        itemsPurchased: summary.itemsPurchased,
        firstPurchaseTime: summary.firstPurchaseTime,
        lastPurchaseTime: summary.lastPurchaseTime,
      })
    ),

  // Remove original events after successful compaction
  pruneStrategy: Compaction.prune.deleteOriginals(),
})
```

**Output Summary Event:**
```typescript
// Single summary event per user per day
const summaryEvent = events.dailyPurchaseSummary({
  date: new Date('2025-12-12'),
  userId: 'usr_123',
  totalAmount: 7998, // Sum of $49.99 + $29.99
  purchaseCount: 2,
  itemsPurchased: 3,
  firstPurchaseTime: new Date('2025-12-12T10:30:00Z'),
  lastPurchaseTime: new Date('2025-12-12T14:45:00Z'),
})
```

**Size Reduction:**
- Before: 1000 purchase events/day × 200 bytes ≈ 200 KB/day
- After: 50 users × 150 bytes ≈ 7.5 KB/day
- **Savings: ~96% reduction** for analytics use cases

**Audit Trail:**
- Full audit mode: Keep all original events in cold storage, emit summaries as additional events
- Summary mode: Replace with summaries after 30-day retention window
- Compliance: Original purchase IDs are lost but daily totals are preserved

---

### Pattern 2: Activity Log Compaction (User Analytics)

**Problem:** User activity tracking generates millions of micro-events (clicks, views, scrolls). After 7 days, granular data is less valuable but totals are needed for retention metrics.

**Compaction Strategy:** Hourly activity digests for events older than 7 days.

**Trigger:** Count-based (after every 10,000 activity events) or time-based (hourly)

**Input Events:**
```typescript
// High-frequency micro-events
const activityEvents = [
  events.pageViewed({ userId: 'usr_123', page: '/dashboard', timestamp: new Date('2025-12-01T10:00:15Z') }),
  events.buttonClicked({ userId: 'usr_123', button: 'new-issue', timestamp: new Date('2025-12-01T10:00:17Z') }),
  events.issueViewed({ userId: 'usr_123', issueId: 42, timestamp: new Date('2025-12-01T10:00:20Z') }),
  // ... thousands more per user per hour
]
```

**Compaction Generator:**
```typescript
const hourlyActivityCompaction = Compaction.generator({
  name: 'hourly-activity-digest',

  // Trigger: After 10k events OR hourly
  trigger: Compaction.trigger.either([
    Compaction.trigger.count({ eventNames: ['page.viewed', 'button.clicked', 'issue.viewed'], threshold: 10000 }),
    Compaction.trigger.schedule('0 * * * *'), // Every hour
  ]),

  // Only compact events older than 7 days
  selectEvents: ({ eventlog, now }) => {
    const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)
    return eventlog
      .events(['page.viewed', 'button.clicked', 'issue.viewed'])
      .where((e) => e.timestamp < sevenDaysAgo)
  },

  aggregate: ({ events }) => {
    // Group by user and hour bucket
    const hourlyDigests = new Map<string, {
      userId: string
      hourBucket: Date
      pageViews: number
      buttonClicks: number
      issueViews: number
      uniquePagesViewed: Set<string>
      uniqueIssuesViewed: Set<number>
    }>()

    for (const event of events) {
      // Truncate to hour bucket
      const hourBucket = new Date(event.timestamp)
      hourBucket.setMinutes(0, 0, 0)

      const key = `${event.userId}:${hourBucket.toISOString()}`
      const existing = hourlyDigests.get(key) ?? {
        userId: event.userId,
        hourBucket,
        pageViews: 0,
        buttonClicks: 0,
        issueViews: 0,
        uniquePagesViewed: new Set(),
        uniqueIssuesViewed: new Set(),
      }

      if (event.name === 'page.viewed') {
        existing.pageViews++
        existing.uniquePagesViewed.add(event.page)
      } else if (event.name === 'button.clicked') {
        existing.buttonClicks++
      } else if (event.name === 'issue.viewed') {
        existing.issueViews++
        existing.uniqueIssuesViewed.add(event.issueId)
      }

      hourlyDigests.set(key, existing)
    }

    return Array.from(hourlyDigests.values()).map((d) => ({
      ...d,
      uniquePagesViewed: Array.from(d.uniquePagesViewed),
      uniqueIssuesViewed: Array.from(d.uniqueIssuesViewed),
    }))
  },

  emit: ({ summaries }) =>
    summaries.map((summary) =>
      events.hourlyActivityDigest({
        userId: summary.userId,
        hourBucket: summary.hourBucket,
        pageViews: summary.pageViews,
        buttonClicks: summary.buttonClicks,
        issueViews: summary.issueViews,
        uniquePages: summary.uniquePagesViewed.length,
        uniqueIssues: summary.uniqueIssuesViewed.length,
      })
    ),

  pruneStrategy: Compaction.prune.deleteOriginals(),
})
```

**Output:**
```typescript
// One digest event per user per hour
events.hourlyActivityDigest({
  userId: 'usr_123',
  hourBucket: new Date('2025-12-01T10:00:00Z'),
  pageViews: 47,
  buttonClicks: 23,
  issueViews: 12,
  uniquePages: 8,
  uniqueIssues: 5,
})
```

**Size Reduction:**
- Before: 1M activity events × 100 bytes = 100 MB
- After: ~10k hourly digests × 120 bytes = 1.2 MB
- **Savings: ~99% reduction**

**Audit Trail:** Granular click-stream lost after 7 days, hourly patterns preserved indefinitely

---

### Pattern 3: Metric Rollup (Time-Series Data)

**Problem:** IoT sensors or performance monitors emit high-frequency metrics. Minute-level precision needed for recent data, but hourly/daily summaries sufficient for historical analysis.

**Compaction Strategy:** Multi-tier time bucketing (like Rama's time_series_module)

**Input Events:**
```typescript
// Minute-level metric events
const metricEvents = [
  events.performanceMetric({
    metric: 'api.latency',
    value: 45, // ms
    timestamp: new Date('2025-12-12T10:00:00Z'),
    tags: { endpoint: '/api/issues', region: 'us-east' },
  }),
  events.performanceMetric({
    metric: 'api.latency',
    value: 52,
    timestamp: new Date('2025-12-12T10:01:00Z'),
    tags: { endpoint: '/api/issues', region: 'us-east' },
  }),
  // ... one per minute
]
```

**Compaction Generator (Multi-Tier):**
```typescript
// Tier 1: Keep minute-level for last 24 hours
// Tier 2: Hourly summaries for last 30 days
// Tier 3: Daily summaries forever

const metricRollupCompaction = Compaction.generator({
  name: 'multi-tier-metric-rollup',

  schedule: Compaction.cron('0 * * * *'), // Hourly

  tiers: [
    {
      name: 'minute-to-hour',
      retentionWindow: '24h',
      granularity: 'hour',
      selectEvents: ({ eventlog, now }) => {
        const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000)
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)

        return eventlog
          .events('performance.metric')
          .where((e) => e.timestamp < oneDayAgo && e.timestamp >= thirtyDaysAgo)
      },
      aggregate: ({ events }) => rollupToHourly(events),
      emit: ({ summaries }) =>
        summaries.map((s) =>
          events.hourlyMetricSummary({
            metric: s.metric,
            hourBucket: s.bucket,
            count: s.count,
            sum: s.sum,
            avg: s.avg,
            min: s.min,
            max: s.max,
            p50: s.p50,
            p95: s.p95,
            p99: s.p99,
            tags: s.tags,
          })
        ),
    },
    {
      name: 'hour-to-day',
      retentionWindow: '30d',
      granularity: 'day',
      selectEvents: ({ eventlog, now }) => {
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)

        return eventlog
          .events('hourly.metric.summary')
          .where((e) => e.hourBucket < thirtyDaysAgo)
      },
      aggregate: ({ events }) => rollupToDaily(events),
      emit: ({ summaries }) =>
        summaries.map((s) =>
          events.dailyMetricSummary({
            metric: s.metric,
            date: s.bucket,
            count: s.count,
            avg: s.avg,
            min: s.min,
            max: s.max,
            tags: s.tags,
          })
        ),
    },
  ],

  pruneStrategy: Compaction.prune.deleteOriginals(),
})

function rollupToHourly(events: Array<{ value: number; timestamp: Date; tags: any }>) {
  const buckets = new Map()

  for (const event of events) {
    const hourBucket = new Date(event.timestamp)
    hourBucket.setMinutes(0, 0, 0)
    const key = `${hourBucket.toISOString()}:${JSON.stringify(event.tags)}`

    if (!buckets.has(key)) {
      buckets.set(key, { values: [], bucket: hourBucket, tags: event.tags })
    }
    buckets.get(key).values.push(event.value)
  }

  return Array.from(buckets.values()).map((b) => ({
    bucket: b.bucket,
    tags: b.tags,
    count: b.values.length,
    sum: b.values.reduce((a, v) => a + v, 0),
    avg: b.values.reduce((a, v) => a + v, 0) / b.values.length,
    min: Math.min(...b.values),
    max: Math.max(...b.values),
    p50: percentile(b.values, 0.5),
    p95: percentile(b.values, 0.95),
    p99: percentile(b.values, 0.99),
  }))
}
```

**Size Reduction:**
- Minute-level: 1440 events/day × 150 bytes = 216 KB/day
- Hourly: 24 events/day × 200 bytes = 4.8 KB/day
- Daily: 1 event/day × 150 bytes = 150 bytes/day
- **Savings: 98% after 30 days**

**Audit Trail:** Statistical summaries preserved, individual data points lost per retention tier

---

### Pattern 4: State Snapshot Compaction (Document Editing)

**Problem:** Document collaboration generates incremental edit events. After 100+ edits, replaying from scratch is slow. Snapshots provide fast rematerialization.

**Compaction Strategy:** Periodic snapshot events replace long chains of incremental edits.

**Input Events:**
```typescript
// Many incremental document edit events
const editEvents = [
  events.documentCreated({ docId: 'doc_1', content: '', timestamp: new Date('2025-12-01T10:00:00Z') }),
  events.textInserted({ docId: 'doc_1', position: 0, text: 'Hello', timestamp: new Date('2025-12-01T10:00:05Z') }),
  events.textInserted({ docId: 'doc_1', position: 5, text: ' world', timestamp: new Date('2025-12-01T10:00:10Z') }),
  events.textDeleted({ docId: 'doc_1', position: 0, length: 5, timestamp: new Date('2025-12-01T10:00:15Z') }),
  // ... 100+ more edit events
]
```

**Compaction Generator:**
```typescript
const documentSnapshotCompaction = Compaction.generator({
  name: 'document-snapshot',

  // Trigger: After every 100 edit events per document
  trigger: Compaction.trigger.perEntity({
    entityKey: 'docId',
    eventNames: ['text.inserted', 'text.deleted', 'text.replaced'],
    threshold: 100,
  }),

  selectEvents: ({ eventlog, entityId }) =>
    eventlog
      .events(['document.created', 'text.inserted', 'text.deleted', 'text.replaced'])
      .where((e) => e.docId === entityId)
      .orderBy('timestamp', 'asc'),

  aggregate: ({ events }) => {
    // Replay all events to compute current document state
    let currentState = {
      docId: events[0].docId,
      content: '',
      metadata: {},
    }

    for (const event of events) {
      switch (event.name) {
        case 'document.created':
          currentState.content = event.content
          currentState.metadata = event.metadata
          break
        case 'text.inserted':
          currentState.content =
            currentState.content.slice(0, event.position) +
            event.text +
            currentState.content.slice(event.position)
          break
        case 'text.deleted':
          currentState.content =
            currentState.content.slice(0, event.position) +
            currentState.content.slice(event.position + event.length)
          break
        case 'text.replaced':
          currentState.content =
            currentState.content.slice(0, event.position) +
            event.newText +
            currentState.content.slice(event.position + event.oldText.length)
          break
      }
    }

    return [currentState]
  },

  emit: ({ summaries, originalEvents }) => [
    events.documentSnapshot({
      docId: summaries[0].docId,
      content: summaries[0].content,
      metadata: summaries[0].metadata,
      snapshotVersion: originalEvents.length,
      timestamp: new Date(),
    }),
  ],

  // Keep snapshots, delete original edit chain
  pruneStrategy: Compaction.prune.deleteOriginals(),
})
```

**Output:**
```typescript
// Single snapshot replaces 100+ edit events
events.documentSnapshot({
  docId: 'doc_1',
  content: 'Final document content after all edits...',
  metadata: { author: 'usr_123', lastModified: new Date() },
  snapshotVersion: 142, // Number of events compacted
  timestamp: new Date('2025-12-01T11:30:00Z'),
})
```

**Size Reduction:**
- Before: 142 edit events × 120 bytes = 17 KB per document
- After: 1 snapshot × (content size + 200 bytes overhead)
- **Savings: ~90% for typical documents**

**Audit Trail:**
- **Lost:** Individual edit operations, edit timestamps, who made which edit
- **Preserved:** Final document state, total number of edits
- **Hybrid approach:** Keep last 100 edits in detail, snapshot older ones

---

### Pattern 5: CRUD Collapse (Entity Lifecycle)

**Problem:** Entities go through many state changes. After deletion or significant time, intermediate states are irrelevant for most queries.

**Compaction Strategy:** Collapse create + N updates into single "final state" event.

**Input Events:**
```typescript
// Issue lifecycle: created, then many updates, eventually closed
const issueEvents = [
  events.createIssue({
    id: 42,
    title: 'Initial title',
    status: 'open',
    priority: 'medium',
    created: new Date('2025-11-01T10:00:00Z'),
  }),
  events.updateIssueTitle({ id: 42, title: 'Updated title', modified: new Date('2025-11-01T14:00:00Z') }),
  events.updateIssuePriority({ id: 42, priority: 'high', modified: new Date('2025-11-02T09:00:00Z') }),
  events.updateIssueStatus({ id: 42, status: 'in-progress', modified: new Date('2025-11-03T11:00:00Z') }),
  events.updateIssueTitle({ id: 42, title: 'Final title', modified: new Date('2025-11-05T16:00:00Z') }),
  events.updateIssueStatus({ id: 42, status: 'closed', modified: new Date('2025-11-10T10:00:00Z') }),
]
```

**Compaction Generator:**
```typescript
const entityLifecycleCompaction = Compaction.generator({
  name: 'issue-lifecycle-collapse',

  // Trigger: 30 days after issue is closed
  trigger: Compaction.trigger.afterEntityEvent({
    entityKey: 'id',
    terminatingEvent: 'issue.status.updated',
    terminatingCondition: (e) => e.status === 'closed',
    delayAfterTermination: '30d',
  }),

  selectEvents: ({ eventlog, entityId }) =>
    eventlog
      .events(['issue.created', 'issue.title.updated', 'issue.priority.updated', 'issue.status.updated'])
      .where((e) => e.id === entityId)
      .orderBy('timestamp', 'asc'),

  aggregate: ({ events }) => {
    // Fold all events into final entity state
    const finalState = { ...events[0] } // Start with creation event

    for (const event of events.slice(1)) {
      // Apply each update
      if (event.title !== undefined) finalState.title = event.title
      if (event.priority !== undefined) finalState.priority = event.priority
      if (event.status !== undefined) finalState.status = event.status
      if (event.modified) finalState.modified = event.modified
    }

    return [{
      ...finalState,
      eventCount: events.length,
    }]
  },

  emit: ({ summaries }) => [
    events.issueFinalState({
      id: summaries[0].id,
      title: summaries[0].title,
      status: summaries[0].status,
      priority: summaries[0].priority,
      created: summaries[0].created,
      closed: summaries[0].modified,
      changeCount: summaries[0].eventCount - 1,
    }),
  ],

  pruneStrategy: Compaction.prune.deleteOriginals(),
})
```

**Output:**
```typescript
// Single collapsed event replaces 6 lifecycle events
events.issueFinalState({
  id: 42,
  title: 'Final title',
  status: 'closed',
  priority: 'high',
  created: new Date('2025-11-01T10:00:00Z'),
  closed: new Date('2025-11-10T10:00:00Z'),
  changeCount: 5,
})
```

**Size Reduction:**
- Before: 6 events × 150 bytes = 900 bytes per issue
- After: 1 event × 180 bytes = 180 bytes
- **Savings: ~80% per closed issue**
- At scale: 10k closed issues = 7.2 MB saved

**Audit Trail:**
- **Lost:** Intermediate state changes, who made each change, timeline of priority shifts
- **Preserved:** Creation time, final state, total change count
- **Use case:** Sufficient for closed/archived issues where history is rarely needed

---

### Pattern 6: Retention-Based Tiered Compaction (Compliance)

**Problem:** GDPR/compliance requires audit trails but storage is expensive. Need detailed events for 30 days, summaries for 7 years, then deletion.

**Compaction Strategy:** Multi-tier retention with progressive summarization.

**Compaction Generator:**
```typescript
const complianceRetentionCompaction = Compaction.generator({
  name: 'gdpr-compliant-retention',

  schedule: Compaction.cron('0 2 * * *'), // Daily at 2 AM

  tiers: [
    {
      name: 'hot-tier-full-detail',
      retentionWindow: '30d',
      action: Compaction.action.keep(), // Keep all events unmodified
    },
    {
      name: 'warm-tier-daily-summaries',
      retentionWindow: '7y',
      selectEvents: ({ eventlog, now }) => {
        const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)
        const sevenYearsAgo = new Date(now.getTime() - 7 * 365 * 24 * 60 * 60 * 1000)

        return eventlog
          .events('*') // All events
          .where((e) => e.timestamp < thirtyDaysAgo && e.timestamp >= sevenYearsAgo)
      },

      aggregate: ({ events }) => {
        // Group by date and event type
        const dailySummaries = new Map()

        for (const event of events) {
          const dateKey = event.timestamp.toISOString().slice(0, 10)
          const key = `${dateKey}:${event.name}`

          if (!dailySummaries.has(key)) {
            dailySummaries.set(key, {
              date: new Date(dateKey),
              eventType: event.name,
              count: 0,
              users: new Set(),
              // Extract PII for anonymization
              containsPII: hasPII(event),
            })
          }

          const summary = dailySummaries.get(key)
          summary.count++
          if (event.userId) summary.users.add(event.userId)
        }

        return Array.from(dailySummaries.values()).map((s) => ({
          ...s,
          uniqueUsers: s.users.size,
          users: undefined, // Remove PII
        }))
      },

      emit: ({ summaries }) =>
        summaries.map((s) =>
          events.dailyEventSummary({
            date: s.date,
            eventType: s.eventType,
            count: s.count,
            uniqueUsers: s.uniqueUsers,
          })
        ),

      pruneStrategy: Compaction.prune.deleteOriginals(),
    },
    {
      name: 'cold-tier-deletion',
      retentionWindow: '0d', // Delete after 7 years
      selectEvents: ({ eventlog, now }) => {
        const sevenYearsAgo = new Date(now.getTime() - 7 * 365 * 24 * 60 * 60 * 1000)

        return eventlog
          .events('*')
          .where((e) => e.timestamp < sevenYearsAgo)
      },

      // Optional: Emit deletion audit event before purging
      emit: ({ originalEvents }) => [
        events.dataRetentionPurge({
          eventCount: originalEvents.length,
          oldestEvent: Math.min(...originalEvents.map((e) => e.timestamp.getTime())),
          newestEvent: Math.max(...originalEvents.map((e) => e.timestamp.getTime())),
          purgedAt: new Date(),
        }),
      ],

      pruneStrategy: Compaction.prune.deleteAll(),
    },
  ],

  // GDPR right-to-erasure hook
  onUserDataDeletion: ({ userId, eventlog }) => {
    // Find all events for user and tombstone them
    const userEvents = eventlog.events('*').where((e) => e.userId === userId)

    return {
      pruneEvents: userEvents,
      emitEvents: [
        events.userDataErased({
          userId: hashUserId(userId), // Pseudonymized ID for audit
          eventCount: userEvents.length,
          erasedAt: new Date(),
        }),
      ],
    }
  },
})

function hasPII(event: any): boolean {
  // Check if event contains PII fields
  const piiFields = ['email', 'phone', 'address', 'ssn', 'name']
  return piiFields.some((field) => event[field] !== undefined)
}

function hashUserId(userId: string): string {
  // One-way hash for audit trail without PII
  return crypto.subtle.digest('SHA-256', new TextEncoder().encode(userId))
    .then((hash) => Array.from(new Uint8Array(hash)))
    .then((bytes) => bytes.map((b) => b.toString(16).padStart(2, '0')).join(''))
}
```

**Size Reduction Timeline:**
- Days 0-30: 100 MB (full detail)
- Days 31-2555 (7 years): 5 MB (daily summaries, ~95% reduction)
- After 7 years: Deleted (100% reduction)

**Audit Trail:**
- Days 0-30: Full audit trail with all details
- Days 31-2555: Aggregate statistics, event counts, no PII
- After 7 years: Deletion audit event only (when purge occurred)

**Compliance:**
- ✅ Meets typical data retention requirements (7 years for financial records)
- ✅ GDPR right-to-erasure via `onUserDataDeletion` hook
- ✅ Automatic PII anonymization in warm tier
- ✅ Audit trail of deletions via `dataRetentionPurge` events

---

## Implementation Considerations

### 1. Compaction Safety
```typescript
// Compaction should be transactional
const compactionTxn = {
  // 1. Generate summary events
  summaries: await generator.aggregate({ events: originalEvents }),

  // 2. Emit summaries to eventlog
  await eventlog.append(summaries),

  // 3. Mark original events as compacted (soft delete)
  await eventlog.markCompacted(originalEvents.map((e) => e.sequenceNumber)),

  // 4. Verify materializers still produce correct state
  await verifyMaterializerIntegrity(),

  // 5. Hard delete original events (optional, after verification period)
  await eventlog.pruneCompacted({ olderThan: '7d' }),
}
```

### 2. Rematerialization After Compaction
```typescript
// Materializers must handle both original and summary events
const materializers = State.SQLite.materializers(events, {
  'purchase.completed': (data) =>
    tables.purchases.insert(data),

  'daily.purchase.summary': (data) =>
    tables.dailyPurchaseSummaries.insert(data),

  // Query layer must union both sources
  'query.totalPurchases': ({ userId }) => `
    SELECT
      COALESCE(SUM(amount), 0) +
      COALESCE((SELECT SUM(totalAmount) FROM dailyPurchaseSummaries WHERE userId = ?), 0) as total
    FROM purchases
    WHERE userId = ?
  `,
})
```

### 3. Incremental Compaction
```typescript
// Don't compact entire eventlog at once - process in batches
const incrementalCompaction = {
  batchSize: 10000, // Events per compaction run
  maxDuration: '5m', // Stop after 5 minutes even if incomplete
  resumable: true,   // Track progress and resume later
}
```

### 4. Audit Trail Modes
```typescript
enum CompactionMode {
  // Keep all original events, emit summaries as additional events
  PRESERVE_ALL = 'preserve-all',

  // Move original events to cold storage, keep summaries in hot eventlog
  ARCHIVE_ORIGINALS = 'archive-originals',

  // Delete original events after retention window
  REPLACE_WITH_SUMMARIES = 'replace-with-summaries',

  // Keep original events immutable, summaries are derived views
  SUMMARIES_AS_VIEWS = 'summaries-as-views',
}
```

### 5. Monitoring & Observability
```typescript
// Emit metrics for each compaction run
const compactionMetrics = {
  eventsProcessed: 10000,
  eventsEmitted: 150,
  spaceReclaimed: '95 MB',
  duration: '3.2s',
  oldestEventCompacted: new Date('2025-11-01'),
  newestEventCompacted: new Date('2025-11-30'),
}
```

---

## Real-World Use Cases Summary

| Use Case | Trigger | Input Volume | Output Volume | Savings | Audit Impact |
|----------|---------|--------------|---------------|---------|--------------|
| **E-commerce purchases** | Daily | 1000 events/day | 50 events/day | 95% | Daily totals, no individual receipts |
| **User activity logs** | Hourly, 7d window | 1M events/week | 10k events/week | 99% | Hourly patterns, no click-stream |
| **Performance metrics** | Multi-tier | 1.5M events/month | 15k events/month | 99% | Statistical summaries only |
| **Document edits** | Per 100 edits | 142 edits/doc | 1 snapshot/doc | 90% | Final state, no edit history |
| **Entity lifecycle** | 30d after close | 6 events/entity | 1 event/entity | 80% | Final state, change count |
| **GDPR compliance** | Daily, multi-tier | 100 MB/month | 5 MB/month (warm) | 95% | Aggregates after 30d |

---

## Sources & Further Reading

Key research and best practices for event log compaction and audit compliance:

- [Kafka, GDPR and Event Sourcing](https://danlebrero.com/2018/04/11/kafka-gdpr-event-sourcing/) - Log compaction strategies for GDPR compliance
- [How to deal with privacy and GDPR in Event-Driven systems](https://event-driven.io/en/gdpr_in_event_driven_architecture/) - Event sourcing patterns for right-to-erasure
- [Security log retention: Best practices and compliance guide](https://auditboard.com/blog/security-log-retention-best-practices-guide) - Retention policies and tiering strategies
- [What Is An Audit Trail? A Complete Guide in 2025](https://www.spendflo.com/blog/audit-trail-complete-guide) - Audit trail requirements and implementation
- [GDPR Audit: Complete Compliance Audit Guide for 2025](https://complydog.com/blog/gdpr-audit-complete-compliance-audit-guide-2025) - GDPR-compliant audit practices
