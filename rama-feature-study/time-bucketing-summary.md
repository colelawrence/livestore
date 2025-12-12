### Concrete Examples

> See complete code examples in `time-bucketing-examples.md` for full implementations.

Multi-granularity time bucketing enables efficient time-series analytics by storing aggregated statistics at multiple time scales (minute/hour/day/month). This dramatically reduces query costs: querying 1 year of data reads ~12 month buckets instead of 525,600 minute buckets.

#### 1. API Latency Tracking

Track request latencies at minute/hour/day/month for performance monitoring dashboards.

**Schema Pattern:**
```typescript
export const tables = {
  latencyStats: State.SQLite.table({
    name: 'latencyStats',
    columns: {
      endpoint: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'm' | 'h' | 'd' | 'mo'
      bucket: State.SQLite.integer(),   // bucket number for granularity
      count: State.SQLite.integer({ default: 0 }),
      totalMs: State.SQLite.integer({ default: 0 }),
      minMs: State.SQLite.integer({ nullable: true }),
      maxMs: State.SQLite.integer({ nullable: true }),
    },
    primaryKey: ['endpoint', 'granularity', 'bucket'],
  }),
}
```

**Bucket Computation (matches Rama's `emit-index-granularities`):**
```typescript
export function computeTimeBuckets(timestampMs: number) {
  const minuteBucket = Math.floor(timestampMs / (1000 * 60))
  const hourBucket = Math.floor(minuteBucket / 60)
  const dayBucket = Math.floor(hourBucket / 24)
  const monthBucket = Math.floor(dayBucket / 30)

  return { m: minuteBucket, h: hourBucket, d: dayBucket, mo: monthBucket }
}
```

**Materializer (updates all granularities atomically):**
```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.ApiRequestCompleted': ({ endpoint, latencyMs, timestampMs }) => {
    const buckets = computeTimeBuckets(timestampMs)
    const stat = { count: 1, totalMs: latencyMs, minMs: latencyMs, maxMs: latencyMs }

    // Single write updates all 4 granularities
    return [
      upsertLatencyStat(endpoint, 'm', buckets.m, stat),
      upsertLatencyStat(endpoint, 'h', buckets.h, stat),
      upsertLatencyStat(endpoint, 'd', buckets.d, stat),
      upsertLatencyStat(endpoint, 'mo', buckets.mo, stat),
    ]
  },
})

function upsertLatencyStat(endpoint: string, granularity: string, bucket: number, stat: LatencyStats) {
  return tables.latencyStats.insert({ endpoint, granularity, bucket, ...stat })
    .onConflict(['endpoint', 'granularity', 'bucket'], (existing) => ({
      count: existing.count + stat.count,
      totalMs: existing.totalMs + stat.totalMs,
      minMs: Math.min(existing.minMs ?? Infinity, stat.minMs),
      maxMs: Math.max(existing.maxMs ?? 0, stat.maxMs),
    }))
}
```

**Optimal Query Planning (matches Rama's `query-granularities`):**
```typescript
// Query from minute 58 of hour 6 to minute 3 of hour 20 becomes:
// - Fetch minutes 58-59 at granularity 'm'
// - Fetch hours 7-19 at granularity 'h' (13 buckets instead of 780 minutes!)
// - Fetch minutes 0-3 of hour 20 at granularity 'm'

export function computeOptimalQueryRanges(
  startMinuteBucket: number,
  endMinuteBucket: number
): QueryRange[] {
  // Recursive function determines which granularities minimize query count
  // Full implementation in time-bucketing-examples.md
}

export async function getLatencyStatsForRange(
  store: LiveStore,
  endpoint: string,
  startMs: number,
  endMs: number
): Promise<LatencyStats> {
  const ranges = computeOptimalQueryRanges(
    Math.floor(startMs / (1000 * 60)),
    Math.floor(endMs / (1000 * 60))
  )

  let aggregated: LatencyStats | null = null

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint = ${endpoint} AND granularity = ${range.granularity}
                   AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      aggregated = combineStats(aggregated, stat)
    }
  }

  return aggregated
}
```

**Query Efficiency:**
- 1 hour range: 60 minute buckets (1.0x efficiency)
- 1 day range: 48 buckets (30x efficiency - uses hour buckets for middle)
- 30 day range: 60 buckets (720x efficiency - uses day/month buckets)

---

#### 2. User Activity Metrics (DAU/WAU/MAU)

Track daily/weekly/monthly active users with automatic rollup.

**Key Pattern:** Store unique user IDs per bucket (use HyperLogLog or Bloom filter at scale).

```typescript
export const tables = {
  activityBuckets: State.SQLite.table({
    name: 'activityBuckets',
    columns: {
      granularity: State.SQLite.text(), // 'd' | 'w' | 'm'
      bucket: State.SQLite.integer(),
      activeUserIds: State.SQLite.text({ default: '' }), // CSV or JSON array
      uniqueUserCount: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['granularity', 'bucket'],
  }),
}

// Materializer merges user ID sets across buckets
const materializers = State.SQLite.materializers(events, {
  'v1.UserActiveEvent': ({ userId, timestampMs }) => {
    const buckets = computeActivityBuckets(timestampMs) // { d, w, m }
    return [
      upsertActivityBucket('d', buckets.d, userId),
      upsertActivityBucket('w', buckets.w, userId),
      upsertActivityBucket('m', buckets.m, userId),
    ]
  },
})
```

**Use Cases:** Growth dashboards, retention analysis, cohort tracking.

---

#### 3. Revenue Analytics

Track revenue at hourly/daily/monthly granularities for financial dashboards.

```typescript
export const tables = {
  revenueStats: State.SQLite.table({
    name: 'revenueStats',
    columns: {
      productId: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'h' | 'd' | 'm'
      bucket: State.SQLite.integer(),
      totalCents: State.SQLite.integer({ default: 0 }),
      transactionCount: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['productId', 'granularity', 'bucket'],
  }),
}

// Query hourly revenue trend for last 24 hours
export async function getHourlyRevenueTrend(store: LiveStore, productId: string) {
  const endBucket = Math.floor(Date.now() / (1000 * 60 * 60))
  const startBucket = endBucket - 24

  return await store.query(
    tables.revenueStats
      .select()
      .where(sql`productId = ${productId} AND granularity = 'h'
                 AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )
}
```

**Use Cases:** Executive dashboards, sales reporting, trend analysis.

---

#### 4. Error Rate Monitoring

Track error counts at minute/hour/day for alerting and incident response.

```typescript
export const tables = {
  errorBuckets: State.SQLite.table({
    name: 'errorBuckets',
    columns: {
      service: State.SQLite.text(),
      errorType: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'm' | 'h' | 'd'
      bucket: State.SQLite.integer(),
      errorCount: State.SQLite.integer({ default: 0 }),
      requestCount: State.SQLite.integer({ default: 0 }),
      errorRate: State.SQLite.real({ default: 0 }),
    },
    primaryKey: ['service', 'errorType', 'granularity', 'bucket'],
  }),
}

// Get error rate for last 5 minutes (for alerting)
export async function getRecentErrorRate(store: LiveStore, service: string) {
  const endBucket = Math.floor(Date.now() / (1000 * 60))
  const startBucket = endBucket - 5

  const errors = await store.query(
    tables.errorBuckets
      .select()
      .where(sql`service = ${service} AND granularity = 'm'
                 AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  const totalErrors = errors.reduce((sum, e) => sum + e.errorCount, 0)
  const totalRequests = errors.reduce((sum, e) => sum + e.requestCount, 0)

  return {
    errorRate: totalRequests > 0 ? totalErrors / totalRequests : 0,
    errorCount: totalErrors,
    requestCount: totalRequests,
  }
}
```

**Use Cases:** SLA monitoring, alerting systems, incident post-mortems.

---

#### 5. Usage Metering for Billing

Track API call counts at hourly/daily/monthly granularities for usage-based billing.

```typescript
export const tables = {
  usageMetrics: State.SQLite.table({
    name: 'usageMetrics',
    columns: {
      customerId: State.SQLite.text(),
      apiEndpoint: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'h' | 'd' | 'm'
      bucket: State.SQLite.integer(),
      callCount: State.SQLite.integer({ default: 0 }),
      computeUnits: State.SQLite.integer({ default: 0 }),
      bytesTransferred: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['customerId', 'apiEndpoint', 'granularity', 'bucket'],
  }),
}

// Check hourly quota for rate limiting
export async function checkHourlyQuota(
  store: LiveStore,
  customerId: string,
  quotaLimit: number
) {
  const currentHourBucket = Math.floor(Date.now() / (1000 * 60 * 60))

  const usage = await store.query(
    tables.usageMetrics
      .select()
      .where(sql`customerId = ${customerId} AND granularity = 'h'
                 AND bucket = ${currentHourBucket}`)
  )

  const totalCalls = usage.reduce((sum, r) => sum + r.callCount, 0)

  return {
    usage: totalCalls,
    remaining: Math.max(0, quotaLimit - totalCalls),
    isOverQuota: totalCalls >= quotaLimit,
  }
}

// Generate monthly billing report
export async function generateMonthlyBill(
  store: LiveStore,
  customerId: string,
  billingMonth: number
) {
  const usage = await store.query(
    tables.usageMetrics
      .select()
      .where(sql`customerId = ${customerId} AND granularity = 'm'
                 AND bucket = ${billingMonth}`)
  )

  return {
    totalCalls: usage.reduce((sum, r) => sum + r.callCount, 0),
    totalComputeUnits: usage.reduce((sum, r) => sum + r.computeUnits, 0),
    totalBytes: usage.reduce((sum, r) => sum + r.bytesTransferred, 0),
    breakdown: usage.map(r => ({
      endpoint: r.apiEndpoint,
      calls: r.callCount,
      computeUnits: r.computeUnits,
    })),
  }
}
```

**Use Cases:** SaaS billing, quota enforcement, cost attribution, customer dashboards.

---

#### 6. Advanced Query Patterns

**Automatic Granularity Selection:**
```typescript
// Automatically choose best granularity based on time range
export async function getStatsWithAutoGranularity(
  store: LiveStore,
  endpoint: string,
  startMs: number,
  endMs: number
) {
  const ranges = computeOptimalQueryRanges(
    Math.floor(startMs / (1000 * 60)),
    Math.floor(endMs / (1000 * 60))
  )

  console.log(`Query plan: ${ranges.length} ranges across ${
    new Set(ranges.map(r => r.granularity)).size
  } granularities`)

  // Fetch and aggregate across optimal buckets
  // Full implementation in time-bucketing-examples.md
}
```

**Batch Multi-Endpoint Queries:**
```typescript
// Efficiently fetch stats for multiple endpoints in parallel
export async function getStatsForMultipleEndpoints(
  store: LiveStore,
  endpoints: string[],
  startMs: number,
  endMs: number
): Promise<Map<string, LatencyStats>> {
  const ranges = computeOptimalQueryRanges(
    Math.floor(startMs / (1000 * 60)),
    Math.floor(endMs / (1000 * 60))
  )

  const results = new Map<string, LatencyStats>()

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint IN (${endpoints.join(',')}) AND granularity = ${range.granularity}
                   AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      const current = results.get(stat.endpoint) ?? nullStats
      results.set(stat.endpoint, combineStats(current, stat))
    }
  }

  return results
}
```

**Query Plan Analysis:**
```typescript
export function analyzeQueryPlan(startMs: number, endMs: number) {
  const startBucket = Math.floor(startMs / (1000 * 60))
  const endBucket = Math.floor(endMs / (1000 * 60))
  const totalMinutes = endBucket - startBucket

  const ranges = computeOptimalQueryRanges(startBucket, endBucket)
  const bucketsToFetch = ranges.reduce((sum, range) => sum + (range.end - range.start), 0)

  return {
    totalMinutes,
    queryRanges: ranges.length,
    bucketsToFetch,
    efficiency: totalMinutes / bucketsToFetch, // Higher is better
  }
}

// Example output:
// 1 hour: 60 buckets (1.0x efficient)
// 1 day: 48 buckets (30.0x efficient)
// 30 days: 60 buckets (720.0x efficient)
```

---

#### Summary: Implementation Patterns

**Key Takeaways:**
1. **Single Event, Multiple Buckets:** Each incoming event updates 3-4 granularity levels simultaneously
2. **Combiner Functions:** Pure functions that merge stats enable efficient aggregation
3. **Optimal Query Planning:** Helper functions compute minimal bucket set to satisfy query range
4. **Space Efficiency:** Coarser granularities add <5% storage overhead (60x/24x/30x fewer buckets)
5. **Query Performance:** Large time ranges query 100-1000x fewer buckets vs minute-only approach

**Use Case Comparison:**

| Pattern | Best For | Granularities | Query Savings |
|---------|----------|---------------|---------------|
| **API Latency** | Performance monitoring | m/h/d/mo | 720x for 30-day query |
| **User Activity** | DAU/WAU/MAU metrics | d/w/m | 30x for monthly active users |
| **Revenue** | Financial dashboards | h/d/m | 720x for monthly reports |
| **Error Rates** | Alerting + incident investigation | m/h/d | 60x for daily trend |
| **Usage Metering** | Billing + quota enforcement | h/d/m | 24x for daily quotas |

**LiveStore Feature Requirements:**

To make multi-granularity bucketing ergonomic, LiveStore could provide:

1. **Bucket Helper API:** Built-in `Schema.TimeBucket(granularities: ['m', 'h', 'd'])` that auto-generates bucket columns
2. **Combiner Aggregator Primitive:** Declarative aggregation functions (like Rama's `combiner`)
3. **Subindexed Maps:** Efficient storage for millions of buckets per entity (relates to feature #6)
4. **Batch Upserts:** Single materializer emits multiple granularity updates efficiently
5. **Query Optimizer:** Automatic query planning across granularities based on range heuristics

**Related Rama Patterns:**
- **Aggregator Primitives (Feature #1):** Combiner functions for merging bucket stats
- **Subindexed Collections (Feature #6):** Efficient storage of millions of time buckets
- **Compaction Generators (Feature #9):** Rolling up old minute buckets into hour/day buckets for eventlog size management
