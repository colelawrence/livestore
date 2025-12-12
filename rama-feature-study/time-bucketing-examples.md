# Multi-Granularity Time Bucketing - Concrete Examples

This document expands on the Multi-Granularity Time Bucketing feature idea with detailed TypeScript/LiveStore examples.

## Overview

Multi-granularity time bucketing allows efficient time-series analytics by storing aggregated statistics at multiple time scales (minute, hour, day, month). This dramatically reduces query costs for large time ranges while maintaining fine-grained detail where needed.

**Key Benefits:**
- Query 1 year of data by reading ~12 month buckets instead of 525,600 minute buckets
- Coarser granularities use minimal space (60x fewer hour buckets than minute buckets)
- Optimal query planning automatically selects best granularity mix
- Single write updates all granularities simultaneously

---

## 1. API Latency Tracking

Track request latencies at minute/hour/day/month granularities for performance monitoring dashboards.

### Schema

```typescript
import { Events, makeSchema, Schema, State } from '@livestore/livestore'

// Stats aggregated per time bucket
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
      lastMs: State.SQLite.integer({ nullable: true }),
      lastUpdated: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
    primaryKey: ['endpoint', 'granularity', 'bucket'],
    indexes: [
      { name: 'latency_endpoint_granularity', columns: ['endpoint', 'granularity', 'bucket'] },
    ],
  }),
}

export const events = {
  apiRequestCompleted: Events.synced({
    name: 'v1.ApiRequestCompleted',
    schema: Schema.Struct({
      endpoint: Schema.String,
      latencyMs: Schema.Number,
      timestampMs: Schema.Number,
    }),
  }),
}
```

### Helper Functions

```typescript
/**
 * Compute time buckets at all granularities from a single timestamp.
 * Matches Rama's emit-index-granularities pattern.
 */
export function computeTimeBuckets(timestampMs: number) {
  const minuteBucket = Math.floor(timestampMs / (1000 * 60))
  const hourBucket = Math.floor(minuteBucket / 60)
  const dayBucket = Math.floor(hourBucket / 24)
  const monthBucket = Math.floor(dayBucket / 30)

  return {
    m: minuteBucket,
    h: hourBucket,
    d: dayBucket,
    mo: monthBucket,
  }
}

/**
 * Compute optimal granularities to minimize queries for a time range.
 * Matches Rama's query-granularities helper.
 *
 * Example: Query from minute 58 of hour 6 to minute 3 of hour 20:
 * Returns: [
 *   { granularity: 'm', start: 58, end: 60 },    // tail of hour 6
 *   { granularity: 'h', start: 7, end: 20 },     // full hours 7-19
 *   { granularity: 'm', start: 1200, end: 1203 } // start of hour 20
 * ]
 */
export type QueryRange = { granularity: 'm' | 'h' | 'd' | 'mo', start: number, end: number }

export function computeOptimalQueryRanges(
  startMinuteBucket: number,
  endMinuteBucket: number
): QueryRange[] {
  const ranges: QueryRange[] = []

  function helper(
    granularity: 'm' | 'h' | 'd' | 'mo',
    start: number,
    end: number
  ): void {
    const nextGranularity = { m: 'h', h: 'd', d: 'mo', mo: null }[granularity] as 'm' | 'h' | 'd' | 'mo' | null
    const divisor = { m: 60, h: 24, d: 30 }[granularity]

    if (!nextGranularity || !divisor) {
      ranges.push({ granularity, start, end })
      return
    }

    // Calculate aligned boundaries at next granularity
    const nextStart = start % divisor !== 0
      ? Math.floor(start / divisor) + 1
      : Math.floor(start / divisor)
    const nextEnd = Math.floor(end / divisor)
    const nextAlignedStart = nextStart * divisor
    const nextAlignedEnd = nextEnd * divisor

    // Recurse for middle section at coarser granularity
    if (nextEnd > nextStart) {
      helper(nextGranularity, nextStart, nextEnd)
    }

    // Add current granularity ranges for non-aligned edges
    if (nextAlignedStart >= nextAlignedEnd) {
      ranges.push({ granularity, start, end })
    } else {
      if (nextAlignedStart > start) {
        ranges.push({ granularity, start, end: nextAlignedStart })
      }
      if (end > nextAlignedEnd) {
        ranges.push({ granularity, start: nextAlignedEnd, end })
      }
    }
  }

  helper('m', startMinuteBucket, endMinuteBucket)
  return ranges
}

/**
 * Merge two latency stats objects.
 * Matches Rama's +combine-measurements combiner.
 */
export function combineStats(
  s1: LatencyStats | null,
  s2: LatencyStats
): LatencyStats {
  if (!s1) return s2

  return {
    count: s1.count + s2.count,
    totalMs: s1.totalMs + s2.totalMs,
    minMs: s1.minMs == null ? s2.minMs : s2.minMs == null ? s1.minMs : Math.min(s1.minMs, s2.minMs),
    maxMs: s1.maxMs == null ? s2.maxMs : s2.maxMs == null ? s1.maxMs : Math.max(s1.maxMs, s2.maxMs),
    lastMs: s2.lastMs ?? s1.lastMs,
  }
}

type LatencyStats = {
  count: number
  totalMs: number
  minMs: number | null
  maxMs: number | null
  lastMs: number | null
}
```

### Materializer

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.ApiRequestCompleted': ({ endpoint, latencyMs, timestampMs }) => {
    const buckets = computeTimeBuckets(timestampMs)
    const singleStat = {
      count: 1,
      totalMs: latencyMs,
      minMs: latencyMs,
      maxMs: latencyMs,
      lastMs: latencyMs,
      lastUpdated: Date.now(),
    }

    // Update all granularities in a single transaction
    return [
      upsertLatencyStat(endpoint, 'm', buckets.m, singleStat),
      upsertLatencyStat(endpoint, 'h', buckets.h, singleStat),
      upsertLatencyStat(endpoint, 'd', buckets.d, singleStat),
      upsertLatencyStat(endpoint, 'mo', buckets.mo, singleStat),
    ]
  },
})

function upsertLatencyStat(
  endpoint: string,
  granularity: string,
  bucket: number,
  stat: LatencyStats & { lastUpdated: number }
) {
  // Use SQL COALESCE for combining stats on conflict
  return tables.latencyStats.insert({
    endpoint,
    granularity,
    bucket,
    ...stat,
  }).onConflict(['endpoint', 'granularity', 'bucket'], (existing) => ({
    count: existing.count + stat.count,
    totalMs: existing.totalMs + stat.totalMs,
    minMs: existing.minMs == null ? stat.minMs : stat.minMs == null ? existing.minMs : Math.min(existing.minMs, stat.minMs),
    maxMs: existing.maxMs == null ? stat.maxMs : stat.maxMs == null ? existing.maxMs : Math.max(existing.maxMs, stat.maxMs),
    lastMs: stat.lastMs ?? existing.lastMs,
    lastUpdated: stat.lastUpdated,
  }))
}
```

### Query Functions

```typescript
import { sql } from '@livestore/livestore'

/**
 * Query latency stats for a time range using optimal granularity mix.
 * Matches Rama's get-stats-for-minute-range query topology.
 */
export async function getLatencyStatsForRange(
  store: LiveStore,
  endpoint: string,
  startMs: number,
  endMs: number
): Promise<LatencyStats> {
  const startBucket = Math.floor(startMs / (1000 * 60))
  const endBucket = Math.floor(endMs / (1000 * 60))
  const ranges = computeOptimalQueryRanges(startBucket, endBucket)

  let aggregated: LatencyStats | null = null

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint = ${endpoint} AND granularity = ${range.granularity} AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      aggregated = combineStats(aggregated, {
        count: stat.count,
        totalMs: stat.totalMs,
        minMs: stat.minMs,
        maxMs: stat.maxMs,
        lastMs: stat.lastMs,
      })
    }
  }

  return aggregated ?? { count: 0, totalMs: 0, minMs: null, maxMs: null, lastMs: null }
}

/**
 * Get p50/p95/p99 latency for dashboard display.
 */
export async function getLatencyPercentiles(
  store: LiveStore,
  endpoint: string,
  startMs: number,
  endMs: number
) {
  const stats = await getLatencyStatsForRange(store, endpoint, startMs, endMs)

  return {
    count: stats.count,
    avgMs: stats.count > 0 ? stats.totalMs / stats.count : 0,
    minMs: stats.minMs ?? 0,
    maxMs: stats.maxMs ?? 0,
    // Note: For true percentiles, store T-Digest or histogram in each bucket
  }
}
```

### Usage Example

```typescript
// Track API requests
await store.commit(events.apiRequestCompleted.create({
  endpoint: '/api/users',
  latencyMs: 125,
  timestampMs: Date.now(),
}))

// Query last 7 days of latency stats
const stats = await getLatencyStatsForRange(
  store,
  '/api/users',
  Date.now() - 7 * 24 * 60 * 60 * 1000,
  Date.now()
)

console.log(`Avg latency: ${stats.totalMs / stats.count}ms`)
console.log(`Min: ${stats.minMs}ms, Max: ${stats.maxMs}ms`)
```

---

## 2. User Activity Metrics (DAU/WAU/MAU)

Track daily/weekly/monthly active users with automatic rollup to support growth dashboards.

### Schema

```typescript
export const tables = {
  activityBuckets: State.SQLite.table({
    name: 'activityBuckets',
    columns: {
      granularity: State.SQLite.text(), // 'd' | 'w' | 'm'
      bucket: State.SQLite.integer(),
      // Store user IDs as comma-separated string (or use JSON/bloom filter for scale)
      activeUserIds: State.SQLite.text({ default: '' }),
      uniqueUserCount: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['granularity', 'bucket'],
  }),
}

export const events = {
  userActiveEvent: Events.synced({
    name: 'v1.UserActiveEvent',
    schema: Schema.Struct({
      userId: Schema.String,
      timestampMs: Schema.Number,
    }),
  }),
}
```

### Helper Functions

```typescript
export function computeActivityBuckets(timestampMs: number) {
  const dayBucket = Math.floor(timestampMs / (1000 * 60 * 60 * 24))
  const weekBucket = Math.floor(dayBucket / 7)
  const monthBucket = Math.floor(dayBucket / 30)

  return { d: dayBucket, w: weekBucket, m: monthBucket }
}

/**
 * Merge user ID sets (simplified version - use HyperLogLog or Bloom filter for scale).
 */
export function mergeUserSets(set1: string, set2: string): { merged: string, count: number } {
  const users1 = set1 ? set1.split(',') : []
  const users2 = set2 ? set2.split(',') : []
  const merged = Array.from(new Set([...users1, ...users2]))

  return {
    merged: merged.join(','),
    count: merged.length,
  }
}
```

### Materializer

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.UserActiveEvent': ({ userId, timestampMs }) => {
    const buckets = computeActivityBuckets(timestampMs)

    return [
      upsertActivityBucket('d', buckets.d, userId),
      upsertActivityBucket('w', buckets.w, userId),
      upsertActivityBucket('m', buckets.m, userId),
    ]
  },
})

function upsertActivityBucket(granularity: string, bucket: number, userId: string) {
  return tables.activityBuckets.insert({
    granularity,
    bucket,
    activeUserIds: userId,
    uniqueUserCount: 1,
  }).onConflict(['granularity', 'bucket'], (existing) => {
    const merged = mergeUserSets(existing.activeUserIds, userId)
    return {
      activeUserIds: merged.merged,
      uniqueUserCount: merged.count,
    }
  })
}
```

### Query Functions

```typescript
/**
 * Get DAU for a specific day.
 */
export async function getDailyActiveUsers(store: LiveStore, date: Date): Promise<number> {
  const dayBucket = Math.floor(date.getTime() / (1000 * 60 * 60 * 24))
  const result = await store.query(
    tables.activityBuckets
      .select()
      .where(sql`granularity = 'd' AND bucket = ${dayBucket}`)
  )

  return result[0]?.uniqueUserCount ?? 0
}

/**
 * Get MAU by combining daily buckets in the last 30 days.
 */
export async function getMonthlyActiveUsers(store: LiveStore, endDate: Date): Promise<number> {
  const endDayBucket = Math.floor(endDate.getTime() / (1000 * 60 * 60 * 24))
  const startDayBucket = endDayBucket - 30

  const results = await store.query(
    tables.activityBuckets
      .select()
      .where(sql`granularity = 'd' AND bucket >= ${startDayBucket} AND bucket < ${endDayBucket}`)
  )

  // Merge all user sets
  let allUsers = ''
  for (const bucket of results) {
    const merged = mergeUserSets(allUsers, bucket.activeUserIds)
    allUsers = merged.merged
  }

  return allUsers.split(',').filter(Boolean).length
}
```

---

## 3. Revenue Analytics

Track revenue with hourly/daily/monthly buckets for financial dashboards and reporting.

### Schema

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
      avgCents: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['productId', 'granularity', 'bucket'],
    indexes: [
      { name: 'revenue_product_granularity', columns: ['productId', 'granularity', 'bucket'] },
    ],
  }),
}

export const events = {
  purchaseCompleted: Events.synced({
    name: 'v1.PurchaseCompleted',
    schema: Schema.Struct({
      productId: Schema.String,
      amountCents: Schema.Number,
      timestampMs: Schema.Number,
    }),
  }),
}
```

### Materializer

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.PurchaseCompleted': ({ productId, amountCents, timestampMs }) => {
    const minuteBucket = Math.floor(timestampMs / (1000 * 60))
    const hourBucket = Math.floor(minuteBucket / 60)
    const dayBucket = Math.floor(hourBucket / 24)
    const monthBucket = Math.floor(dayBucket / 30)

    return [
      upsertRevenueStat('h', hourBucket, productId, amountCents),
      upsertRevenueStat('d', dayBucket, productId, amountCents),
      upsertRevenueStat('m', monthBucket, productId, amountCents),
    ]
  },
})

function upsertRevenueStat(
  granularity: string,
  bucket: number,
  productId: string,
  amountCents: number
) {
  return tables.revenueStats.insert({
    productId,
    granularity,
    bucket,
    totalCents: amountCents,
    transactionCount: 1,
    avgCents: amountCents,
  }).onConflict(['productId', 'granularity', 'bucket'], (existing) => {
    const newTotal = existing.totalCents + amountCents
    const newCount = existing.transactionCount + 1
    return {
      totalCents: newTotal,
      transactionCount: newCount,
      avgCents: Math.floor(newTotal / newCount),
    }
  })
}
```

### Query Functions

```typescript
/**
 * Get hourly revenue trend for last 24 hours.
 */
export async function getHourlyRevenueTrend(
  store: LiveStore,
  productId: string,
  hours: number = 24
): Promise<Array<{ hour: number, revenue: number }>> {
  const endBucket = Math.floor(Date.now() / (1000 * 60 * 60))
  const startBucket = endBucket - hours

  const results = await store.query(
    tables.revenueStats
      .select()
      .where(sql`productId = ${productId} AND granularity = 'h' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  return results.map(r => ({
    hour: r.bucket,
    revenue: r.totalCents / 100, // Convert to dollars
  }))
}

/**
 * Get monthly revenue summary.
 */
export async function getMonthlyRevenue(
  store: LiveStore,
  productId: string,
  monthsBack: number = 6
): Promise<number> {
  const endBucket = Math.floor(Date.now() / (1000 * 60 * 60 * 24 * 30))
  const startBucket = endBucket - monthsBack

  const results = await store.query(
    tables.revenueStats
      .select()
      .where(sql`productId = ${productId} AND granularity = 'm' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  return results.reduce((sum, r) => sum + r.totalCents, 0) / 100
}
```

---

## 4. Error Rate Monitoring

Track error counts at multiple granularities for alerting and incident response.

### Schema

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
      errorRate: State.SQLite.real({ default: 0 }), // percentage
      lastErrorMessage: State.SQLite.text({ nullable: true }),
    },
    primaryKey: ['service', 'errorType', 'granularity', 'bucket'],
  }),

  requestBuckets: State.SQLite.table({
    name: 'requestBuckets',
    columns: {
      service: State.SQLite.text(),
      granularity: State.SQLite.text(),
      bucket: State.SQLite.integer(),
      requestCount: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['service', 'granularity', 'bucket'],
  }),
}

export const events = {
  requestCompleted: Events.synced({
    name: 'v1.RequestCompleted',
    schema: Schema.Struct({
      service: Schema.String,
      timestampMs: Schema.Number,
      error: Schema.Optional(Schema.Struct({
        type: Schema.String,
        message: Schema.String,
      })),
    }),
  }),
}
```

### Materializer

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.RequestCompleted': ({ service, timestampMs, error }) => {
    const buckets = computeTimeBuckets(timestampMs)
    const updates = []

    // Track total requests at all granularities
    for (const [granularity, bucket] of Object.entries(buckets)) {
      updates.push(
        tables.requestBuckets.insert({
          service,
          granularity,
          bucket,
          requestCount: 1,
        }).onConflict(['service', 'granularity', 'bucket'], (existing) => ({
          requestCount: existing.requestCount + 1,
        }))
      )

      // Track errors if present
      if (error) {
        updates.push(
          tables.errorBuckets.insert({
            service,
            errorType: error.type,
            granularity,
            bucket,
            errorCount: 1,
            requestCount: 1,
            errorRate: 1.0,
            lastErrorMessage: error.message,
          }).onConflict(['service', 'errorType', 'granularity', 'bucket'], (existing) => ({
            errorCount: existing.errorCount + 1,
            requestCount: existing.requestCount + 1,
            errorRate: (existing.errorCount + 1) / (existing.requestCount + 1),
            lastErrorMessage: error.message,
          }))
        )
      }
    }

    return updates
  },
})
```

### Query Functions

```typescript
/**
 * Get error rate for last N minutes (for alerting).
 */
export async function getRecentErrorRate(
  store: LiveStore,
  service: string,
  minutes: number = 5
): Promise<{ errorRate: number, errorCount: number, requestCount: number }> {
  const endBucket = Math.floor(Date.now() / (1000 * 60))
  const startBucket = endBucket - minutes

  const errors = await store.query(
    tables.errorBuckets
      .select()
      .where(sql`service = ${service} AND granularity = 'm' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  const requests = await store.query(
    tables.requestBuckets
      .select()
      .where(sql`service = ${service} AND granularity = 'm' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  const totalErrors = errors.reduce((sum, e) => sum + e.errorCount, 0)
  const totalRequests = requests.reduce((sum, r) => sum + r.requestCount, 0)

  return {
    errorRate: totalRequests > 0 ? totalErrors / totalRequests : 0,
    errorCount: totalErrors,
    requestCount: totalRequests,
  }
}

/**
 * Get daily error trends for incident investigation.
 */
export async function getDailyErrorTrend(
  store: LiveStore,
  service: string,
  days: number = 7
): Promise<Array<{ day: number, errorRate: number, topErrors: string[] }>> {
  const endBucket = Math.floor(Date.now() / (1000 * 60 * 60 * 24))
  const startBucket = endBucket - days

  const errors = await store.query(
    tables.errorBuckets
      .select()
      .where(sql`service = ${service} AND granularity = 'd' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  const requests = await store.query(
    tables.requestBuckets
      .select()
      .where(sql`service = ${service} AND granularity = 'd' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  const byDay = new Map<number, { errors: number, requests: number, types: Set<string> }>()

  for (const err of errors) {
    const day = byDay.get(err.bucket) ?? { errors: 0, requests: 0, types: new Set() }
    day.errors += err.errorCount
    day.types.add(err.errorType)
    byDay.set(err.bucket, day)
  }

  for (const req of requests) {
    const day = byDay.get(req.bucket) ?? { errors: 0, requests: 0, types: new Set() }
    day.requests += req.requestCount
    byDay.set(req.bucket, day)
  }

  return Array.from(byDay.entries()).map(([bucket, data]) => ({
    day: bucket,
    errorRate: data.requests > 0 ? data.errors / data.requests : 0,
    topErrors: Array.from(data.types),
  }))
}
```

---

## 5. Usage Metering for Billing

Track API call counts at multiple time scales for usage-based billing and quota enforcement.

### Schema

```typescript
export const tables = {
  usageMetrics: State.SQLite.table({
    name: 'usageMetrics',
    columns: {
      customerId: State.SQLite.text(),
      apiEndpoint: State.SQLite.text(),
      granularity: State.SQLite.text(), // 'h' | 'd' | 'm' (month)
      bucket: State.SQLite.integer(),
      callCount: State.SQLite.integer({ default: 0 }),
      computeUnits: State.SQLite.integer({ default: 0 }), // weighted by complexity
      bytesTransferred: State.SQLite.integer({ default: 0 }),
    },
    primaryKey: ['customerId', 'apiEndpoint', 'granularity', 'bucket'],
  }),
}

export const events = {
  apiCallCompleted: Events.synced({
    name: 'v1.ApiCallCompleted',
    schema: Schema.Struct({
      customerId: Schema.String,
      apiEndpoint: Schema.String,
      timestampMs: Schema.Number,
      computeUnits: Schema.Number,
      bytesTransferred: Schema.Number,
    }),
  }),
}
```

### Materializer

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.ApiCallCompleted': ({ customerId, apiEndpoint, timestampMs, computeUnits, bytesTransferred }) => {
    const minuteBucket = Math.floor(timestampMs / (1000 * 60))
    const hourBucket = Math.floor(minuteBucket / 60)
    const dayBucket = Math.floor(hourBucket / 24)
    const monthBucket = Math.floor(dayBucket / 30)

    return [
      upsertUsageMetric('h', hourBucket, customerId, apiEndpoint, computeUnits, bytesTransferred),
      upsertUsageMetric('d', dayBucket, customerId, apiEndpoint, computeUnits, bytesTransferred),
      upsertUsageMetric('m', monthBucket, customerId, apiEndpoint, computeUnits, bytesTransferred),
    ]
  },
})

function upsertUsageMetric(
  granularity: string,
  bucket: number,
  customerId: string,
  apiEndpoint: string,
  computeUnits: number,
  bytesTransferred: number
) {
  return tables.usageMetrics.insert({
    customerId,
    apiEndpoint,
    granularity,
    bucket,
    callCount: 1,
    computeUnits,
    bytesTransferred,
  }).onConflict(['customerId', 'apiEndpoint', 'granularity', 'bucket'], (existing) => ({
    callCount: existing.callCount + 1,
    computeUnits: existing.computeUnits + computeUnits,
    bytesTransferred: existing.bytesTransferred + bytesTransferred,
  }))
}
```

### Query Functions

```typescript
/**
 * Check if customer is approaching hourly quota (for rate limiting).
 */
export async function checkHourlyQuota(
  store: LiveStore,
  customerId: string,
  quotaLimit: number
): Promise<{ usage: number, remaining: number, isOverQuota: boolean }> {
  const currentHourBucket = Math.floor(Date.now() / (1000 * 60 * 60))

  const results = await store.query(
    tables.usageMetrics
      .select()
      .where(sql`customerId = ${customerId} AND granularity = 'h' AND bucket = ${currentHourBucket}`)
  )

  const totalCalls = results.reduce((sum, r) => sum + r.callCount, 0)

  return {
    usage: totalCalls,
    remaining: Math.max(0, quotaLimit - totalCalls),
    isOverQuota: totalCalls >= quotaLimit,
  }
}

/**
 * Generate monthly billing report.
 */
export async function generateMonthlyBill(
  store: LiveStore,
  customerId: string,
  billingMonth: number // month bucket number
): Promise<{
  totalCalls: number
  totalComputeUnits: number
  totalBytes: number
  breakdown: Array<{ endpoint: string, calls: number, computeUnits: number }>
}> {
  const results = await store.query(
    tables.usageMetrics
      .select()
      .where(sql`customerId = ${customerId} AND granularity = 'm' AND bucket = ${billingMonth}`)
  )

  const breakdown = results.map(r => ({
    endpoint: r.apiEndpoint,
    calls: r.callCount,
    computeUnits: r.computeUnits,
  }))

  return {
    totalCalls: results.reduce((sum, r) => sum + r.callCount, 0),
    totalComputeUnits: results.reduce((sum, r) => sum + r.computeUnits, 0),
    totalBytes: results.reduce((sum, r) => sum + r.bytesTransferred, 0),
    breakdown,
  }
}

/**
 * Get usage trend over last 30 days for customer dashboard.
 */
export async function getDailyUsageTrend(
  store: LiveStore,
  customerId: string,
  days: number = 30
): Promise<Array<{ day: number, calls: number, computeUnits: number }>> {
  const endBucket = Math.floor(Date.now() / (1000 * 60 * 60 * 24))
  const startBucket = endBucket - days

  const results = await store.query(
    tables.usageMetrics
      .select()
      .where(sql`customerId = ${customerId} AND granularity = 'd' AND bucket >= ${startBucket} AND bucket < ${endBucket}`)
  )

  // Group by day bucket
  const byDay = new Map<number, { calls: number, computeUnits: number }>()

  for (const result of results) {
    const day = byDay.get(result.bucket) ?? { calls: 0, computeUnits: 0 }
    day.calls += result.callCount
    day.computeUnits += result.computeUnits
    byDay.set(result.bucket, day)
  }

  return Array.from(byDay.entries()).map(([bucket, data]) => ({
    day: bucket,
    calls: data.calls,
    computeUnits: data.computeUnits,
  }))
}
```

---

## 6. Custom Granularity Queries

Helper patterns for querying across granularity boundaries efficiently.

### Advanced Query Patterns

```typescript
/**
 * Query with automatic granularity selection based on time range.
 * Small ranges use fine granularity, large ranges use coarse granularity.
 */
export async function getStatsWithAutoGranularity(
  store: LiveStore,
  endpoint: string,
  startMs: number,
  endMs: number
): Promise<LatencyStats> {
  const rangeMs = endMs - startMs
  const ranges = computeOptimalQueryRanges(
    Math.floor(startMs / (1000 * 60)),
    Math.floor(endMs / (1000 * 60))
  )

  console.log(`Query range: ${rangeMs / 1000 / 60} minutes`)
  console.log(`Optimal query plan: ${ranges.length} ranges across ${new Set(ranges.map(r => r.granularity)).size} granularities`)

  let aggregated: LatencyStats | null = null

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint = ${endpoint} AND granularity = ${range.granularity} AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      aggregated = combineStats(aggregated, {
        count: stat.count,
        totalMs: stat.totalMs,
        minMs: stat.minMs,
        maxMs: stat.maxMs,
        lastMs: stat.lastMs,
      })
    }
  }

  return aggregated ?? { count: 0, totalMs: 0, minMs: null, maxMs: null, lastMs: null }
}

/**
 * Stream stats across granularities for real-time dashboard updates.
 */
export async function* streamStatsAcrossGranularities(
  store: LiveStore,
  endpoint: string,
  startBucket: number,
  endBucket: number
): AsyncGenerator<{ granularity: string, bucket: number, stats: LatencyStats }> {
  const ranges = computeOptimalQueryRanges(startBucket, endBucket)

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint = ${endpoint} AND granularity = ${range.granularity} AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      yield {
        granularity: range.granularity,
        bucket: stat.bucket,
        stats: {
          count: stat.count,
          totalMs: stat.totalMs,
          minMs: stat.minMs,
          maxMs: stat.maxMs,
          lastMs: stat.lastMs,
        },
      }
    }
  }
}

/**
 * Batch query multiple endpoints efficiently.
 */
export async function getStatsForMultipleEndpoints(
  store: LiveStore,
  endpoints: string[],
  startMs: number,
  endMs: number
): Promise<Map<string, LatencyStats>> {
  const startBucket = Math.floor(startMs / (1000 * 60))
  const endBucket = Math.floor(endMs / (1000 * 60))
  const ranges = computeOptimalQueryRanges(startBucket, endBucket)

  const results = new Map<string, LatencyStats>()

  // Fetch all stats in parallel
  for (const endpoint of endpoints) {
    results.set(endpoint, { count: 0, totalMs: 0, minMs: null, maxMs: null, lastMs: null })
  }

  for (const range of ranges) {
    const stats = await store.query(
      tables.latencyStats
        .select()
        .where(sql`endpoint IN (${endpoints.join(',')}) AND granularity = ${range.granularity} AND bucket >= ${range.start} AND bucket < ${range.end}`)
    )

    for (const stat of stats) {
      const current = results.get(stat.endpoint)!
      results.set(stat.endpoint, combineStats(current, {
        count: stat.count,
        totalMs: stat.totalMs,
        minMs: stat.minMs,
        maxMs: stat.maxMs,
        lastMs: stat.lastMs,
      }))
    }
  }

  return results
}
```

### Bucket Alignment Utilities

```typescript
/**
 * Round timestamp to granularity boundary.
 */
export function alignTimeToBucket(timestampMs: number, granularity: 'm' | 'h' | 'd' | 'mo'): number {
  const divisors = {
    m: 1000 * 60,
    h: 1000 * 60 * 60,
    d: 1000 * 60 * 60 * 24,
    mo: 1000 * 60 * 60 * 24 * 30,
  }

  const divisor = divisors[granularity]
  return Math.floor(timestampMs / divisor) * divisor
}

/**
 * Get human-readable bucket description.
 */
export function describeBucket(granularity: string, bucket: number): string {
  const descriptions = {
    m: () => {
      const date = new Date(bucket * 1000 * 60)
      return `${date.toISOString().slice(0, 16)}`
    },
    h: () => {
      const date = new Date(bucket * 1000 * 60 * 60)
      return `${date.toISOString().slice(0, 13)}:00`
    },
    d: () => {
      const date = new Date(bucket * 1000 * 60 * 60 * 24)
      return date.toISOString().slice(0, 10)
    },
    mo: () => {
      const date = new Date(bucket * 1000 * 60 * 60 * 24 * 30)
      return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`
    },
  }

  return descriptions[granularity as keyof typeof descriptions]?.() ?? `Bucket ${bucket}`
}

/**
 * Test query plan efficiency.
 */
export function analyzeQueryPlan(startMs: number, endMs: number): {
  totalMinutes: number
  queryRanges: number
  bucketsToFetch: number
  efficiency: number
} {
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
```

### Usage Example

```typescript
// Analyze query efficiency for different ranges
const oneHourPlan = analyzeQueryPlan(Date.now() - 60 * 60 * 1000, Date.now())
console.log(`1 hour query: ${oneHourPlan.bucketsToFetch} buckets (${oneHourPlan.efficiency.toFixed(1)}x efficient)`)

const oneDayPlan = analyzeQueryPlan(Date.now() - 24 * 60 * 60 * 1000, Date.now())
console.log(`1 day query: ${oneDayPlan.bucketsToFetch} buckets (${oneDayPlan.efficiency.toFixed(1)}x efficient)`)

const oneMonthPlan = analyzeQueryPlan(Date.now() - 30 * 24 * 60 * 60 * 1000, Date.now())
console.log(`30 day query: ${oneMonthPlan.bucketsToFetch} buckets (${oneMonthPlan.efficiency.toFixed(1)}x efficient)`)

// Expected output:
// 1 hour query: 60 buckets (1.0x efficient)
// 1 day query: 48 buckets (30.0x efficient) - uses hour buckets for middle
// 30 day query: 60 buckets (720.0x efficient) - uses day/month buckets
```

---

## Summary: Implementation Patterns

### Key Takeaways

1. **Single Event, Multiple Buckets**: Each incoming event updates 3-4 granularity levels simultaneously
2. **Combiner Functions**: Pure functions that merge stats enable efficient aggregation
3. **Optimal Query Planning**: Helper functions compute minimal bucket set to satisfy query range
4. **Space Efficiency**: Coarser granularities add <5% storage overhead due to 60x/24x/30x fewer buckets
5. **Query Performance**: Large time ranges query 100-1000x fewer buckets vs. minute-only approach

### Use Case Summary

| Pattern | Best For | Granularities | Query Savings |
|---------|----------|---------------|---------------|
| **API Latency** | Performance monitoring | m/h/d/mo | 720x for 30-day query |
| **User Activity** | DAU/WAU/MAU metrics | d/w/m | 30x for monthly active users |
| **Revenue** | Financial dashboards | h/d/m | 720x for monthly reports |
| **Error Rates** | Alerting + incident investigation | m/h/d | 60x for daily trend |
| **Usage Metering** | Billing + quota enforcement | h/d/m | 24x for daily quotas |

### LiveStore Feature Requirements

To make multi-granularity bucketing ergonomic in LiveStore, consider:

1. **Bucket Helper API**: Built-in `Schema.TimeBucket(granularities: ['m', 'h', 'd'])` that auto-generates bucket columns
2. **Combiner Aggregator Primitive**: Declarative aggregation functions (like Rama's `combiner`)
3. **Subindexed Maps**: Efficient storage for millions of buckets per entity (relates to feature #6)
4. **Batch Upserts**: Single materializer emits multiple granularity updates efficiently
5. **Query Optimizer**: Automatic query planning across granularities based on range heuristics
