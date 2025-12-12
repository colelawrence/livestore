## Task Globals for IO Resources - Concrete Examples

### Overview

Task globals provide managed lifecycle for external resources needed by materializers. This pattern enables:

- **Connection pooling**: Reuse HTTP clients, database connections, etc. across events
- **Resource cleanup**: Guaranteed cleanup on store shutdown
- **Thread safety**: Resources can be task-local or shared with proper synchronization
- **Error handling**: Failed resources fail the materializer gracefully
- **Performance**: Avoid recreating connections for every event

---

### Example 1: HTTP Client Pool for Webhooks

**Use case**: Send webhook notifications on specific events without creating a new connection for each event.

```typescript
import { Effect, Layer, HttpClient, Scope } from '@livestore/utils/effect'
import type { LiveStoreSchema } from '@livestore/common'

/**
 * Task global for managing HTTP client lifecycle.
 * The client is created once when the store boots and reused across all events.
 */
interface WebhookHttpClient {
  readonly _tag: 'WebhookHttpClient'
  readonly client: HttpClient.HttpClient.Default
}

const WebhookHttpClient = Effect.Tag<WebhookHttpClient>()

/**
 * Layer that provides the HTTP client with connection pooling.
 * Automatically cleaned up when the store shuts down via Scope.
 */
const makeWebhookHttpClientLayer = Layer.effect(
  WebhookHttpClient,
  Effect.gen(function* () {
    // HttpClient from Effect provides connection pooling by default
    const client = yield* HttpClient.HttpClient

    return {
      _tag: 'WebhookHttpClient' as const,
      client,
    }
  }),
).pipe(
  Layer.provide(
    // Configure connection pool settings
    Layer.setConfigProvider(
      HttpClient.HttpClient,
      HttpClient.makeConfig({
        maxConnections: 100,
        timeout: Effect.Duration.seconds(30),
      }),
    ),
  ),
)

/**
 * Usage in materializer: Send webhook when order is created.
 */
const materializers = State.SQLite.materializers(events, {
  'v1.OrderCreated': ({ orderId, customerId, total }, { query }) =>
    Effect.gen(function* () {
      // Access the task global HTTP client
      const { client } = yield* WebhookHttpClient

      // Insert the order into the database
      const insertOp = tables.orders.insert({ orderId, customerId, total, status: 'pending' })

      // Send webhook notification (fire-and-forget via fork)
      yield* HttpClient.request
        .post('https://api.example.com/webhooks/order-created')
        .pipe(
          HttpClient.request.jsonBody({
            orderId,
            customerId,
            total,
            timestamp: Date.now(),
          }),
          client.execute,
          Effect.tapErrorCause((cause) =>
            // Log errors but don't fail the materializer
            Effect.logError('Webhook delivery failed', { orderId, cause }),
          ),
          Effect.forkDaemon, // Run in background, don't block materialization
        )

      return insertOp
    }).pipe(Effect.provide(makeWebhookHttpClientLayer)),
})

/**
 * Advanced: Retry webhooks with exponential backoff
 */
const sendWebhookWithRetry = (payload: WebhookPayload) =>
  Effect.gen(function* () {
    const { client } = yield* WebhookHttpClient

    yield* HttpClient.request
      .post('https://api.example.com/webhooks/order-created')
      .pipe(
        HttpClient.request.jsonBody(payload),
        client.execute,
        Effect.retry({
          schedule: Schedule.exponential('100 millis').pipe(
            Schedule.jittered,
            Schedule.upTo('30 seconds'),
          ),
        }),
        Effect.timeout('60 seconds'),
      )
  })
```

**Key patterns**:
- HTTP client created once via Layer, shared across events
- Webhooks sent via `Effect.forkDaemon` to avoid blocking materialization
- Connection pooling handled by Effect's HttpClient
- Automatic cleanup when store shuts down via Scope

---

### Example 2: AI/LLM Client with Connection Pooling

**Use case**: Generate AI summaries or embeddings for content without recreating the client.

```typescript
import { Anthropic } from '@anthropic-ai/sdk'
import { OpenAI } from 'openai'
import { Effect, Context, Layer, Scope } from '@livestore/utils/effect'

/**
 * Task global for AI client (Anthropic Claude, OpenAI, etc.)
 */
interface AIClient {
  readonly _tag: 'AIClient'
  readonly anthropic: Anthropic
  readonly openai: OpenAI
}

const AIClient = Context.GenericTag<AIClient>('AIClient')

/**
 * Layer providing AI clients with connection pooling.
 * Uses environment variables for API keys.
 */
const makeAIClientLayer = Layer.scoped(
  AIClient,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    // Initialize clients (they manage their own connection pools internally)
    const anthropic = new Anthropic({
      apiKey: process.env.ANTHROPIC_API_KEY,
      maxRetries: 3,
    })

    const openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      maxRetries: 3,
    })

    // Register cleanup handlers
    yield* Scope.addFinalizer(
      scope,
      Effect.sync(() => {
        // Anthropic/OpenAI SDKs don't expose explicit cleanup,
        // but we can log for observability
        console.log('AI clients shutting down')
      }),
    )

    return {
      _tag: 'AIClient' as const,
      anthropic,
      openai,
    }
  }),
)

/**
 * Helper: Generate embedding using OpenAI
 */
const generateEmbedding = (text: string) =>
  Effect.gen(function* () {
    const { openai } = yield* AIClient

    const response = yield* Effect.tryPromise({
      try: () =>
        openai.embeddings.create({
          model: 'text-embedding-3-small',
          input: text,
          dimensions: 1536,
        }),
      catch: (error) => ({
        _tag: 'OpenAIError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    return response.data[0].embedding
  })

/**
 * Helper: Generate summary using Claude
 */
const generateSummary = (content: string, maxTokens = 200) =>
  Effect.gen(function* () {
    const { anthropic } = yield* AIClient

    const response = yield* Effect.tryPromise({
      try: () =>
        anthropic.messages.create({
          model: 'claude-3-5-sonnet-20241022',
          max_tokens: maxTokens,
          messages: [
            {
              role: 'user',
              content: `Summarize the following in 2-3 sentences:\n\n${content}`,
            },
          ],
        }),
      catch: (error) => ({
        _tag: 'AnthropicError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    const textBlock = response.content.find((block) => block.type === 'text')
    return textBlock?.type === 'text' ? textBlock.text : ''
  })

/**
 * Usage in materializer: Generate article summary and embedding
 */
const materializers = State.SQLite.materializers(events, {
  'v1.ArticlePublished': ({ articleId, title, content }, { query }) =>
    Effect.gen(function* () {
      // Generate AI-powered summary and embedding concurrently
      const [summary, embedding] = yield* Effect.all(
        [generateSummary(content), generateEmbedding(content)],
        { concurrency: 2 },
      )

      // Store article with AI-generated metadata
      return [
        tables.articles.insert({
          articleId,
          title,
          content,
          summary,
          publishedAt: Date.now(),
        }),
        tables.articleEmbeddings.insert({
          articleId,
          embedding: JSON.stringify(embedding), // SQLite doesn't have array type
        }),
      ]
    }).pipe(
      Effect.provide(makeAIClientLayer),
      Effect.tapErrorCause((cause) =>
        Effect.logError('AI generation failed', { articleId, cause }),
      ),
      // Timeout for AI calls to prevent hanging
      Effect.timeout('30 seconds'),
    ),
})
```

**Key patterns**:
- AI clients created once, reused across events
- Connection pooling managed internally by SDK clients
- Concurrent AI calls via `Effect.all` with concurrency control
- Timeout protection to prevent hanging materializers
- Graceful error handling with logging

---

### Example 3: Database Connection for Data Enrichment

**Use case**: Enrich events with data from an external PostgreSQL database.

```typescript
import { Effect, Layer, Scope, Pool } from '@livestore/utils/effect'
import { Pool as PgPool } from 'pg'

/**
 * Task global for PostgreSQL connection pool.
 */
interface PostgresPool {
  readonly _tag: 'PostgresPool'
  readonly pool: PgPool
}

const PostgresPool = Context.GenericTag<PostgresPool>('PostgresPool')

/**
 * Layer providing PostgreSQL connection pool.
 */
const makePostgresPoolLayer = Layer.scoped(
  PostgresPool,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    // Create connection pool
    const pool = new PgPool({
      host: process.env.PG_HOST,
      port: parseInt(process.env.PG_PORT ?? '5432', 10),
      database: process.env.PG_DATABASE,
      user: process.env.PG_USER,
      password: process.env.PG_PASSWORD,
      max: 20, // Maximum pool size
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    })

    // Test connection on startup
    yield* Effect.tryPromise({
      try: async () => {
        const client = await pool.connect()
        client.release()
      },
      catch: (error) => ({
        _tag: 'PostgresConnectionError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    // Register cleanup
    yield* Scope.addFinalizer(
      scope,
      Effect.promise(() => pool.end()),
    )

    return {
      _tag: 'PostgresPool' as const,
      pool,
    }
  }),
)

/**
 * Helper: Query external database
 */
const queryPostgres = <T>(query: string, params: any[] = []) =>
  Effect.gen(function* () {
    const { pool } = yield* PostgresPool

    const result = yield* Effect.tryPromise({
      try: () => pool.query<T>(query, params),
      catch: (error) => ({
        _tag: 'PostgresQueryError' as const,
        message: error instanceof Error ? error.message : String(error),
        query,
      }),
    })

    return result.rows
  })

/**
 * Usage in materializer: Enrich user signup with external CRM data
 */
const materializers = State.SQLite.materializers(events, {
  'v1.UserSignedUp': ({ userId, email, name }, { query }) =>
    Effect.gen(function* () {
      // Fetch enrichment data from external CRM database
      const crmData = yield* queryPostgres<{
        company: string
        industry: string
        employeeCount: number
      }>(
        'SELECT company, industry, employee_count FROM crm.contacts WHERE email = $1',
        [email],
      ).pipe(
        Effect.map((rows) => rows[0]),
        // Default values if not found
        Effect.catchTag('PostgresQueryError', () =>
          Effect.succeed({
            company: null,
            industry: null,
            employeeCount: null,
          }),
        ),
      )

      // Insert user with enriched data
      return tables.users.insert({
        userId,
        email,
        name,
        company: crmData.company,
        industry: crmData.industry,
        employeeCount: crmData.employeeCount,
        createdAt: Date.now(),
      })
    }).pipe(
      Effect.provide(makePostgresPoolLayer),
      Effect.timeout('5 seconds'),
    ),
})
```

**Key patterns**:
- Connection pool created once, connections reused
- Test connection on startup to fail fast
- Graceful fallback if enrichment fails
- Timeout protection for external queries
- Proper cleanup via Scope finalizers

---

### Example 4: Message Queue Client (Kafka/Redis)

**Use case**: Publish events to external message queue for cross-system integration.

```typescript
import { Effect, Layer, Queue, Scope } from '@livestore/utils/effect'
import { Kafka, Producer } from 'kafkajs'
import { createClient, RedisClientType } from 'redis'

/**
 * Task global for Kafka producer
 */
interface KafkaProducer {
  readonly _tag: 'KafkaProducer'
  readonly producer: Producer
}

const KafkaProducer = Context.GenericTag<KafkaProducer>('KafkaProducer')

/**
 * Layer providing Kafka producer with connection management.
 */
const makeKafkaProducerLayer = Layer.scoped(
  KafkaProducer,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    const kafka = new Kafka({
      clientId: 'livestore-app',
      brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
      retry: {
        retries: 5,
        initialRetryTime: 300,
      },
    })

    const producer = kafka.producer({
      allowAutoTopicCreation: false,
      transactionTimeout: 30000,
    })

    // Connect on startup
    yield* Effect.tryPromise({
      try: () => producer.connect(),
      catch: (error) => ({
        _tag: 'KafkaConnectionError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    // Register cleanup
    yield* Scope.addFinalizer(
      scope,
      Effect.promise(() => producer.disconnect()),
    )

    return {
      _tag: 'KafkaProducer' as const,
      producer,
    }
  }),
)

/**
 * Helper: Publish message to Kafka topic
 */
const publishToKafka = (topic: string, key: string, value: unknown) =>
  Effect.gen(function* () {
    const { producer } = yield* KafkaProducer

    yield* Effect.tryPromise({
      try: () =>
        producer.send({
          topic,
          messages: [
            {
              key,
              value: JSON.stringify(value),
              timestamp: Date.now().toString(),
            },
          ],
        }),
      catch: (error) => ({
        _tag: 'KafkaPublishError' as const,
        message: error instanceof Error ? error.message : String(error),
        topic,
        key,
      }),
    })
  })

/**
 * Task global for Redis pub/sub client
 */
interface RedisPubSub {
  readonly _tag: 'RedisPubSub'
  readonly client: RedisClientType
}

const RedisPubSub = Context.GenericTag<RedisPubSub>('RedisPubSub')

/**
 * Layer providing Redis client for pub/sub.
 */
const makeRedisPubSubLayer = Layer.scoped(
  RedisPubSub,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    const client = createClient({
      url: process.env.REDIS_URL ?? 'redis://localhost:6379',
      socket: {
        reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
      },
    })

    // Connect on startup
    yield* Effect.tryPromise({
      try: () => client.connect(),
      catch: (error) => ({
        _tag: 'RedisConnectionError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    // Register cleanup
    yield* Scope.addFinalizer(
      scope,
      Effect.promise(() => client.quit()),
    )

    return {
      _tag: 'RedisPubSub' as const,
      client,
    }
  }),
)

/**
 * Helper: Publish message to Redis channel
 */
const publishToRedis = (channel: string, message: unknown) =>
  Effect.gen(function* () {
    const { client } = yield* RedisPubSub

    yield* Effect.tryPromise({
      try: () => client.publish(channel, JSON.stringify(message)),
      catch: (error) => ({
        _tag: 'RedisPublishError' as const,
        message: error instanceof Error ? error.message : String(error),
        channel,
      }),
    })
  })

/**
 * Usage in materializer: Publish to both Kafka and Redis for event streaming
 */
const materializers = State.SQLite.materializers(events, {
  'v1.OrderShipped': ({ orderId, trackingNumber, customerId }, { query }) =>
    Effect.gen(function* () {
      const timestamp = Date.now()

      // Publish to external message queues (fire-and-forget)
      yield* Effect.all(
        [
          publishToKafka('orders.shipped', orderId, {
            orderId,
            trackingNumber,
            customerId,
            timestamp,
          }),
          publishToRedis('notifications:shipping', {
            type: 'order_shipped',
            orderId,
            trackingNumber,
            customerId,
          }),
        ],
        { concurrency: 2 },
      ).pipe(
        Effect.tapErrorCause((cause) =>
          Effect.logError('Message queue publish failed', { orderId, cause }),
        ),
        Effect.forkDaemon, // Don't block materialization
      )

      // Update local database
      return tables.orders.update({ status: 'shipped', trackingNumber, shippedAt: timestamp }).where({ orderId })
    }).pipe(
      Effect.provide(Layer.mergeAll(makeKafkaProducerLayer, makeRedisPubSubLayer)),
    ),
})
```

**Key patterns**:
- Kafka producer and Redis client managed as task globals
- Connection established once at startup, reused for all events
- Automatic reconnection handled by client libraries
- Fire-and-forget publishing via `Effect.forkDaemon`
- Graceful cleanup on shutdown

---

### Example 5: File System Handle for Large File Processing

**Use case**: Process large files (logs, exports) without reopening file handles.

```typescript
import { Effect, Layer, Scope, Ref, Chunk } from '@livestore/utils/effect'
import { createWriteStream, createReadStream, WriteStream } from 'fs'
import { open, FileHandle } from 'fs/promises'
import { pipeline } from 'stream/promises'

/**
 * Task global for managing file handles for export operations.
 */
interface ExportFileManager {
  readonly _tag: 'ExportFileManager'
  readonly writeStream: WriteStream
  readonly filePath: string
}

const ExportFileManager = Context.GenericTag<ExportFileManager>('ExportFileManager')

/**
 * Layer providing managed file handle for writing.
 */
const makeExportFileManagerLayer = (filePath: string) =>
  Layer.scoped(
    ExportFileManager,
    Effect.gen(function* () {
      const scope = yield* Scope.Scope

      // Open write stream
      const writeStream = createWriteStream(filePath, {
        flags: 'a', // Append mode
        encoding: 'utf8',
      })

      // Wait for stream to be ready
      yield* Effect.async<void, { _tag: 'FileOpenError'; message: string }>((resume) => {
        writeStream.on('open', () => resume(Effect.void))
        writeStream.on('error', (error) =>
          resume(
            Effect.fail({
              _tag: 'FileOpenError',
              message: error.message,
            }),
          ),
        )
      })

      // Register cleanup
      yield* Scope.addFinalizer(
        scope,
        Effect.async<void>((resume) => {
          writeStream.end(() => resume(Effect.void))
        }),
      )

      return {
        _tag: 'ExportFileManager' as const,
        writeStream,
        filePath,
      }
    }),
  )

/**
 * Helper: Write line to export file
 */
const writeToExportFile = (line: string) =>
  Effect.gen(function* () {
    const { writeStream } = yield* ExportFileManager

    yield* Effect.async<void, { _tag: 'FileWriteError'; message: string }>((resume) => {
      const success = writeStream.write(line + '\n')
      if (success) {
        resume(Effect.void)
      } else {
        writeStream.once('drain', () => resume(Effect.void))
      }
      writeStream.once('error', (error) =>
        resume(
          Effect.fail({
            _tag: 'FileWriteError',
            message: error.message,
          }),
        ),
      )
    })
  })

/**
 * Usage in materializer: Append to audit log file
 */
const materializers = State.SQLite.materializers(events, {
  'v1.UserActionLogged': ({ userId, action, metadata }, { query }) =>
    Effect.gen(function* () {
      const timestamp = new Date().toISOString()

      // Write to audit log file (fire-and-forget)
      yield* writeToExportFile(
        JSON.stringify({
          timestamp,
          userId,
          action,
          metadata,
        }),
      ).pipe(
        Effect.tapErrorCause((cause) =>
          Effect.logError('Audit log write failed', { userId, action, cause }),
        ),
        Effect.forkDaemon,
      )

      // Update database
      return tables.userActions.insert({
        userId,
        action,
        metadata: JSON.stringify(metadata),
        timestamp: Date.now(),
      })
    }).pipe(
      Effect.provide(makeExportFileManagerLayer('/var/log/app/audit.jsonl')),
    ),
})

/**
 * Advanced: Batch file writes for performance
 */
interface BatchFileWriter {
  readonly _tag: 'BatchFileWriter'
  readonly queue: Queue.Queue<string>
  readonly filePath: string
}

const BatchFileWriter = Context.GenericTag<BatchFileWriter>('BatchFileWriter')

const makeBatchFileWriterLayer = (filePath: string, batchSize = 100, flushIntervalMs = 1000) =>
  Layer.scoped(
    BatchFileWriter,
    Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const queue = yield* Queue.bounded<string>(10000)

      const writeStream = createWriteStream(filePath, { flags: 'a', encoding: 'utf8' })

      // Background worker to batch and flush writes
      const worker = Effect.gen(function* () {
        while (true) {
          // Collect batch
          const batch = yield* Queue.takeBetween(queue, 1, batchSize).pipe(
            Effect.timeout(Effect.Duration.millis(flushIntervalMs)),
            Effect.map(Option.getOrElse(() => [] as string[])),
          )

          if (batch.length > 0) {
            yield* Effect.async<void>((resume) => {
              writeStream.write(batch.join('\n') + '\n', (error) => {
                if (error) {
                  resume(
                    Effect.fail({
                      _tag: 'FileWriteError',
                      message: error.message,
                    }),
                  )
                } else {
                  resume(Effect.void)
                }
              })
            })
          }
        }
      }).pipe(
        Effect.tapErrorCause((cause) => Effect.logError('Batch file writer error', { cause })),
        Effect.retry(Schedule.exponential('1 second')),
        Effect.forkScoped,
      )

      yield* worker

      // Cleanup
      yield* Scope.addFinalizer(
        scope,
        Effect.gen(function* () {
          yield* Queue.shutdown(queue)
          yield* Effect.promise(() => new Promise<void>((resolve) => writeStream.end(resolve)))
        }),
      )

      return {
        _tag: 'BatchFileWriter' as const,
        queue,
        filePath,
      }
    }),
  )

const writeToBatchFile = (line: string) =>
  Effect.gen(function* () {
    const { queue } = yield* BatchFileWriter
    yield* Queue.offer(queue, line)
  })
```

**Key patterns**:
- File handles opened once, reused across events
- Automatic flushing and cleanup via Scope
- Batched writes for better performance
- Background worker for async file I/O
- Backpressure handling via bounded queues

---

### Example 6: Cache Client (Redis/Memcached)

**Use case**: Cache expensive computations or external API responses.

```typescript
import { Effect, Layer, Scope, Cache, Duration } from '@livestore/utils/effect'
import { createClient, RedisClientType } from 'redis'

/**
 * Task global for Redis cache client
 */
interface RedisCache {
  readonly _tag: 'RedisCache'
  readonly client: RedisClientType
}

const RedisCache = Context.GenericTag<RedisCache>('RedisCache')

/**
 * Layer providing Redis cache client.
 */
const makeRedisCacheLayer = Layer.scoped(
  RedisCache,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    const client = createClient({
      url: process.env.REDIS_URL ?? 'redis://localhost:6379',
      socket: {
        reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
      },
    })

    // Connect
    yield* Effect.tryPromise({
      try: () => client.connect(),
      catch: (error) => ({
        _tag: 'RedisConnectionError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })

    // Cleanup
    yield* Scope.addFinalizer(
      scope,
      Effect.promise(() => client.quit()),
    )

    return {
      _tag: 'RedisCache' as const,
      client,
    }
  }),
)

/**
 * Helper: Get from cache with fallback
 */
const getCached = <T>(key: string, fallback: Effect.Effect<T, any, any>, ttlSeconds = 3600) =>
  Effect.gen(function* () {
    const { client } = yield* RedisCache

    // Try cache first
    const cached = yield* Effect.tryPromise({
      try: () => client.get(key),
      catch: () => null,
    })

    if (cached !== null) {
      return JSON.parse(cached) as T
    }

    // Cache miss - compute value
    const value = yield* fallback

    // Store in cache (fire-and-forget)
    yield* Effect.tryPromise({
      try: () => client.setEx(key, ttlSeconds, JSON.stringify(value)),
      catch: (error) => error,
    }).pipe(
      Effect.tapErrorCause((cause) => Effect.logWarning('Cache write failed', { key, cause })),
      Effect.forkDaemon,
    )

    return value
  })

/**
 * Helper: Invalidate cache key
 */
const invalidateCache = (key: string | string[]) =>
  Effect.gen(function* () {
    const { client } = yield* RedisCache
    const keys = Array.isArray(key) ? key : [key]
    yield* Effect.tryPromise({
      try: () => client.del(keys),
      catch: (error) => ({
        _tag: 'CacheInvalidationError' as const,
        message: error instanceof Error ? error.message : String(error),
      }),
    })
  })

/**
 * Usage in materializer: Cache user profile enrichment
 */
const materializers = State.SQLite.materializers(events, {
  'v1.UserProfileViewed': ({ userId, viewerId }, { query }) =>
    Effect.gen(function* () {
      // Fetch user with caching
      const userProfile = yield* getCached(
        `user:profile:${userId}`,
        Effect.gen(function* () {
          // This expensive operation only runs on cache miss
          const user = yield* queryExternalAPI(`/users/${userId}`)
          const stats = yield* queryExternalAPI(`/users/${userId}/stats`)
          return { ...user, stats }
        }),
        3600, // 1 hour TTL
      )

      // Record the view
      return tables.profileViews.insert({
        userId,
        viewerId,
        timestamp: Date.now(),
        userProfileSnapshot: JSON.stringify(userProfile),
      })
    }).pipe(Effect.provide(makeRedisCacheLayer)),

  'v1.UserProfileUpdated': ({ userId, updates }, { query }) =>
    Effect.gen(function* () {
      // Update database
      const updateOp = tables.users.update(updates).where({ userId })

      // Invalidate cache (fire-and-forget)
      yield* invalidateCache([`user:profile:${userId}`, `user:stats:${userId}`]).pipe(
        Effect.tapErrorCause((cause) =>
          Effect.logWarning('Cache invalidation failed', { userId, cause }),
        ),
        Effect.forkDaemon,
      )

      return updateOp
    }).pipe(Effect.provide(makeRedisCacheLayer)),
})

/**
 * Advanced: In-memory cache with Redis backup
 */
const makeHybridCacheLayer = Layer.scoped(
  RedisCache,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    // Create in-memory cache (backed by Effect Cache)
    const memoryCache = yield* Cache.make({
      capacity: 1000,
      timeToLive: Duration.minutes(5),
      lookup: (key: string) =>
        Effect.gen(function* () {
          const { client } = yield* RedisCache
          const value = yield* Effect.tryPromise(() => client.get(key))
          return value ? JSON.parse(value) : null
        }),
    })

    const client = createClient({
      url: process.env.REDIS_URL ?? 'redis://localhost:6379',
    })

    yield* Effect.tryPromise(() => client.connect())

    yield* Scope.addFinalizer(
      scope,
      Effect.promise(() => client.quit()),
    )

    return {
      _tag: 'RedisCache' as const,
      client,
      memoryCache,
    }
  }),
)
```

**Key patterns**:
- Redis client created once, connection pooled internally
- Two-tier caching: memory + Redis for optimal performance
- Cache-aside pattern with automatic fallback
- Fire-and-forget cache invalidation
- Graceful degradation if cache fails

---

### Common Patterns Across All Examples

#### 1. **Resource Definition with Lifecycle**

```typescript
const makeResourceLayer = Layer.scoped(
  ResourceTag,
  Effect.gen(function* () {
    const scope = yield* Scope.Scope

    // 1. Initialize resource
    const resource = yield* initializeResource()

    // 2. Test/validate resource
    yield* validateResource(resource)

    // 3. Register cleanup
    yield* Scope.addFinalizer(scope, cleanupResource(resource))

    return { _tag: 'ResourceTag', resource }
  }),
)
```

#### 2. **Accessing Resources in Materializers**

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.SomeEvent': (args, { query }) =>
    Effect.gen(function* () {
      // Access the task global resource
      const { resource } = yield* ResourceTag

      // Use resource for side effects
      yield* useResource(resource)

      // Return database operations
      return tables.foo.insert(args)
    }).pipe(
      Effect.provide(makeResourceLayer),
      Effect.timeout('10 seconds'),
    ),
})
```

#### 3. **Error Handling**

```typescript
Effect.gen(function* () {
  // ...
})
  .pipe(
    // Retry transient failures
    Effect.retry({
      schedule: Schedule.exponential('100 millis').pipe(Schedule.upTo('5 seconds')),
    }),
    // Timeout protection
    Effect.timeout('30 seconds'),
    // Log errors without failing materializer
    Effect.tapErrorCause((cause) => Effect.logError('Operation failed', { cause })),
    // Fire-and-forget for non-critical operations
    Effect.forkDaemon,
  )
```

#### 4. **Cleanup on Shutdown**

```typescript
// Automatic cleanup via Scope finalizers
yield* Scope.addFinalizer(
  scope,
  Effect.gen(function* () {
    yield* closeConnections(resource)
    yield* flushBuffers(resource)
    yield* logShutdown(resource)
  }),
)
```

#### 5. **Sync vs Async Usage**

```typescript
// Synchronous: Block materialization (for critical operations)
const result = yield* syncOperation()

// Asynchronous: Fire-and-forget (for non-critical side effects)
yield* asyncOperation().pipe(
  Effect.tapErrorCause((cause) => Effect.logError('Async operation failed', { cause })),
  Effect.forkDaemon, // Don't block materialization
)
```

---

### Integration with Store Lifecycle

```typescript
import { createStore } from '@livestore/livestore'
import { Layer } from '@livestore/utils/effect'

const store = yield* createStore({
  schema,
  adapter: nodeAdapter,
  // Provide all task global layers
  layer: Layer.mergeAll(
    makeWebhookHttpClientLayer,
    makeAIClientLayer,
    makePostgresPoolLayer,
    makeKafkaProducerLayer,
    makeRedisCacheLayer,
  ),
})

// Resources are initialized when store boots
// Resources are cleaned up when store shuts down
yield* store.shutdown()
```

---

### Benefits Summary

1. **Performance**: Connection pooling eliminates setup overhead per event
2. **Resource Management**: Automatic cleanup prevents leaks
3. **Error Handling**: Graceful degradation when external services fail
4. **Observability**: Centralized logging and monitoring
5. **Testability**: Easy to mock resources via Layer substitution
6. **Type Safety**: Full TypeScript inference for resources
7. **Composability**: Layers can be combined and shared across stores

---

### Comparison to Rama

| Feature | Rama | LiveStore (with Effect) |
|---------|------|------------------------|
| Resource definition | `TaskGlobalObject` interface | `Layer.scoped` with `Context.Tag` |
| Lifecycle hooks | `prepareForTask`, `close` | `Effect.gen` + `Scope.addFinalizer` |
| Async integration | `completable-future>` | `Effect.tryPromise` + `Effect.fork` |
| Thread safety | Manual synchronization | Scope-managed resources |
| Error handling | Try-catch in topology | `Effect` error channel |
| Cleanup | `close()` method | Automatic via `Scope` |
| Testing | Mock implementations | Layer substitution |

**Next Steps**: Implement first-class support for task globals in LiveStore schema, potentially via a `resources` configuration alongside `materializers`.
