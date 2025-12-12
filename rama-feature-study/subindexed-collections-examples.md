# Subindexed Collections - Concrete Examples

This document provides detailed TypeScript/LiveStore examples for implementing subindexed collection patterns inspired by Rama's `{:subindex? true}` feature.

## Core Concept

In Rama, subindexed collections allow storing nested maps/sets where each entry is stored individually rather than as a single serialized blob. This enables:
- **O(1) access** to individual entries within a collection
- **Efficient pagination** without loading entire collections
- **Range queries** on sorted collections
- **Size queries** without full scans
- **Individual entry deletion** without rewriting the parent

Current LiveStore approach uses **separate join tables** for one-to-many relationships. Subindexed collections would provide a **more ergonomic alternative** with better locality and simpler queries.

---

## Example 1: User Transactions (Banking/Finance)

### Problem
Each user can have thousands or millions of transactions. Loading all transactions to find recent ones is inefficient.

### Rama Pattern
```clojure
(declare-pstate mb $$outgoing-transfers
  {Long ; user-id
   (map-schema String ; transfer-id
               (fixed-keys-schema {:to-user-id Long :amt Long :success? Boolean})
               {:subindex? true})})
```

### LiveStore Schema (Current: Join Table Approach)

```typescript
import { State, Events, makeSchema, Schema } from '@livestore/livestore'

// Current approach: Separate tables with foreign keys
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      balance: State.SQLite.integer({ default: 0 }),
    },
  }),

  transactions: State.SQLite.table({
    name: 'transactions',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      userId: State.SQLite.integer({ nullable: false }),
      toUserId: State.SQLite.integer({ nullable: false }),
      amount: State.SQLite.integer({ nullable: false }),
      success: State.SQLite.boolean({ default: true }),
      createdAt: State.SQLite.integer({ nullable: false }), // timestamp for ordering
    },
    indexes: [
      // Critical: Index for efficient user transaction queries
      { name: 'tx_user_time', columns: ['userId', 'createdAt'] },
      { name: 'tx_to_user_time', columns: ['toUserId', 'createdAt'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Subindexed Collections)

```typescript
// HYPOTHETICAL: What subindexed collections might look like in LiveStore
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      balance: State.SQLite.integer({ default: 0 }),
      // Subindexed collection: map from transaction ID to transaction data
      outgoingTransactions: State.SQLite.subindexedMap({
        keySchema: Schema.String, // transaction ID
        valueSchema: Schema.Struct({
          toUserId: Schema.Number,
          amount: Schema.Number,
          success: Schema.Boolean,
          timestamp: Schema.Number,
        }),
        // Optional: Sort by timestamp for efficient range queries
        sortBy: 'timestamp',
        sortOrder: 'desc',
      }),
      incomingTransactions: State.SQLite.subindexedMap({
        keySchema: Schema.String,
        valueSchema: Schema.Struct({
          fromUserId: Schema.Number,
          amount: Schema.Number,
          success: Schema.Boolean,
          timestamp: Schema.Number,
        }),
        sortBy: 'timestamp',
        sortOrder: 'desc',
      }),
    },
  }),
}
```

### Data Insertion (Current Approach)

```typescript
// Event for creating a transaction
const events = {
  transferCreated: Events.synced({
    name: 'v1.TransferCreated',
    schema: Schema.Struct({
      id: Schema.String,
      fromUserId: Schema.Number,
      toUserId: Schema.Number,
      amount: Schema.Number,
      timestamp: Schema.Number,
    }),
  }),
}

// Materializer: Insert one transaction record
const materializers = State.SQLite.materializers(events, {
  'v1.TransferCreated': ({ id, fromUserId, toUserId, amount, timestamp }) => [
    // Check balance (would need server-side validation in real system)
    tables.transactions.insert({ id, userId: fromUserId, toUserId, amount, success: true, createdAt: timestamp }),
    // Update balances
    tables.users.update({ balance: tables.users.column('balance').minus(amount) }).where({ id: fromUserId }),
    tables.users.update({ balance: tables.users.column('balance').plus(amount) }).where({ id: toUserId }),
  ],
})
```

### Data Insertion (Hypothetical: Subindexed)

```typescript
// HYPOTHETICAL: Subindexed approach
const materializers = State.SQLite.materializers(events, {
  'v1.TransferCreated': ({ id, fromUserId, toUserId, amount, timestamp }) => [
    // Add single entry to sender's outgoing transactions map
    tables.users.updateSubindexedMap('outgoingTransactions')
      .set(id, { toUserId, amount, success: true, timestamp })
      .where({ id: fromUserId }),

    // Add single entry to receiver's incoming transactions map
    tables.users.updateSubindexedMap('incomingTransactions')
      .set(id, { fromUserId, amount, success: true, timestamp })
      .where({ id: toUserId }),

    // Update balances
    tables.users.update({ balance: tables.users.column('balance').minus(amount) }).where({ id: fromUserId }),
    tables.users.update({ balance: tables.users.column('balance').plus(amount) }).where({ id: toUserId }),
  ],
})
```

### Pagination Queries (Current Approach)

```typescript
// Query recent transactions for a user with cursor-based pagination
function getRecentTransactions(store: Store, userId: number, cursor?: { createdAt: number, id: string }, limit = 20) {
  let query = tables.transactions
    .select()
    .where({ userId })
    .orderBy('createdAt', 'desc')
    .limit(limit)

  if (cursor) {
    // Cursor-based: Get transactions older than cursor
    query = query.where(
      tables.transactions.column('createdAt').lt(cursor.createdAt)
        .or(
          tables.transactions.column('createdAt').eq(cursor.createdAt)
            .and(tables.transactions.column('id').gt(cursor.id))
        )
    )
  }

  return store.query(query)
}
```

### Pagination Queries (Hypothetical: Subindexed)

```typescript
// HYPOTHETICAL: Direct range query on subindexed map
function getRecentTransactions(store: Store, userId: number, cursor?: number, limit = 20) {
  return store.query(
    tables.users
      .selectSubindexedMap('outgoingTransactions')
      .where({ id: userId })
      .rangeFrom(cursor ?? Infinity) // Start from cursor timestamp (or latest)
      .limit(limit)
  )
}

// Returns: Array<{ key: string, value: { toUserId, amount, success, timestamp } }>
```

### Size Queries (Current vs Hypothetical)

```typescript
// Current: COUNT query over transactions table
function getTransactionCount(store: Store, userId: number) {
  return store.query(
    tables.transactions.count().where({ userId })
  )[0].count
}

// HYPOTHETICAL: O(1) size query on subindexed map metadata
function getTransactionCount(store: Store, userId: number) {
  return store.query(
    tables.users
      .selectSubindexedMapSize('outgoingTransactions')
      .where({ id: userId })
  )[0].size
}
```

### Individual Entry Deletion

```typescript
// Current: Delete from transactions table
function deleteTransaction(store: Store, transactionId: string) {
  store.commit(events.transactionDeleted({ id: transactionId }))
}

// Materializer
const materializers = {
  'v1.TransactionDeleted': ({ id }) =>
    tables.transactions.delete().where({ id })
}

// HYPOTHETICAL: Delete single entry from subindexed map
const materializers = {
  'v1.TransactionDeleted': ({ id, userId }) =>
    tables.users.updateSubindexedMap('outgoingTransactions')
      .delete(id)
      .where({ id: userId })
}
```

---

## Example 2: Project Tasks with Status Filtering

### Problem
Projects can have thousands of tasks. Need to efficiently filter by status and paginate without loading all tasks.

### LiveStore Schema (Current)

```typescript
export const tables = {
  projects: State.SQLite.table({
    name: 'projects',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      name: State.SQLite.text({ default: '' }),
    },
  }),

  tasks: State.SQLite.table({
    name: 'tasks',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      projectId: State.SQLite.integer({ nullable: false }),
      title: State.SQLite.text({ default: '' }),
      status: State.SQLite.text({ default: 'todo' }), // 'todo' | 'in_progress' | 'done'
      priority: State.SQLite.integer({ default: 0 }),
      createdAt: State.SQLite.integer({ nullable: false }),
    },
    indexes: [
      // Multi-column index for filtering and sorting
      { name: 'tasks_project_status_priority', columns: ['projectId', 'status', 'priority'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Multiple Subindexed Maps)

```typescript
// HYPOTHETICAL: One subindexed map per status for efficient filtering
export const tables = {
  projects: State.SQLite.table({
    name: 'projects',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      name: State.SQLite.text({ default: '' }),

      // Separate subindexed maps per status
      todoTasks: State.SQLite.subindexedMap({
        keySchema: Schema.String, // task ID
        valueSchema: Schema.Struct({
          title: Schema.String,
          priority: Schema.Number,
          createdAt: Schema.Number,
        }),
        sortBy: 'priority', // Sort by priority within each status
        sortOrder: 'desc',
      }),
      inProgressTasks: State.SQLite.subindexedMap({
        keySchema: Schema.String,
        valueSchema: Schema.Struct({
          title: Schema.String,
          priority: Schema.Number,
          createdAt: Schema.Number,
        }),
        sortBy: 'priority',
        sortOrder: 'desc',
      }),
      doneTasks: State.SQLite.subindexedMap({
        keySchema: Schema.String,
        valueSchema: Schema.Struct({
          title: Schema.String,
          priority: Schema.Number,
          createdAt: Schema.Number,
        }),
        sortBy: 'createdAt', // Completed tasks sorted by time
        sortOrder: 'desc',
      }),
    },
  }),
}
```

### Materializers with Status Transitions

```typescript
// Current approach
const events = {
  taskCreated: Events.synced({
    name: 'v1.TaskCreated',
    schema: Schema.Struct({
      id: Schema.String,
      projectId: Schema.Number,
      title: Schema.String,
      priority: Schema.Number,
    }),
  }),
  taskStatusChanged: Events.synced({
    name: 'v1.TaskStatusChanged',
    schema: Schema.Struct({
      id: Schema.String,
      newStatus: Schema.Literal('todo', 'in_progress', 'done'),
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.TaskCreated': ({ id, projectId, title, priority }) =>
    tables.tasks.insert({ id, projectId, title, priority, status: 'todo', createdAt: Date.now() }),

  'v1.TaskStatusChanged': ({ id, newStatus }) =>
    tables.tasks.update({ status: newStatus }).where({ id }),
})

// HYPOTHETICAL: Subindexed approach with status bucket moves
const materializers = State.SQLite.materializers(events, {
  'v1.TaskCreated': ({ id, projectId, title, priority }) =>
    tables.projects.updateSubindexedMap('todoTasks')
      .set(id, { title, priority, createdAt: Date.now() })
      .where({ id: projectId }),

  // Status change requires removing from one bucket and adding to another
  'v1.TaskStatusChanged': ({ id, projectId, newStatus, taskData }) => {
    // This would need to know current status to remove from correct bucket
    // Alternative: Have event include full context or query current state
    const moves = []

    // Remove from all buckets (or track current status)
    moves.push(
      tables.projects.updateSubindexedMap('todoTasks').delete(id).where({ id: projectId }),
      tables.projects.updateSubindexedMap('inProgressTasks').delete(id).where({ id: projectId }),
      tables.projects.updateSubindexedMap('doneTasks').delete(id).where({ id: projectId })
    )

    // Add to new bucket
    const bucket = newStatus === 'todo' ? 'todoTasks'
      : newStatus === 'in_progress' ? 'inProgressTasks'
      : 'doneTasks'

    moves.push(
      tables.projects.updateSubindexedMap(bucket)
        .set(id, taskData)
        .where({ id: projectId })
    )

    return moves
  },
})
```

### Queries with Status Filtering

```typescript
// Current: Filter by status in WHERE clause
function getTasksByStatus(store: Store, projectId: number, status: 'todo' | 'in_progress' | 'done', limit = 50) {
  return store.query(
    tables.tasks
      .select()
      .where({ projectId, status })
      .orderBy('priority', 'desc')
      .limit(limit)
  )
}

// HYPOTHETICAL: Direct access to status bucket
function getTasksByStatus(store: Store, projectId: number, status: 'todo' | 'in_progress' | 'done', limit = 50) {
  const bucket = status === 'todo' ? 'todoTasks'
    : status === 'in_progress' ? 'inProgressTasks'
    : 'doneTasks'

  return store.query(
    tables.projects
      .selectSubindexedMap(bucket)
      .where({ id: projectId })
      .limit(limit)
  )
}

// Benefits: No index scan, direct access to the status subset
```

---

## Example 3: Social Graph (Followers/Following)

### Problem
Users can have millions of followers. Need to:
- Paginate follower lists
- Check if user A follows user B (O(1) lookup)
- Get follower count without full scan

### Rama Pattern (Twitter Clone)

```java
stream.pstate("$$partitionedFollowers",
  PState.mapSchema(Long.class, // followee ID
    PState.mapSchema(Long.class, Follower.class).subindexed()) // follower ID -> Follower data
);
```

### LiveStore Schema (Current)

```typescript
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),
      followerCount: State.SQLite.integer({ default: 0 }), // Denormalized for performance
      followingCount: State.SQLite.integer({ default: 0 }),
    },
  }),

  follows: State.SQLite.table({
    name: 'follows',
    columns: {
      followerId: State.SQLite.integer({ nullable: false }),
      followeeId: State.SQLite.integer({ nullable: false }),
      followedAt: State.SQLite.integer({ nullable: false }),
      notificationsEnabled: State.SQLite.boolean({ default: false }),
    },
    indexes: [
      // Composite primary key for uniqueness
      { name: 'follows_pk', columns: ['followerId', 'followeeId'], unique: true },
      // Index for reverse lookups (who follows this user)
      { name: 'follows_followee_time', columns: ['followeeId', 'followedAt'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Subindexed)

```typescript
// HYPOTHETICAL: Followers/following as subindexed sets
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),

      // Map from follower ID to follow metadata
      followers: State.SQLite.subindexedMap({
        keySchema: Schema.Number, // follower user ID
        valueSchema: Schema.Struct({
          followedAt: Schema.Number,
          notificationsEnabled: Schema.Boolean,
        }),
        sortBy: 'followedAt',
        sortOrder: 'desc',
      }),

      // Map from followee ID to follow metadata
      following: State.SQLite.subindexedMap({
        keySchema: Schema.Number, // followee user ID
        valueSchema: Schema.Struct({
          followedAt: Schema.Number,
          notificationsEnabled: Schema.Boolean,
        }),
        sortBy: 'followedAt',
        sortOrder: 'desc',
      }),
    },
  }),
}
```

### Follow/Unfollow Operations

```typescript
const events = {
  userFollowed: Events.synced({
    name: 'v1.UserFollowed',
    schema: Schema.Struct({
      followerId: Schema.Number,
      followeeId: Schema.Number,
      timestamp: Schema.Number,
    }),
  }),
  userUnfollowed: Events.synced({
    name: 'v1.UserUnfollowed',
    schema: Schema.Struct({
      followerId: Schema.Number,
      followeeId: Schema.Number,
    }),
  }),
}

// Current approach
const materializers = State.SQLite.materializers(events, {
  'v1.UserFollowed': ({ followerId, followeeId, timestamp }) => [
    tables.follows.insert({ followerId, followeeId, followedAt: timestamp, notificationsEnabled: false }),
    tables.users.update({ followerCount: tables.users.column('followerCount').plus(1) }).where({ id: followeeId }),
    tables.users.update({ followingCount: tables.users.column('followingCount').plus(1) }).where({ id: followerId }),
  ],

  'v1.UserUnfollowed': ({ followerId, followeeId }) => [
    tables.follows.delete().where({ followerId, followeeId }),
    tables.users.update({ followerCount: tables.users.column('followerCount').minus(1) }).where({ id: followeeId }),
    tables.users.update({ followingCount: tables.users.column('followingCount').minus(1) }).where({ id: followerId }),
  ],
})

// HYPOTHETICAL: Subindexed approach (no denormalized counts needed)
const materializers = State.SQLite.materializers(events, {
  'v1.UserFollowed': ({ followerId, followeeId, timestamp }) => [
    // Add to followee's followers map
    tables.users.updateSubindexedMap('followers')
      .set(followerId, { followedAt: timestamp, notificationsEnabled: false })
      .where({ id: followeeId }),

    // Add to follower's following map
    tables.users.updateSubindexedMap('following')
      .set(followeeId, { followedAt: timestamp, notificationsEnabled: false })
      .where({ id: followerId }),
  ],

  'v1.UserUnfollowed': ({ followerId, followeeId }) => [
    tables.users.updateSubindexedMap('followers').delete(followerId).where({ id: followeeId }),
    tables.users.updateSubindexedMap('following').delete(followeeId).where({ id: followerId }),
  ],
})
```

### Relationship Checks and Counts

```typescript
// Current: Check if A follows B
function isFollowing(store: Store, followerId: number, followeeId: number): boolean {
  const result = store.query(
    tables.follows.count().where({ followerId, followeeId })
  )
  return result[0].count > 0
}

// HYPOTHETICAL: O(1) key existence check
function isFollowing(store: Store, followerId: number, followeeId: number): boolean {
  return store.query(
    tables.users.selectSubindexedMap('following')
      .where({ id: followerId })
      .hasKey(followeeId)
  )[0]
}

// Current: Get follower count (using denormalized counter)
function getFollowerCount(store: Store, userId: number): number {
  return store.query(tables.users.select('followerCount').where({ id: userId }))[0].followerCount
}

// HYPOTHETICAL: O(1) size query
function getFollowerCount(store: Store, userId: number): number {
  return store.query(
    tables.users.selectSubindexedMapSize('followers').where({ id: userId })
  )[0].size
}
```

### Paginated Follower List

```typescript
// Current: Join query with pagination
function getFollowers(store: Store, userId: number, cursor?: { followedAt: number, followerId: number }, limit = 50) {
  let query = tables.follows
    .select(['followerId', 'followedAt', 'notificationsEnabled'])
    .where({ followeeId: userId })
    .orderBy('followedAt', 'desc')
    .limit(limit)

  if (cursor) {
    query = query.where(
      tables.follows.column('followedAt').lt(cursor.followedAt)
        .or(
          tables.follows.column('followedAt').eq(cursor.followedAt)
            .and(tables.follows.column('followerId').gt(cursor.followerId))
        )
    )
  }

  return store.query(query)
}

// HYPOTHETICAL: Direct range query on subindexed map
function getFollowers(store: Store, userId: number, cursor?: number, limit = 50) {
  return store.query(
    tables.users.selectSubindexedMap('followers')
      .where({ id: userId })
      .rangeFrom(cursor ?? Infinity)
      .limit(limit)
  )
}
// Returns: [{ key: followerId, value: { followedAt, notificationsEnabled } }, ...]
```

---

## Example 4: Notification Timeline

### Problem
Each user receives many notifications. Need to:
- Fetch recent N notifications
- Mark individual notifications as read
- Clear all notifications
- Maintain order by timestamp

### Rama Pattern (Twitter Clone)

```java
// Twitter uses KeyToFixedItemsPStateGroup for bounded timelines
KeyToFixedItemsPStateGroup accountIdToNotificationsTimeline =
  new KeyToFixedItemsPStateGroup("$$accountIdToNotificationsTimeline", 800, Long.class, Notification.class);
```

### LiveStore Schema (Current)

```typescript
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),
    },
  }),

  notifications: State.SQLite.table({
    name: 'notifications',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      userId: State.SQLite.integer({ nullable: false }),
      type: State.SQLite.text({ nullable: false }), // 'mention', 'follow', 'like', etc.
      // Store notification data as JSON
      data: State.SQLite.text({
        nullable: false,
        schema: Schema.parseJson(Schema.Struct({
          actorId: Schema.Number,
          targetId: Schema.optional(Schema.Number),
          text: Schema.optional(Schema.String),
        })),
      }),
      read: State.SQLite.boolean({ default: false }),
      createdAt: State.SQLite.integer({ nullable: false }),
    },
    indexes: [
      { name: 'notif_user_time', columns: ['userId', 'createdAt'] },
      { name: 'notif_user_read', columns: ['userId', 'read'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Subindexed with TTL)

```typescript
// HYPOTHETICAL: Bounded subindexed map with automatic eviction
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),

      notifications: State.SQLite.subindexedMap({
        keySchema: Schema.String, // notification ID
        valueSchema: Schema.Struct({
          type: Schema.Literal('mention', 'follow', 'like', 'boost', 'reply'),
          actorId: Schema.Number,
          targetId: Schema.Number.pipe(Schema.optional),
          text: Schema.String.pipe(Schema.optional),
          read: Schema.Boolean,
          createdAt: Schema.Number,
        }),
        sortBy: 'createdAt',
        sortOrder: 'desc',
        // Optional: Automatic eviction (keep last N entries)
        maxSize: 800,
      }),
    },
  }),
}
```

### Creating Notifications

```typescript
const events = {
  notificationCreated: Events.synced({
    name: 'v1.NotificationCreated',
    schema: Schema.Struct({
      id: Schema.String,
      userId: Schema.Number,
      type: Schema.Literal('mention', 'follow', 'like', 'boost', 'reply'),
      actorId: Schema.Number,
      targetId: Schema.Number.pipe(Schema.optional),
      text: Schema.String.pipe(Schema.optional),
      createdAt: Schema.Number,
    }),
  }),
}

// Current
const materializers = State.SQLite.materializers(events, {
  'v1.NotificationCreated': ({ id, userId, type, actorId, targetId, text, createdAt }) =>
    tables.notifications.insert({
      id,
      userId,
      type,
      data: JSON.stringify({ actorId, targetId, text }),
      read: false,
      createdAt,
    }),
})

// HYPOTHETICAL
const materializers = State.SQLite.materializers(events, {
  'v1.NotificationCreated': ({ id, userId, type, actorId, targetId, text, createdAt }) =>
    tables.users.updateSubindexedMap('notifications')
      .set(id, { type, actorId, targetId, text, read: false, createdAt })
      .where({ id: userId }),
  // If maxSize is 800, oldest notification is automatically evicted
})
```

### Fetching Notifications

```typescript
// Current: Standard pagination query
function getNotifications(store: Store, userId: number, limit = 20, cursor?: number) {
  let query = tables.notifications
    .select()
    .where({ userId })
    .orderBy('createdAt', 'desc')
    .limit(limit)

  if (cursor) {
    query = query.where(tables.notifications.column('createdAt').lt(cursor))
  }

  return store.query(query)
}

// HYPOTHETICAL: Range query on subindexed timeline
function getNotifications(store: Store, userId: number, limit = 20, cursor?: number) {
  return store.query(
    tables.users.selectSubindexedMap('notifications')
      .where({ id: userId })
      .rangeFrom(cursor ?? Infinity)
      .limit(limit)
  )
}
```

### Marking as Read

```typescript
// Current
const events = {
  notificationRead: Events.synced({
    name: 'v1.NotificationRead',
    schema: Schema.Struct({ id: Schema.String }),
  }),
}

const materializers = {
  'v1.NotificationRead': ({ id }) =>
    tables.notifications.update({ read: true }).where({ id }),
}

// HYPOTHETICAL: Update value in subindexed map
const materializers = {
  'v1.NotificationRead': ({ id, userId }) =>
    tables.users.updateSubindexedMap('notifications')
      .update(id, (notif) => ({ ...notif, read: true }))
      .where({ id: userId }),
}
```

### Clear All Notifications

```typescript
// Current
const materializers = {
  'v1.NotificationsCleared': ({ userId }) =>
    tables.notifications.delete().where({ userId }),
}

// HYPOTHETICAL: Clear entire subindexed map
const materializers = {
  'v1.NotificationsCleared': ({ userId }) =>
    tables.users.updateSubindexedMap('notifications').clear().where({ id: userId }),
}
```

---

## Example 5: Comment Threads with Nested Replies

### Problem
Posts can have thousands of comments. Comments can have nested replies forming a tree structure. Need to:
- Load top-level comments with pagination
- Load replies for a specific comment
- Count total comments/replies

### LiveStore Schema (Current: Adjacency List)

```typescript
export const tables = {
  posts: State.SQLite.table({
    name: 'posts',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      authorId: State.SQLite.integer({ nullable: false }),
      text: State.SQLite.text({ default: '' }),
      commentCount: State.SQLite.integer({ default: 0 }), // Denormalized
    },
  }),

  comments: State.SQLite.table({
    name: 'comments',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      postId: State.SQLite.integer({ nullable: false }),
      parentCommentId: State.SQLite.text({ nullable: true }), // null = top-level
      authorId: State.SQLite.integer({ nullable: false }),
      text: State.SQLite.text({ default: '' }),
      replyCount: State.SQLite.integer({ default: 0 }), // Denormalized
      createdAt: State.SQLite.integer({ nullable: false }),
    },
    indexes: [
      // Top-level comments for a post
      { name: 'comments_post_parent_time', columns: ['postId', 'parentCommentId', 'createdAt'] },
      // Replies to a specific comment
      { name: 'comments_parent_time', columns: ['parentCommentId', 'createdAt'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Nested Subindexed Maps)

```typescript
// HYPOTHETICAL: Two-level subindexed structure
export const tables = {
  posts: State.SQLite.table({
    name: 'posts',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      authorId: State.SQLite.integer({ nullable: false }),
      text: State.SQLite.text({ default: '' }),

      // Top-level comments
      comments: State.SQLite.subindexedMap({
        keySchema: Schema.String, // comment ID
        valueSchema: Schema.Struct({
          authorId: Schema.Number,
          text: Schema.String,
          createdAt: Schema.Number,
          // Nested subindexed map for replies
          replies: Schema.SubindexedMap({
            keySchema: Schema.String, // reply ID
            valueSchema: Schema.Struct({
              authorId: Schema.Number,
              text: Schema.String,
              createdAt: Schema.Number,
            }),
            sortBy: 'createdAt',
            sortOrder: 'asc', // Chronological for replies
          }),
        }),
        sortBy: 'createdAt',
        sortOrder: 'desc', // Reverse chronological for top-level
      }),
    },
  }),
}
```

### Adding Comments/Replies

```typescript
const events = {
  commentAdded: Events.synced({
    name: 'v1.CommentAdded',
    schema: Schema.Struct({
      id: Schema.String,
      postId: Schema.Number,
      parentCommentId: Schema.String.pipe(Schema.optional),
      authorId: Schema.Number,
      text: Schema.String,
      createdAt: Schema.Number,
    }),
  }),
}

// Current
const materializers = State.SQLite.materializers(events, {
  'v1.CommentAdded': ({ id, postId, parentCommentId, authorId, text, createdAt }) => {
    const ops = [
      tables.comments.insert({ id, postId, parentCommentId: parentCommentId ?? null, authorId, text, replyCount: 0, createdAt }),
    ]

    if (parentCommentId) {
      // Reply to a comment
      ops.push(
        tables.comments.update({ replyCount: tables.comments.column('replyCount').plus(1) })
          .where({ id: parentCommentId })
      )
    } else {
      // Top-level comment
      ops.push(
        tables.posts.update({ commentCount: tables.posts.column('commentCount').plus(1) })
          .where({ id: postId })
      )
    }

    return ops
  },
})

// HYPOTHETICAL
const materializers = State.SQLite.materializers(events, {
  'v1.CommentAdded': ({ id, postId, parentCommentId, authorId, text, createdAt }) => {
    if (parentCommentId) {
      // Add reply to parent comment's replies map
      return tables.posts.updateSubindexedMap('comments')
        .updateValue(parentCommentId, (comment) => ({
          ...comment,
          replies: comment.replies.set(id, { authorId, text, createdAt }),
        }))
        .where({ id: postId })
    } else {
      // Add top-level comment
      return tables.posts.updateSubindexedMap('comments')
        .set(id, {
          authorId,
          text,
          createdAt,
          replies: new Map(), // Empty replies map
        })
        .where({ id: postId })
    }
  },
})
```

### Querying Comments

```typescript
// Current: Get top-level comments
function getTopLevelComments(store: Store, postId: number, limit = 20, cursor?: number) {
  let query = tables.comments
    .select()
    .where({ postId, parentCommentId: null })
    .orderBy('createdAt', 'desc')
    .limit(limit)

  if (cursor) {
    query = query.where(tables.comments.column('createdAt').lt(cursor))
  }

  return store.query(query)
}

// Current: Get replies to a comment
function getReplies(store: Store, commentId: string, limit = 10) {
  return store.query(
    tables.comments
      .select()
      .where({ parentCommentId: commentId })
      .orderBy('createdAt', 'asc')
      .limit(limit)
  )
}

// HYPOTHETICAL: Direct access to subindexed structures
function getTopLevelComments(store: Store, postId: number, limit = 20, cursor?: number) {
  return store.query(
    tables.posts.selectSubindexedMap('comments')
      .where({ id: postId })
      .rangeFrom(cursor ?? Infinity)
      .limit(limit)
  )
}

function getReplies(store: Store, postId: number, commentId: string, limit = 10) {
  return store.query(
    tables.posts.selectSubindexedMap('comments')
      .where({ id: postId })
      .getKey(commentId)
      .selectSubindexedMap('replies')
      .limit(limit)
  )
}
```

### Comment Counts

```typescript
// Current: Uses denormalized counters
function getCommentCount(store: Store, postId: number) {
  return store.query(tables.posts.select('commentCount').where({ id: postId }))[0].commentCount
}

function getReplyCount(store: Store, commentId: string) {
  return store.query(tables.comments.select('replyCount').where({ id: commentId }))[0].replyCount
}

// HYPOTHETICAL: O(1) size queries on subindexed maps
function getCommentCount(store: Store, postId: number) {
  return store.query(
    tables.posts.selectSubindexedMapSize('comments').where({ id: postId })
  )[0].size
}

function getReplyCount(store: Store, postId: number, commentId: string) {
  return store.query(
    tables.posts.selectSubindexedMap('comments')
      .where({ id: postId })
      .getKey(commentId)
      .selectSubindexedMapSize('replies')
  )[0].size
}
```

---

## Example 6: Activity Feed with Time-Based Queries

### Problem
Users need activity feeds showing recent actions. Requirements:
- Fetch activities in time ranges (last 24h, last week, etc.)
- Different activity types (post, comment, like, follow)
- Efficient pagination with time-based cursors

### LiveStore Schema (Current)

```typescript
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),
    },
  }),

  activities: State.SQLite.table({
    name: 'activities',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      userId: State.SQLite.integer({ nullable: false }),
      type: State.SQLite.text({ nullable: false }),
      // JSON data for activity details
      data: State.SQLite.text({
        nullable: false,
        schema: Schema.parseJson(Schema.Struct({
          targetId: Schema.Number.pipe(Schema.optional),
          targetType: Schema.String.pipe(Schema.optional),
          metadata: Schema.Record(Schema.String, Schema.Unknown).pipe(Schema.optional),
        })),
      }),
      timestamp: State.SQLite.integer({ nullable: false }),
    },
    indexes: [
      { name: 'activities_user_time', columns: ['userId', 'timestamp'] },
      { name: 'activities_user_type_time', columns: ['userId', 'type', 'timestamp'] },
    ],
  }),
}
```

### LiveStore Schema (Hypothetical: Subindexed with Time Bucketing)

```typescript
// HYPOTHETICAL: Sorted subindexed map with timestamp keys
export const tables = {
  users: State.SQLite.table({
    name: 'users',
    columns: {
      id: State.SQLite.integer({ primaryKey: true }),
      username: State.SQLite.text({ unique: true }),

      // Activities sorted by timestamp (descending)
      activityFeed: State.SQLite.subindexedMap({
        keySchema: Schema.Number, // timestamp (for natural sorting)
        // Use composite key: `${timestamp}-${activityId}` for uniqueness
        valueSchema: Schema.Struct({
          id: Schema.String, // activity ID
          type: Schema.Literal('post', 'comment', 'like', 'follow', 'boost'),
          targetId: Schema.Number.pipe(Schema.optional),
          targetType: Schema.String.pipe(Schema.optional),
          metadata: Schema.Record(Schema.String, Schema.Unknown).pipe(Schema.optional),
        }),
        // Sorted by key (timestamp) automatically
        sortBy: 'key',
        sortOrder: 'desc',
        // Optional: Auto-evict old activities
        maxSize: 5000,
      }),
    },
  }),
}
```

### Recording Activities

```typescript
const events = {
  activityRecorded: Events.synced({
    name: 'v1.ActivityRecorded',
    schema: Schema.Struct({
      id: Schema.String,
      userId: Schema.Number,
      type: Schema.Literal('post', 'comment', 'like', 'follow', 'boost'),
      targetId: Schema.Number.pipe(Schema.optional),
      targetType: Schema.String.pipe(Schema.optional),
      metadata: Schema.Record(Schema.String, Schema.Unknown).pipe(Schema.optional),
      timestamp: Schema.Number,
    }),
  }),
}

// Current
const materializers = State.SQLite.materializers(events, {
  'v1.ActivityRecorded': ({ id, userId, type, targetId, targetType, metadata, timestamp }) =>
    tables.activities.insert({
      id,
      userId,
      type,
      data: JSON.stringify({ targetId, targetType, metadata }),
      timestamp,
    }),
})

// HYPOTHETICAL
const materializers = State.SQLite.materializers(events, {
  'v1.ActivityRecorded': ({ id, userId, type, targetId, targetType, metadata, timestamp }) =>
    tables.users.updateSubindexedMap('activityFeed')
      .set(timestamp, { id, type, targetId, targetType, metadata })
      .where({ id: userId }),
})
```

### Time Range Queries

```typescript
// Current: Range filter on timestamp
function getRecentActivities(store: Store, userId: number, since: number, limit = 50) {
  return store.query(
    tables.activities
      .select()
      .where({ userId })
      .where(tables.activities.column('timestamp').gte(since))
      .orderBy('timestamp', 'desc')
      .limit(limit)
  )
}

function getActivitiesBetween(store: Store, userId: number, startTime: number, endTime: number) {
  return store.query(
    tables.activities
      .select()
      .where({ userId })
      .where(
        tables.activities.column('timestamp').gte(startTime)
          .and(tables.activities.column('timestamp').lte(endTime))
      )
      .orderBy('timestamp', 'desc')
  )
}

// HYPOTHETICAL: Native range query on sorted subindexed map
function getRecentActivities(store: Store, userId: number, since: number, limit = 50) {
  return store.query(
    tables.users.selectSubindexedMap('activityFeed')
      .where({ id: userId })
      .rangeFrom(Infinity) // Start from most recent
      .rangeTo(since)       // Stop at 'since' timestamp
      .limit(limit)
  )
}

function getActivitiesBetween(store: Store, userId: number, startTime: number, endTime: number) {
  return store.query(
    tables.users.selectSubindexedMap('activityFeed')
      .where({ id: userId })
      .rangeFrom(endTime)   // Start from end
      .rangeTo(startTime)   // Stop at start
  )
}
```

### Activity Type Filtering

```typescript
// Current: Multi-column index for type filtering
function getActivitiesByType(
  store: Store,
  userId: number,
  type: 'post' | 'comment' | 'like' | 'follow' | 'boost',
  limit = 50
) {
  return store.query(
    tables.activities
      .select()
      .where({ userId, type })
      .orderBy('timestamp', 'desc')
      .limit(limit)
  )
}

// HYPOTHETICAL: Could use separate subindexed maps per type (like Example 2)
// Or filter in application layer after range query
function getActivitiesByType(
  store: Store,
  userId: number,
  type: 'post' | 'comment' | 'like' | 'follow' | 'boost',
  limit = 50
) {
  // Option 1: Filter in application (if type diversity is high)
  const activities = store.query(
    tables.users.selectSubindexedMap('activityFeed')
      .where({ id: userId })
      .limit(limit * 2) // Overfetch to account for filtering
  )

  return activities.filter(a => a.value.type === type).slice(0, limit)

  // Option 2: Separate subindexed maps per type (better for large datasets)
  // Similar to Example 2 with project tasks
}
```

---

## Key Patterns Summary

### When to Use Subindexed Collections

1. **One-to-many relationships with large cardinality**
   - Users with thousands of transactions, followers, notifications
   - Avoid loading entire collections into memory

2. **Frequent pagination needs**
   - Cursor-based pagination becomes trivial with sorted maps
   - Range queries are natural operations

3. **Individual entry operations**
   - Adding/removing single items without rewriting parent
   - Checking existence of specific keys

4. **Size queries**
   - Get collection size without scanning all entries
   - Useful for badges, counters, UI displays

5. **Sorted access patterns**
   - Timeline-style data (newest first)
   - Priority queues, leaderboards
   - Time-range queries

### Design Considerations

**Advantages:**
- Locality: Related data stored together (single table)
- Simpler queries: No joins needed for parent-child access
- Efficient partial access: Query only needed entries
- Natural pagination: Built-in cursor support
- O(1) operations: Size, key existence, single entry access

**Trade-offs:**
- Schema complexity: Nested structures need careful design
- Migration complexity: Changing subindexed structure harder than adding columns
- Query flexibility: Less flexible than SQL joins for ad-hoc queries
- Index overhead: Each subindexed map needs internal indexing structures

**Current LiveStore Alternative:**
- Separate join tables with composite indexes work well
- More SQL-native, better for ad-hoc queries
- Established patterns, familiar to developers
- Good enough for most use cases

**When Subindexed Would Shine:**
- Very high cardinality relationships (millions of entries per parent)
- Strong access patterns (always query by parent ID)
- Timeline/feed architectures (Twitter, Instagram, etc.)
- Real-time apps where denormalization helps locality
- Scenarios where Rama's approach has proven valuable

---

## Implementation Notes

If implementing subindexed collections in LiveStore, consider:

1. **Storage Strategy**
   - Each subindexed map → separate SQLite table
   - Table name: `{parent_table}_{column_name}_{parent_id}`
   - Or single table with `(parent_id, entry_key)` composite key

2. **Query Builder Extensions**
   ```typescript
   // Hypothetical API surface
   table.selectSubindexedMap(column)
     .where({ id: parentId })
     .rangeFrom(cursor)
     .rangeTo(endCursor)
     .limit(n)
     .hasKey(key)
     .getKey(key)

   table.updateSubindexedMap(column)
     .set(key, value)
     .delete(key)
     .clear()
     .update(key, updateFn)

   table.selectSubindexedMapSize(column)
   ```

3. **Materializer Support**
   - Event handlers can atomically update subindexed maps
   - Support for cross-partition updates (like Rama's `|hash`)

4. **Schema Migrations**
   - How to evolve subindexed map schemas?
   - Backfilling strategies for adding new subindexed columns

5. **Performance Tuning**
   - Automatic cleanup of evicted entries (maxSize)
   - Compaction strategies for deleted entries
   - Index maintenance for sorted maps

6. **Type Safety**
   - Full TypeScript inference for nested structures
   - Schema validation with Effect Schema
   - Query builder type safety for subindexed operations
