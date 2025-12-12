# Ack Returns for Event Commits - Concrete Examples

This document provides concrete TypeScript/LiveStore examples showing how ack return patterns could work, inspired by Rama's `ack-return>` operator.

## Overview

In Rama, `ack-return>` allows stream topologies to synchronously return values to the client that triggered an event append. This is useful for:

- Returning server-generated IDs after entity creation
- Providing immediate validation feedback
- Returning computed/derived values (slugs, hashes, etc.)
- Conflict detection and resolution feedback
- Returning fully-materialized entities with defaults applied

## Example 1: Generated ID Return

**Use Case:** Client creates a post but the ID is server-generated (e.g., using nanoid on the server). Client needs the ID immediately for optimistic UI updates.

### Event Definition

```typescript
import { Events, Schema } from '@livestore/livestore'

export const events = {
  postCreated: Events.synced({
    name: 'v1.PostCreated',
    schema: Schema.Struct({
      // ID is optional - if not provided, server generates one
      id: Schema.optional(Schema.String),
      title: Schema.String,
      content: Schema.String,
      authorId: Schema.String,
    }),
  }),
}
```

### Materializer with Ack Return

```typescript
import { State, defineMaterializer } from '@livestore/livestore'
import { nanoid } from '@livestore/utils/nanoid'

const materializers = State.SQLite.materializers(events, {
  'v1.PostCreated': defineMaterializer(
    events.postCreated,
    ({ id, title, content, authorId }, { ackReturn }) => {
      // Generate ID if not provided by client
      const postId = id ?? nanoid()

      // Return the ID to the client
      ackReturn({ postId })

      return posts.insert({
        id: postId,
        title,
        content,
        authorId,
        createdAt: new Date(),
      })
    }
  ),
})
```

### Client Usage

```typescript
import { useStore } from '@livestore/react'

function CreatePostForm() {
  const { store } = useStore()
  const [title, setTitle] = useState('')
  const [content, setContent] = useState('')

  const handleSubmit = async () => {
    // Await the ack return value
    const result = await store.commit(
      events.postCreated({
        // Don't provide ID - let server generate it
        title,
        content,
        authorId: currentUserId,
      })
    )

    if (result.ok) {
      // Type-safe access to returned value
      const { postId } = result.value

      // Navigate to the new post immediately
      navigate(`/posts/${postId}`)
    }
  }

  return (
    <form onSubmit={handleSubmit}>
      <input value={title} onChange={e => setTitle(e.target.value)} />
      <textarea value={content} onChange={e => setContent(e.target.value)} />
      <button type="submit">Create Post</button>
    </form>
  )
}
```

### Type Safety

```typescript
// Ack return type is inferred from materializer
type PostCreatedAck = {
  postId: string
}

// Store.commit returns Promise<Result<AckValue, MaterializeError>>
type CommitResult = Awaited<ReturnType<typeof store.commit<typeof events.postCreated>>>
// => Result<{ postId: string }, MaterializeError>
```

---

## Example 2: Validation Result Return

**Use Case:** Server-side validation of username uniqueness. Client gets immediate feedback without polling.

### Event Definition

```typescript
export const events = {
  userRegistered: Events.synced({
    name: 'v1.UserRegistered',
    schema: Schema.Struct({
      username: Schema.String.pipe(Schema.minLength(3), Schema.maxLength(20)),
      email: Schema.String.pipe(Schema.pattern(/^[^\s@]+@[^\s@]+\.[^\s@]+$/)),
      password: Schema.String.pipe(Schema.minLength(8)),
    }),
  }),
}
```

### Materializer with Validation

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.UserRegistered': defineMaterializer(
    events.userRegistered,
    ({ username, email, password }, { query, ackReturn }) => {
      // Check for username uniqueness
      const existingUsername = query(
        users.select().where({ username }).first()
      )

      if (existingUsername) {
        ackReturn({
          success: false,
          error: 'USERNAME_TAKEN',
          message: `Username "${username}" is already taken`,
        })
        return [] // No-op: don't insert
      }

      // Check for email uniqueness
      const existingEmail = query(
        users.select().where({ email }).first()
      )

      if (existingEmail) {
        ackReturn({
          success: false,
          error: 'EMAIL_TAKEN',
          message: `Email "${email}" is already registered`,
        })
        return []
      }

      // Validation passed
      const userId = nanoid()

      ackReturn({
        success: true,
        userId,
      })

      return users.insert({
        id: userId,
        username,
        email,
        passwordHash: hashPassword(password),
        createdAt: new Date(),
      })
    }
  ),
})
```

### Client Usage with Error Handling

```typescript
function RegistrationForm() {
  const { store } = useStore()
  const [errors, setErrors] = useState<Record<string, string>>({})

  const handleSubmit = async (data: FormData) => {
    const result = await store.commit(
      events.userRegistered({
        username: data.username,
        email: data.email,
        password: data.password,
      })
    )

    if (!result.ok) {
      // Materializer threw an error
      setErrors({ general: 'Registration failed. Please try again.' })
      return
    }

    const ack = result.value

    if (!ack.success) {
      // Validation failed - show specific error
      if (ack.error === 'USERNAME_TAKEN') {
        setErrors({ username: ack.message })
      } else if (ack.error === 'EMAIL_TAKEN') {
        setErrors({ email: ack.message })
      }
      return
    }

    // Success!
    navigate(`/welcome?userId=${ack.userId}`)
  }

  return (
    <form onSubmit={handleSubmit}>
      <input name="username" />
      {errors.username && <span className="error">{errors.username}</span>}

      <input name="email" type="email" />
      {errors.email && <span className="error">{errors.email}</span>}

      <input name="password" type="password" />
      {errors.password && <span className="error">{errors.password}</span>}

      <button type="submit">Register</button>
    </form>
  )
}
```

---

## Example 3: Computed Field Return

**Use Case:** Client creates a blog post with a title. Server generates a URL-safe slug and returns it for immediate routing.

### Event Definition

```typescript
export const events = {
  articleCreated: Events.synced({
    name: 'v1.ArticleCreated',
    schema: Schema.Struct({
      title: Schema.String,
      content: Schema.String,
      authorId: Schema.String,
    }),
  }),
}
```

### Materializer Computing Slug

```typescript
import slugify from 'slugify'

const materializers = State.SQLite.materializers(events, {
  'v1.ArticleCreated': defineMaterializer(
    events.articleCreated,
    ({ title, content, authorId }, { query, ackReturn }) => {
      // Generate base slug from title
      let slug = slugify(title, { lower: true, strict: true })

      // Ensure slug uniqueness by checking database
      let finalSlug = slug
      let counter = 1

      while (true) {
        const existing = query(
          articles.select().where({ slug: finalSlug }).first()
        )

        if (!existing) break

        // Slug collision - append counter
        finalSlug = `${slug}-${counter}`
        counter++
      }

      const articleId = nanoid()

      // Return computed slug to client
      ackReturn({
        articleId,
        slug: finalSlug,
      })

      return articles.insert({
        id: articleId,
        title,
        slug: finalSlug,
        content,
        authorId,
        createdAt: new Date(),
      })
    }
  ),
})
```

### Client Usage

```typescript
function CreateArticle() {
  const { store } = useStore()
  const navigate = useNavigate()

  const handleSubmit = async (title: string, content: string) => {
    const result = await store.commit(
      events.articleCreated({
        title,
        content,
        authorId: currentUserId,
      })
    )

    if (result.ok) {
      const { slug } = result.value

      // Navigate to the article using the server-generated slug
      navigate(`/articles/${slug}`)

      // Could also show a toast with the shareable URL
      toast.success(`Article published! Share: ${window.location.origin}/articles/${slug}`)
    }
  }

  return <ArticleForm onSubmit={handleSubmit} />
}
```

---

## Example 4: Conflict Detection

**Use Case:** Optimistic concurrency control. Client attempts to update an entity, but server detects the entity was already modified.

### Event Definition

```typescript
export const events = {
  documentUpdated: Events.synced({
    name: 'v1.DocumentUpdated',
    schema: Schema.Struct({
      id: Schema.String,
      content: Schema.String,
      // Version number for optimistic locking
      expectedVersion: Schema.Number,
    }),
  }),
}
```

### Materializer with Conflict Detection

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.DocumentUpdated': defineMaterializer(
    events.documentUpdated,
    ({ id, content, expectedVersion }, { query, ackReturn }) => {
      // Check current version
      const doc = query(
        documents.select().where({ id }).first()
      )

      if (!doc) {
        ackReturn({
          success: false,
          conflict: 'DOCUMENT_NOT_FOUND',
          message: `Document ${id} does not exist`,
        })
        return []
      }

      if (doc.version !== expectedVersion) {
        // Version mismatch - conflict!
        ackReturn({
          success: false,
          conflict: 'VERSION_MISMATCH',
          message: 'Document was modified by another user',
          currentVersion: doc.version,
          currentContent: doc.content,
        })
        return []
      }

      // No conflict - proceed with update
      const newVersion = doc.version + 1

      ackReturn({
        success: true,
        newVersion,
      })

      return documents.update({
        content,
        version: newVersion,
        updatedAt: new Date(),
      }).where({ id })
    }
  ),
})
```

### Client Usage with Conflict Resolution

```typescript
function DocumentEditor({ documentId }: { documentId: string }) {
  const { store } = useStore()
  const doc = store.useQuery(getDocument({ id: documentId }))
  const [localContent, setLocalContent] = useState(doc.content)
  const [localVersion, setLocalVersion] = useState(doc.version)

  const handleSave = async () => {
    const result = await store.commit(
      events.documentUpdated({
        id: documentId,
        content: localContent,
        expectedVersion: localVersion,
      })
    )

    if (!result.ok) {
      toast.error('Failed to save document')
      return
    }

    const ack = result.value

    if (!ack.success) {
      if (ack.conflict === 'VERSION_MISMATCH') {
        // Show conflict resolution UI
        const shouldOverwrite = await showConflictDialog({
          local: localContent,
          remote: ack.currentContent,
        })

        if (shouldOverwrite) {
          // Retry with current version
          setLocalVersion(ack.currentVersion)
          // Recursively retry
          handleSave()
        } else {
          // Accept remote changes
          setLocalContent(ack.currentContent)
          setLocalVersion(ack.currentVersion)
        }
      }
      return
    }

    // Success - update local version
    setLocalVersion(ack.newVersion)
    toast.success('Document saved')
  }

  return (
    <div>
      <textarea
        value={localContent}
        onChange={e => setLocalContent(e.target.value)}
      />
      <button onClick={handleSave}>Save (v{localVersion})</button>
    </div>
  )
}
```

---

## Example 5: Created Entity Return

**Use Case:** Client creates a task. Server applies defaults (priority, status, timestamps) and returns the fully-populated entity for immediate optimistic UI updates.

### Event Definition

```typescript
export const events = {
  taskCreated: Events.synced({
    name: 'v1.TaskCreated',
    schema: Schema.Struct({
      title: Schema.String,
      description: Schema.String.pipe(Schema.optional),
      assigneeId: Schema.String.pipe(Schema.optional),
      // Client doesn't provide these - server sets defaults
    }),
  }),
}
```

### Materializer Returning Full Entity

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.TaskCreated': defineMaterializer(
    events.taskCreated,
    ({ title, description, assigneeId }, { query, ackReturn }) => {
      const taskId = nanoid()
      const now = new Date()

      // Compute default priority based on assignee workload
      let priority = 'medium'
      if (assigneeId) {
        const workload = query(
          tasks.select()
            .where({ assigneeId, status: 'open' })
            .count()
        )

        if (workload > 10) {
          priority = 'low' // Assignee is overloaded
        }
      }

      // Build the full task object with all defaults
      const task = {
        id: taskId,
        title,
        description: description ?? '',
        assigneeId: assigneeId ?? null,
        priority,
        status: 'open',
        createdAt: now,
        updatedAt: now,
        dueDate: null,
      }

      // Return the complete task to client
      ackReturn({ task })

      return tasks.insert(task)
    }
  ),
})
```

### Client Usage with Optimistic UI

```typescript
function TaskList() {
  const { store } = useStore()
  const tasks = store.useQuery(getAllTasks())
  const [optimisticTasks, setOptimisticTasks] = useState<Task[]>([])

  const createTask = async (title: string, assigneeId?: string) => {
    // Show a placeholder immediately (loading state)
    const tempId = `temp-${Date.now()}`
    setOptimisticTasks(prev => [...prev, {
      id: tempId,
      title,
      status: 'creating...',
    }])

    const result = await store.commit(
      events.taskCreated({
        title,
        assigneeId,
      })
    )

    if (result.ok) {
      const { task } = result.value

      // Replace placeholder with real task (with all defaults applied)
      setOptimisticTasks(prev =>
        prev.filter(t => t.id !== tempId)
      )

      // Real task is now in the query results via store.commit materialization
      // But we got the full entity back immediately for any additional UI needs

      toast.success(
        `Task created with ${task.priority} priority`
      )
    } else {
      // Remove placeholder on error
      setOptimisticTasks(prev =>
        prev.filter(t => t.id !== tempId)
      )
      toast.error('Failed to create task')
    }
  }

  return (
    <div>
      <ul>
        {[...tasks, ...optimisticTasks].map(task => (
          <TaskItem key={task.id} task={task} />
        ))}
      </ul>
      <CreateTaskButton onCreate={createTask} />
    </div>
  )
}
```

---

## Example 6: Batch Result Return

**Use Case:** Client imports multiple items in a batch. Server processes each, and some may fail validation. Client needs to know which succeeded/failed.

### Event Definition

```typescript
export const events = {
  contactsBatchImported: Events.synced({
    name: 'v1.ContactsBatchImported',
    schema: Schema.Struct({
      contacts: Schema.Array(
        Schema.Struct({
          name: Schema.String,
          email: Schema.String,
          phone: Schema.String.pipe(Schema.optional),
        })
      ),
    }),
  }),
}
```

### Materializer with Batch Processing

```typescript
const materializers = State.SQLite.materializers(events, {
  'v1.ContactsBatchImported': defineMaterializer(
    events.contactsBatchImported,
    ({ contacts }, { query, ackReturn }) => {
      const results: Array<{
        index: number
        success: boolean
        contactId?: string
        error?: string
      }> = []

      const inserts = []

      for (let i = 0; i < contacts.length; i++) {
        const contact = contacts[i]

        // Validate email format (additional server-side validation)
        if (!contact.email.includes('@')) {
          results.push({
            index: i,
            success: false,
            error: 'Invalid email format',
          })
          continue
        }

        // Check for duplicate email
        const existing = query(
          contactsTable.select()
            .where({ email: contact.email })
            .first()
        )

        if (existing) {
          results.push({
            index: i,
            success: false,
            error: `Email ${contact.email} already exists`,
          })
          continue
        }

        // Success - prepare insert
        const contactId = nanoid()

        results.push({
          index: i,
          success: true,
          contactId,
        })

        inserts.push({
          id: contactId,
          name: contact.name,
          email: contact.email,
          phone: contact.phone ?? null,
          createdAt: new Date(),
        })
      }

      // Return detailed results for each contact
      const summary = {
        total: contacts.length,
        succeeded: results.filter(r => r.success).length,
        failed: results.filter(r => !r.success).length,
        results,
      }

      ackReturn(summary)

      // Insert all successful contacts
      return inserts.map(contact =>
        contactsTable.insert(contact)
      )
    }
  ),
})
```

### Client Usage with Progress Feedback

```typescript
function ContactImport() {
  const { store } = useStore()
  const [importing, setImporting] = useState(false)
  const [results, setResults] = useState<BatchResults | null>(null)

  const handleImport = async (csvData: string) => {
    setImporting(true)

    const contacts = parseCSV(csvData)

    const result = await store.commit(
      events.contactsBatchImported({ contacts })
    )

    setImporting(false)

    if (!result.ok) {
      toast.error('Import failed')
      return
    }

    const { total, succeeded, failed, results } = result.value

    setResults(results)

    if (failed === 0) {
      toast.success(`Successfully imported all ${total} contacts!`)
    } else {
      toast.warning(
        `Imported ${succeeded} of ${total} contacts. ${failed} failed.`
      )
    }
  }

  return (
    <div>
      <FileUpload onUpload={handleImport} disabled={importing} />

      {importing && <Spinner />}

      {results && (
        <ResultsTable>
          {results.results.map((result, i) => (
            <tr key={i}>
              <td>{i + 1}</td>
              <td>{result.success ? '✓' : '✗'}</td>
              <td>{result.success ? result.contactId : result.error}</td>
            </tr>
          ))}
        </ResultsTable>
      )}
    </div>
  )
}
```

---

## Implementation Considerations

### Type Safety

Ack returns must be fully type-safe:

```typescript
type AckReturnType<T extends EventDef> =
  T extends { ackReturn: infer R } ? R : void

// Materializer can optionally call ackReturn
type MaterializerContext<TEventDef> = {
  query: MaterializerContextQuery
  currentFacts: EventDefFacts
  event: LiveStoreEvent.Client.Decoded
  ackReturn: <TAck>(value: TAck) => void
}

// Store.commit return type is derived from ack return
store.commit: <TEvent>(event: TEvent) =>
  Promise<Result<AckReturnType<TEvent>, MaterializeError>>
```

### Error Handling

Materializer failures must be distinguished from validation failures:

```typescript
const result = await store.commit(events.userCreated({ ... }))

// Materializer threw an exception
if (!result.ok) {
  console.error('Materialize error:', result.error)
  return
}

// Materializer ran successfully, returned ack value
const ack = result.value

// Check if ack indicates validation failure
if ('success' in ack && !ack.success) {
  console.log('Validation failed:', ack.error)
}
```

### Performance Considerations

- Ack returns should be lightweight (no expensive computation)
- For batch operations, return summarized results, not full entities
- Consider timeouts for materializers that might hang
- Ack returns must be deterministic (same event = same ack value)

### Synchronization Behavior

Key questions to resolve:

1. **When does ack return fire?**
   - Immediately after local materialization?
   - After sync to leader?
   - After sync to other clients?

2. **What about offline commits?**
   - Should ack returns be queued and delivered when online?
   - Should they timeout if offline too long?

3. **Multi-client behavior:**
   - If Client A commits an event, Client B's materializer also runs
   - Does Client B's ack return get sent anywhere?
   - Probably not - only the originating client should receive ack

### Backwards Compatibility

```typescript
// Existing materializers without ackReturn continue to work
const oldMaterializer = ({ id, name }) => {
  return users.insert({ id, name })
}

// New materializers can opt-in to ack returns
const newMaterializer = ({ id, name }, { ackReturn }) => {
  ackReturn({ userId: id })
  return users.insert({ id, name })
}

// Commit without ack returns remains synchronous
store.commit(events.oldEvent({ ... })) // void

// Commit with ack returns returns Promise
await store.commit(events.newEvent({ ... })) // Promise<Result<AckValue, Error>>
```

---

## Comparison to Rama

### Similarities

- Synchronous feedback from stream processing to depot clients
- Useful for returning server-generated values (IDs, computed fields)
- Validation feedback without polling
- Type-safe return values

### Differences

| Rama | LiveStore |
|------|-----------|
| Stream topology returns to depot append | Materializer returns to commit caller |
| Single-threaded deterministic processing | Multi-client distributed processing |
| Always returns (no async/offline concerns) | Must handle offline clients gracefully |
| JVM types via Rama schema | TypeScript types via Effect Schema |
| `(ack-return> *value)` operator | `ackReturn({ value })` function call |

### LiveStore-Specific Advantages

1. **Type inference:** TypeScript can infer ack return types from materializer definitions
2. **Promise-based:** Fits naturally into async/await patterns
3. **Client-side materialization:** Ack returns can be instant (no server round-trip) for client-only events
4. **Effect integration:** Can leverage Effect for advanced error handling

### LiveStore-Specific Challenges

1. **Multi-client coordination:** How to ensure only originating client receives ack?
2. **Offline support:** Queue acks? Timeout? Fail immediately?
3. **Determinism:** Ack returns must be deterministic for replay/sync consistency
4. **Leader vs client-session:** Should acks come from leader materializer or client-session materializer?
