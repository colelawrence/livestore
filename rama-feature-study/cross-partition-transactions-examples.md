## 3. Cross-Partition Transactions - Concrete Examples

### Overview

Cross-partition transactions enable atomic operations across multiple entities that may be stored on different partitions. This is critical for maintaining consistency in distributed scenarios like fund transfers, inventory moves, or role reassignments.

**Key Requirements:**
- Exactly-once semantics: Each transaction executes once, even if retried due to failures
- Atomic visibility: All updates succeed together or none succeed
- Validation before commit: Check preconditions (e.g., sufficient funds) before applying changes
- Consistent failure states: Both sides of the transaction record the same outcome
- Audit trail: Both sides record the transaction in their history

---

### Example 1: Fund Transfer Between Accounts

**Use Case:** Transfer money between user accounts with insufficient funds protection.

```typescript
// Schema Definition
import { Events, makeSchema, Schema, State } from '@livestore/livestore'

const tables = {
  accounts: State.SQLite.table({
    name: 'accounts',
    columns: {
      userId: State.SQLite.text({ primaryKey: true }),
      balance: State.SQLite.integer({ default: 0 }), // Store as cents
      currency: State.SQLite.text({ default: 'USD' }),
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  outgoingTransfers: State.SQLite.table({
    name: 'outgoing_transfers',
    columns: {
      id: State.SQLite.text({ primaryKey: true }), // transferId
      fromUserId: State.SQLite.text(),
      toUserId: State.SQLite.text(),
      amount: State.SQLite.integer(),
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  incomingTransfers: State.SQLite.table({
    name: 'incoming_transfers',
    columns: {
      id: State.SQLite.text({ primaryKey: true }), // transferId
      fromUserId: State.SQLite.text(),
      toUserId: State.SQLite.text(),
      amount: State.SQLite.integer(),
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),
}

// Transactional Event
const events = {
  fundsTransferred: Events.synced({
    name: 'v1.FundsTransferred',
    schema: Schema.Struct({
      transferId: Schema.String,
      fromUserId: Schema.String,
      toUserId: Schema.String,
      amount: Schema.Number, // cents
      createdAt: Schema.Date,
    }),
  }),

  accountDeposited: Events.synced({
    name: 'v1.AccountDeposited',
    schema: Schema.Struct({
      userId: Schema.String,
      amount: Schema.Number,
      createdAt: Schema.Date,
    }),
  }),
}

// Materializers with validation and atomic updates
const materializers = State.SQLite.materializers(events, {
  'v1.FundsTransferred': ({ transferId, fromUserId, toUserId, amount, createdAt }) => {
    // 1. Validation: Check sender has sufficient funds
    const senderAccount = tables.accounts.select().where({ userId: fromUserId }).one()
    const senderBalance = senderAccount?.balance ?? 0
    const success = senderBalance >= amount
    const failureReason = success ? null : 'insufficient_funds'

    const operations = []

    if (success) {
      // 2. Deduct from sender
      operations.push(
        tables.accounts.update({
          balance: senderBalance - amount,
          updatedAt: createdAt,
        }).where({ userId: fromUserId })
      )

      // 3. Credit to receiver (atomic with deduction via microbatch)
      const receiverAccount = tables.accounts.select().where({ userId: toUserId }).one()
      const receiverBalance = receiverAccount?.balance ?? 0
      operations.push(
        receiverAccount
          ? tables.accounts.update({
              balance: receiverBalance + amount,
              updatedAt: createdAt,
            }).where({ userId: toUserId })
          : tables.accounts.insert({
              userId: toUserId,
              balance: amount,
              currency: 'USD',
              updatedAt: createdAt
            })
      )
    }

    // 4. Record transfer in sender's outgoing history
    operations.push(
      tables.outgoingTransfers.insert({
        id: transferId,
        fromUserId,
        toUserId,
        amount,
        success,
        failureReason,
        createdAt,
      })
    )

    // 5. Record transfer in receiver's incoming history
    operations.push(
      tables.incomingTransfers.insert({
        id: transferId,
        fromUserId,
        toUserId,
        amount,
        success,
        failureReason,
        createdAt,
      })
    )

    return operations
  },

  'v1.AccountDeposited': ({ userId, amount, createdAt }) => {
    const account = tables.accounts.select().where({ userId }).one()
    const balance = account?.balance ?? 0
    return account
      ? tables.accounts.update({
          balance: balance + amount,
          updatedAt: createdAt,
        }).where({ userId })
      : tables.accounts.insert({
          userId,
          balance: amount,
          currency: 'USD',
          updatedAt: createdAt
        })
  },
})

// Usage Example
async function transferFunds(store, fromUserId: string, toUserId: string, amount: number) {
  const transferId = crypto.randomUUID()

  await store.append(events.fundsTransferred.create({
    transferId,
    fromUserId,
    toUserId,
    amount,
    createdAt: new Date(),
  }))

  // Query outcome from either perspective
  const outcome = await store.queryDb(
    tables.outgoingTransfers.select().where({ id: transferId }).one(),
    { label: 'transferOutcome' }
  )

  return outcome // { success: true/false, failureReason?: string }
}

// Example test scenario
async function runTransferScenario(store) {
  // Setup: Alice has $200, Bob has $100
  await store.append(events.accountDeposited.create({ userId: 'alice', amount: 20000, createdAt: new Date() }))
  await store.append(events.accountDeposited.create({ userId: 'bob', amount: 10000, createdAt: new Date() }))

  // Transfer #1: Alice -> Bob $50 (succeeds)
  await transferFunds(store, 'alice', 'bob', 5000)
  // Balances: Alice=$150, Bob=$150

  // Transfer #2: Alice -> Charlie $160 (fails - insufficient funds)
  await transferFunds(store, 'alice', 'charlie', 16000)
  // Balances: Alice=$150, Bob=$150, Charlie=$0

  // Transfer #3: Alice -> Charlie $25 (succeeds)
  await transferFunds(store, 'alice', 'charlie', 2500)
  // Balances: Alice=$125, Bob=$150, Charlie=$25

  // Both alice and charlie have consistent records of transfer #2 failure
  const aliceOutgoing = await store.queryDb(
    tables.outgoingTransfers.select().where({ fromUserId: 'alice' }),
    { label: 'aliceTransfers' }
  )
  const charlieIncoming = await store.queryDb(
    tables.incomingTransfers.select().where({ toUserId: 'charlie' }),
    { label: 'charlieTransfers' }
  )

  // Both show success=false for the $160 transfer
  console.log(aliceOutgoing.find(t => t.amount === 16000))
  // { success: false, failureReason: 'insufficient_funds' }
  console.log(charlieIncoming.find(t => t.amount === 16000))
  // { success: false, failureReason: 'insufficient_funds' }
}
```

**Key Points:**
- Validation happens first before any state changes
- Failed transfers are recorded on both sides with consistent failure state
- Successful transfers atomically update both balances
- Complete audit trail of all transfer attempts

---

### Example 2: Inventory Transfer Between Warehouses

**Use Case:** Move physical inventory between locations with availability checks.

```typescript
// Schema Definition
const tables = {
  inventoryByLocation: State.SQLite.table({
    name: 'inventory_by_location',
    columns: {
      locationId: State.SQLite.text(),
      productId: State.SQLite.text(),
      quantity: State.SQLite.integer({ default: 0 }),
      reservedQuantity: State.SQLite.integer({ default: 0 }),
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  inventoryTransfers: State.SQLite.table({
    name: 'inventory_transfers',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      productId: State.SQLite.text(),
      fromLocationId: State.SQLite.text(),
      toLocationId: State.SQLite.text(),
      quantity: State.SQLite.integer(),
      status: State.SQLite.text(), // 'pending', 'in_transit', 'completed', 'failed'
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      initiatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
      completedAt: State.SQLite.integer({ nullable: true, schema: Schema.DateFromNumber }),
    },
  }),
}

const events = {
  inventoryTransferInitiated: Events.synced({
    name: 'v1.InventoryTransferInitiated',
    schema: Schema.Struct({
      transferId: Schema.String,
      productId: Schema.String,
      fromLocationId: Schema.String,
      toLocationId: Schema.String,
      quantity: Schema.Number,
      initiatedAt: Schema.Date,
    }),
  }),

  inventoryTransferCompleted: Events.synced({
    name: 'v1.InventoryTransferCompleted',
    schema: Schema.Struct({
      transferId: Schema.String,
      completedAt: Schema.Date,
    }),
  }),

  inventoryTransferCancelled: Events.synced({
    name: 'v1.InventoryTransferCancelled',
    schema: Schema.Struct({
      transferId: Schema.String,
      reason: Schema.String,
      cancelledAt: Schema.Date,
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.InventoryTransferInitiated': ({
    transferId,
    productId,
    fromLocationId,
    toLocationId,
    quantity,
    initiatedAt
  }) => {
    // 1. Validate source location has sufficient inventory
    const sourceInventory = tables.inventoryByLocation
      .select()
      .where({ locationId: fromLocationId, productId })
      .one()

    const availableQuantity = (sourceInventory?.quantity ?? 0) - (sourceInventory?.reservedQuantity ?? 0)
    const success = availableQuantity >= quantity
    const failureReason = success ? null : 'insufficient_inventory'

    const operations = []

    if (success) {
      // 2. Reserve inventory at source (deduct from available)
      operations.push(
        tables.inventoryByLocation.update({
          reservedQuantity: (sourceInventory?.reservedQuantity ?? 0) + quantity,
          updatedAt: initiatedAt,
        }).where({ locationId: fromLocationId, productId })
      )
    }

    // 3. Record transfer (even if failed for audit trail)
    operations.push(
      tables.inventoryTransfers.insert({
        id: transferId,
        productId,
        fromLocationId,
        toLocationId,
        quantity,
        status: success ? 'pending' : 'failed',
        success,
        failureReason,
        initiatedAt,
        completedAt: success ? null : initiatedAt,
      })
    )

    return operations
  },

  'v1.InventoryTransferCompleted': ({ transferId, completedAt }) => {
    // Get transfer details
    const transfer = tables.inventoryTransfers.select().where({ id: transferId }).one()
    if (!transfer || transfer.status !== 'pending') {
      return [] // Invalid or already completed
    }

    const { productId, fromLocationId, toLocationId, quantity } = transfer
    const operations = []

    // 1. Deduct from source (convert reservation to actual reduction)
    const sourceInventory = tables.inventoryByLocation
      .select()
      .where({ locationId: fromLocationId, productId })
      .one()

    operations.push(
      tables.inventoryByLocation.update({
        quantity: (sourceInventory?.quantity ?? 0) - quantity,
        reservedQuantity: Math.max(0, (sourceInventory?.reservedQuantity ?? 0) - quantity),
        updatedAt: completedAt,
      }).where({ locationId: fromLocationId, productId })
    )

    // 2. Add to destination
    const destInventory = tables.inventoryByLocation
      .select()
      .where({ locationId: toLocationId, productId })
      .one()

    operations.push(
      destInventory
        ? tables.inventoryByLocation.update({
            quantity: (destInventory.quantity ?? 0) + quantity,
            updatedAt: completedAt,
          }).where({ locationId: toLocationId, productId })
        : tables.inventoryByLocation.insert({
            locationId: toLocationId,
            productId,
            quantity,
            reservedQuantity: 0,
            updatedAt: completedAt,
          })
    )

    // 3. Update transfer status
    operations.push(
      tables.inventoryTransfers.update({
        status: 'completed',
        completedAt,
      }).where({ id: transferId })
    )

    return operations
  },

  'v1.InventoryTransferCancelled': ({ transferId, reason, cancelledAt }) => {
    const transfer = tables.inventoryTransfers.select().where({ id: transferId }).one()
    if (!transfer || transfer.status !== 'pending') {
      return []
    }

    const { productId, fromLocationId, quantity } = transfer
    const operations = []

    // 1. Release reserved inventory at source
    const sourceInventory = tables.inventoryByLocation
      .select()
      .where({ locationId: fromLocationId, productId })
      .one()

    operations.push(
      tables.inventoryByLocation.update({
        reservedQuantity: Math.max(0, (sourceInventory?.reservedQuantity ?? 0) - quantity),
        updatedAt: cancelledAt,
      }).where({ locationId: fromLocationId, productId })
    )

    // 2. Update transfer status
    operations.push(
      tables.inventoryTransfers.update({
        status: 'failed',
        success: false,
        failureReason: reason,
        completedAt: cancelledAt,
      }).where({ id: transferId })
    )

    return operations
  },
})

// Usage Example
async function transferInventory(
  store,
  productId: string,
  fromLocationId: string,
  toLocationId: string,
  quantity: number
) {
  const transferId = crypto.randomUUID()

  // Initiate transfer (validates and reserves)
  await store.append(events.inventoryTransferInitiated.create({
    transferId,
    productId,
    fromLocationId,
    toLocationId,
    quantity,
    initiatedAt: new Date(),
  }))

  // Check if transfer was successful
  const transfer = await store.queryDb(
    tables.inventoryTransfers.select().where({ id: transferId }).one(),
    { label: 'transferStatus' }
  )

  if (!transfer.success) {
    throw new Error(`Transfer failed: ${transfer.failureReason}`)
  }

  // Simulate physical shipment delay...
  // await delay(1000)

  // Complete the transfer (moves inventory)
  await store.append(events.inventoryTransferCompleted.create({
    transferId,
    completedAt: new Date(),
  }))

  return transferId
}
```

**Key Points:**
- Two-phase commit: Reserve first, then complete
- Cancellation support to rollback reservations
- Atomic movement prevents inventory loss or duplication
- Status tracking for logistics integration

---

### Example 3: Team Member Reassignment

**Use Case:** Move a user from one team to another atomically.

```typescript
// Schema Definition
const tables = {
  teams: State.SQLite.table({
    name: 'teams',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      name: State.SQLite.text(),
      memberCount: State.SQLite.integer({ default: 0 }),
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  teamMembers: State.SQLite.table({
    name: 'team_members',
    columns: {
      teamId: State.SQLite.text(),
      userId: State.SQLite.text(),
      role: State.SQLite.text(), // 'member', 'admin', 'owner'
      joinedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  teamMemberHistory: State.SQLite.table({
    name: 'team_member_history',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      userId: State.SQLite.text(),
      fromTeamId: State.SQLite.text({ nullable: true }),
      toTeamId: State.SQLite.text({ nullable: true }),
      action: State.SQLite.text(), // 'joined', 'left', 'transferred'
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),
}

const events = {
  userTransferredBetweenTeams: Events.synced({
    name: 'v1.UserTransferredBetweenTeams',
    schema: Schema.Struct({
      transferId: Schema.String,
      userId: Schema.String,
      fromTeamId: Schema.String,
      toTeamId: Schema.String,
      newRole: Schema.String,
      createdAt: Schema.Date,
    }),
  }),

  userJoinedTeam: Events.synced({
    name: 'v1.UserJoinedTeam',
    schema: Schema.Struct({
      teamId: Schema.String,
      userId: Schema.String,
      role: Schema.String,
      joinedAt: Schema.Date,
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.UserTransferredBetweenTeams': ({
    transferId,
    userId,
    fromTeamId,
    toTeamId,
    newRole,
    createdAt
  }) => {
    // 1. Validate user is currently on the source team
    const membership = tables.teamMembers
      .select()
      .where({ teamId: fromTeamId, userId })
      .one()

    const success = !!membership
    const failureReason = success ? null : 'user_not_on_source_team'

    const operations = []

    if (success) {
      // 2. Remove from source team
      operations.push(
        tables.teamMembers.delete().where({ teamId: fromTeamId, userId })
      )

      // 3. Decrement source team member count
      const sourceTeam = tables.teams.select().where({ id: fromTeamId }).one()
      operations.push(
        tables.teams.update({
          memberCount: Math.max(0, (sourceTeam?.memberCount ?? 1) - 1),
          updatedAt: createdAt,
        }).where({ id: fromTeamId })
      )

      // 4. Add to destination team
      operations.push(
        tables.teamMembers.insert({
          teamId: toTeamId,
          userId,
          role: newRole,
          joinedAt: createdAt,
        })
      )

      // 5. Increment destination team member count
      const destTeam = tables.teams.select().where({ id: toTeamId }).one()
      operations.push(
        tables.teams.update({
          memberCount: (destTeam?.memberCount ?? 0) + 1,
          updatedAt: createdAt,
        }).where({ id: toTeamId })
      )
    }

    // 6. Record in history
    operations.push(
      tables.teamMemberHistory.insert({
        id: transferId,
        userId,
        fromTeamId,
        toTeamId,
        action: 'transferred',
        success,
        failureReason,
        createdAt,
      })
    )

    return operations
  },

  'v1.UserJoinedTeam': ({ teamId, userId, role, joinedAt }) => {
    const operations = []

    // Add user to team
    operations.push(
      tables.teamMembers.insert({ teamId, userId, role, joinedAt })
    )

    // Increment member count
    const team = tables.teams.select().where({ id: teamId }).one()
    operations.push(
      tables.teams.update({
        memberCount: (team?.memberCount ?? 0) + 1,
        updatedAt: joinedAt,
      }).where({ id: teamId })
    )

    // Record in history
    operations.push(
      tables.teamMemberHistory.insert({
        id: crypto.randomUUID(),
        userId,
        fromTeamId: null,
        toTeamId: teamId,
        action: 'joined',
        success: true,
        failureReason: null,
        createdAt: joinedAt,
      })
    )

    return operations
  },
})
```

**Key Points:**
- Atomic remove from source, add to destination
- Member counts stay consistent
- Validation prevents invalid transfers
- Complete audit trail of all membership changes

---

### Example 4: Order Fulfillment with Multi-Table Updates

**Use Case:** Process an order by deducting inventory, creating order record, and updating customer balance.

```typescript
// Schema Definition
const tables = {
  products: State.SQLite.table({
    name: 'products',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      name: State.SQLite.text(),
      stock: State.SQLite.integer({ default: 0 }),
      price: State.SQLite.integer(), // cents
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  customers: State.SQLite.table({
    name: 'customers',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      name: State.SQLite.text(),
      creditBalance: State.SQLite.integer({ default: 0 }), // cents
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  orders: State.SQLite.table({
    name: 'orders',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      customerId: State.SQLite.text(),
      totalAmount: State.SQLite.integer(),
      status: State.SQLite.text(), // 'pending', 'fulfilled', 'failed'
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  orderItems: State.SQLite.table({
    name: 'order_items',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      orderId: State.SQLite.text(),
      productId: State.SQLite.text(),
      quantity: State.SQLite.integer(),
      unitPrice: State.SQLite.integer(),
      totalPrice: State.SQLite.integer(),
    },
  }),
}

const events = {
  orderPlaced: Events.synced({
    name: 'v1.OrderPlaced',
    schema: Schema.Struct({
      orderId: Schema.String,
      customerId: Schema.String,
      items: Schema.Array(Schema.Struct({
        productId: Schema.String,
        quantity: Schema.Number,
      })),
      useCredit: Schema.Boolean,
      createdAt: Schema.Date,
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.OrderPlaced': ({ orderId, customerId, items, useCredit, createdAt }) => {
    const operations = []
    let totalAmount = 0
    let failureReason = null

    // 1. Validate inventory for all items
    for (const item of items) {
      const product = tables.products.select().where({ id: item.productId }).one()
      if (!product) {
        failureReason = `product_not_found:${item.productId}`
        break
      }
      if (product.stock < item.quantity) {
        failureReason = `insufficient_stock:${item.productId}`
        break
      }
      totalAmount += product.price * item.quantity
    }

    // 2. Validate customer credit if using credit
    if (!failureReason && useCredit) {
      const customer = tables.customers.select().where({ id: customerId }).one()
      if (!customer || customer.creditBalance < totalAmount) {
        failureReason = 'insufficient_credit'
      }
    }

    const success = !failureReason

    if (success) {
      // 3. Deduct inventory for each item
      for (const item of items) {
        const product = tables.products.select().where({ id: item.productId }).one()!
        operations.push(
          tables.products.update({
            stock: product.stock - item.quantity,
            updatedAt: createdAt,
          }).where({ id: item.productId })
        )

        // Create order line item
        operations.push(
          tables.orderItems.insert({
            id: crypto.randomUUID(),
            orderId,
            productId: item.productId,
            quantity: item.quantity,
            unitPrice: product.price,
            totalPrice: product.price * item.quantity,
          })
        )
      }

      // 4. Deduct from customer credit if applicable
      if (useCredit) {
        const customer = tables.customers.select().where({ id: customerId }).one()!
        operations.push(
          tables.customers.update({
            creditBalance: customer.creditBalance - totalAmount,
            updatedAt: createdAt,
          }).where({ id: customerId })
        )
      }
    }

    // 5. Create order record (even if failed)
    operations.push(
      tables.orders.insert({
        id: orderId,
        customerId,
        totalAmount,
        status: success ? 'fulfilled' : 'failed',
        success,
        failureReason,
        createdAt,
      })
    )

    return operations
  },
})

// Usage Example
async function placeOrder(
  store,
  customerId: string,
  items: Array<{ productId: string; quantity: number }>,
  useCredit: boolean
) {
  const orderId = crypto.randomUUID()

  await store.append(events.orderPlaced.create({
    orderId,
    customerId,
    items,
    useCredit,
    createdAt: new Date(),
  }))

  const order = await store.queryDb(
    tables.orders.select().where({ id: orderId }).one(),
    { label: 'orderStatus' }
  )

  if (!order.success) {
    throw new Error(`Order failed: ${order.failureReason}`)
  }

  return order
}
```

**Key Points:**
- Multi-table validation before any changes
- All-or-nothing: Either all items fulfilled or order fails
- Inventory, customer balance, and order state all updated atomically
- Clear failure reasons for debugging

---

### Example 5: Permission Delegation / Ownership Transfer

**Use Case:** Transfer admin rights or ownership from one user to another with role validation.

```typescript
// Schema Definition
const tables = {
  resourcePermissions: State.SQLite.table({
    name: 'resource_permissions',
    columns: {
      resourceId: State.SQLite.text(),
      userId: State.SQLite.text(),
      role: State.SQLite.text(), // 'owner', 'admin', 'editor', 'viewer'
      grantedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  permissionHistory: State.SQLite.table({
    name: 'permission_history',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      resourceId: State.SQLite.text(),
      fromUserId: State.SQLite.text(),
      toUserId: State.SQLite.text(),
      role: State.SQLite.text(),
      action: State.SQLite.text(), // 'transferred', 'granted', 'revoked'
      success: State.SQLite.boolean(),
      failureReason: State.SQLite.text({ nullable: true }),
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),
}

const events = {
  ownershipTransferred: Events.synced({
    name: 'v1.OwnershipTransferred',
    schema: Schema.Struct({
      transferId: Schema.String,
      resourceId: Schema.String,
      fromUserId: Schema.String,
      toUserId: Schema.String,
      downgradePreviousOwner: Schema.Boolean, // true = make them admin, false = remove entirely
      createdAt: Schema.Date,
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.OwnershipTransferred': ({
    transferId,
    resourceId,
    fromUserId,
    toUserId,
    downgradePreviousOwner,
    createdAt
  }) => {
    // 1. Validate current owner
    const currentOwner = tables.resourcePermissions
      .select()
      .where({ resourceId, userId: fromUserId, role: 'owner' })
      .one()

    const success = !!currentOwner
    const failureReason = success ? null : 'current_user_not_owner'

    const operations = []

    if (success) {
      if (downgradePreviousOwner) {
        // 2a. Downgrade current owner to admin
        operations.push(
          tables.resourcePermissions.update({
            role: 'admin',
            grantedAt: createdAt,
          }).where({ resourceId, userId: fromUserId })
        )
      } else {
        // 2b. Remove current owner entirely
        operations.push(
          tables.resourcePermissions.delete().where({ resourceId, userId: fromUserId })
        )
      }

      // 3. Grant ownership to new user
      const existingPermission = tables.resourcePermissions
        .select()
        .where({ resourceId, userId: toUserId })
        .one()

      if (existingPermission) {
        // Upgrade existing permission to owner
        operations.push(
          tables.resourcePermissions.update({
            role: 'owner',
            grantedAt: createdAt,
          }).where({ resourceId, userId: toUserId })
        )
      } else {
        // Grant new owner permission
        operations.push(
          tables.resourcePermissions.insert({
            resourceId,
            userId: toUserId,
            role: 'owner',
            grantedAt: createdAt,
          })
        )
      }
    }

    // 4. Record transfer in history
    operations.push(
      tables.permissionHistory.insert({
        id: transferId,
        resourceId,
        fromUserId,
        toUserId,
        role: 'owner',
        action: 'transferred',
        success,
        failureReason,
        createdAt,
      })
    )

    return operations
  },
})
```

**Key Points:**
- Validates current owner before transfer
- Atomic revoke + grant prevents ownership gaps
- Optional downgrade for graceful transitions
- Complete permission audit trail

---

### Example 6: Escrow Pattern (Hold and Release)

**Use Case:** Hold funds in escrow until conditions are met, then release to recipient or refund.

```typescript
// Schema Definition
const tables = {
  accounts: State.SQLite.table({
    name: 'accounts',
    columns: {
      userId: State.SQLite.text({ primaryKey: true }),
      availableBalance: State.SQLite.integer({ default: 0 }),
      escrowBalance: State.SQLite.integer({ default: 0 }),
      updatedAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
    },
  }),

  escrows: State.SQLite.table({
    name: 'escrows',
    columns: {
      id: State.SQLite.text({ primaryKey: true }),
      payerId: State.SQLite.text(),
      payeeId: State.SQLite.text(),
      amount: State.SQLite.integer(),
      status: State.SQLite.text(), // 'held', 'released', 'refunded'
      condition: State.SQLite.text(), // Description of release condition
      createdAt: State.SQLite.integer({ schema: Schema.DateFromNumber }),
      resolvedAt: State.SQLite.integer({ nullable: true, schema: Schema.DateFromNumber }),
    },
  }),
}

const events = {
  escrowCreated: Events.synced({
    name: 'v1.EscrowCreated',
    schema: Schema.Struct({
      escrowId: Schema.String,
      payerId: Schema.String,
      payeeId: Schema.String,
      amount: Schema.Number,
      condition: Schema.String,
      createdAt: Schema.Date,
    }),
  }),

  escrowReleased: Events.synced({
    name: 'v1.EscrowReleased',
    schema: Schema.Struct({
      escrowId: Schema.String,
      releasedAt: Schema.Date,
    }),
  }),

  escrowRefunded: Events.synced({
    name: 'v1.EscrowRefunded',
    schema: Schema.Struct({
      escrowId: Schema.String,
      reason: Schema.String,
      refundedAt: Schema.Date,
    }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'v1.EscrowCreated': ({ escrowId, payerId, payeeId, amount, condition, createdAt }) => {
    // 1. Validate payer has sufficient available balance
    const payerAccount = tables.accounts.select().where({ userId: payerId }).one()
    const availableBalance = payerAccount?.availableBalance ?? 0

    if (availableBalance < amount) {
      // Record failed escrow attempt
      return tables.escrows.insert({
        id: escrowId,
        payerId,
        payeeId,
        amount,
        status: 'failed',
        condition,
        createdAt,
        resolvedAt: createdAt,
      })
    }

    const operations = []

    // 2. Move funds from available to escrow
    operations.push(
      tables.accounts.update({
        availableBalance: availableBalance - amount,
        escrowBalance: (payerAccount?.escrowBalance ?? 0) + amount,
        updatedAt: createdAt,
      }).where({ userId: payerId })
    )

    // 3. Create escrow record
    operations.push(
      tables.escrows.insert({
        id: escrowId,
        payerId,
        payeeId,
        amount,
        status: 'held',
        condition,
        createdAt,
        resolvedAt: null,
      })
    )

    return operations
  },

  'v1.EscrowReleased': ({ escrowId, releasedAt }) => {
    const escrow = tables.escrows.select().where({ id: escrowId }).one()
    if (!escrow || escrow.status !== 'held') {
      return [] // Invalid or already resolved
    }

    const { payerId, payeeId, amount } = escrow
    const operations = []

    // 1. Remove from payer's escrow
    const payerAccount = tables.accounts.select().where({ userId: payerId }).one()
    operations.push(
      tables.accounts.update({
        escrowBalance: Math.max(0, (payerAccount?.escrowBalance ?? 0) - amount),
        updatedAt: releasedAt,
      }).where({ userId: payerId })
    )

    // 2. Add to payee's available balance
    const payeeAccount = tables.accounts.select().where({ userId: payeeId }).one()
    operations.push(
      payeeAccount
        ? tables.accounts.update({
            availableBalance: (payeeAccount.availableBalance ?? 0) + amount,
            updatedAt: releasedAt,
          }).where({ userId: payeeId })
        : tables.accounts.insert({
            userId: payeeId,
            availableBalance: amount,
            escrowBalance: 0,
            updatedAt: releasedAt,
          })
    )

    // 3. Update escrow status
    operations.push(
      tables.escrows.update({
        status: 'released',
        resolvedAt: releasedAt,
      }).where({ id: escrowId })
    )

    return operations
  },

  'v1.EscrowRefunded': ({ escrowId, reason, refundedAt }) => {
    const escrow = tables.escrows.select().where({ id: escrowId }).one()
    if (!escrow || escrow.status !== 'held') {
      return []
    }

    const { payerId, amount } = escrow
    const operations = []

    // 1. Return funds from escrow to payer's available balance
    const payerAccount = tables.accounts.select().where({ userId: payerId }).one()
    operations.push(
      tables.accounts.update({
        availableBalance: (payerAccount?.availableBalance ?? 0) + amount,
        escrowBalance: Math.max(0, (payerAccount?.escrowBalance ?? 0) - amount),
        updatedAt: refundedAt,
      }).where({ userId: payerId })
    )

    // 2. Update escrow status
    operations.push(
      tables.escrows.update({
        status: 'refunded',
        resolvedAt: refundedAt,
      }).where({ id: escrowId })
    )

    return operations
  },
})

// Usage Example
async function createEscrow(
  store,
  payerId: string,
  payeeId: string,
  amount: number,
  condition: string
) {
  const escrowId = crypto.randomUUID()

  await store.append(events.escrowCreated.create({
    escrowId,
    payerId,
    payeeId,
    amount,
    condition,
    createdAt: new Date(),
  }))

  return escrowId
}

async function releaseEscrow(store, escrowId: string) {
  await store.append(events.escrowReleased.create({
    escrowId,
    releasedAt: new Date(),
  }))
}

async function refundEscrow(store, escrowId: string, reason: string) {
  await store.append(events.escrowRefunded.create({
    escrowId,
    reason,
    refundedAt: new Date(),
  }))
}

// Example workflow
async function runEscrowWorkflow(store) {
  // 1. Buyer deposits funds into escrow
  const escrowId = await createEscrow(
    store,
    'buyer-123',
    'seller-456',
    50000, // $500
    'Delivery confirmed by buyer'
  )

  // 2. Seller delivers goods...

  // 3a. Buyer confirms delivery -> release to seller
  await releaseEscrow(store, escrowId)

  // OR

  // 3b. Delivery fails -> refund to buyer
  // await refundEscrow(store, escrowId, 'Item not delivered')
}
```

**Key Points:**
- Three-state lifecycle: held -> released/refunded
- Funds are locked (unavailable) but not transferred until release
- Atomic release prevents double-spending
- Support for both happy path (release) and failure path (refund)

---

## Common Patterns Across All Examples

### 1. Validation-First Approach
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

### 2. Consistent Dual Recording
```typescript
// Both sides of transaction record the same outcome
operations.push(
  outgoingTable.insert({ id, success, failureReason }),
  incomingTable.insert({ id, success, failureReason })
)
```

### 3. Atomic Multi-Entity Updates
```typescript
// All operations in array execute atomically
return [
  table1.update({ ... }),
  table2.update({ ... }),
  table3.insert({ ... }),
  historyTable.insert({ ... })
]
```

### 4. Idempotency via Status Checks
```typescript
const record = tables.transfers.select().where({ id }).one()
if (!record || record.status !== 'pending') {
  return [] // Already processed or invalid
}
```

### 5. Audit Trail for Everything
```typescript
// Always insert to history table, even on failure
historyTable.insert({
  id,
  action,
  success,
  failureReason,
  timestamp
})
```

---

## LiveStore-Specific Implementation Considerations

### Current Limitations
1. **No built-in partition hopping**: Unlike Rama's `|hash` operator, LiveStore doesn't have explicit partition control
2. **Single-partition execution**: All materializer operations execute on the same partition
3. **No distributed transaction coordinator**: Must rely on eventlog ordering for consistency

### Workarounds
1. **Event ordering**: Use single eventlog to ensure serializable execution
2. **Optimistic locking**: Check version numbers in validation step
3. **Compensation events**: Create rollback events if complex multi-step transactions fail partway

### Future Enhancements Needed
1. **Partition-aware events**: Events that trigger materializers on multiple partitions
2. **Two-phase commit support**: Built-in coordinator for distributed transactions
3. **Cross-partition queries**: Efficient queries that span multiple partitions
4. **Saga pattern support**: Long-running transactions with compensation

---

## Summary

These examples demonstrate six critical patterns for cross-partition transactions:

1. **Fund Transfer**: Debit source, credit destination with insufficient funds check
2. **Inventory Transfer**: Two-phase (reserve + complete) with cancellation support
3. **Team Reassignment**: Remove from source, add to destination atomically
4. **Order Fulfillment**: Multi-table coordination (inventory, order, customer)
5. **Permission Delegation**: Revoke + grant with role validation
6. **Escrow Pattern**: Hold, then release or refund

**Common Requirements:**
- Validation before state changes
- All-or-nothing semantics
- Consistent failure recording on all sides
- Complete audit trail
- Idempotent execution

**Key to Implementation:**
- Return array of operations from materializer
- All operations execute atomically within partition
- Use eventlog ordering for cross-partition consistency
- Always record outcome (success/failure) for audit
