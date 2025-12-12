# Fanout Patterns for Scalable Delivery - Concrete Examples

## Overview

Fanout is the process of delivering a single event/update to many recipients efficiently. Rama's Twitter clone demonstrates sophisticated fanout with:
- **Partitioned followers** spread across tasks for balanced processing
- **Resumable fanout** with continuation state for fault tolerance
- **Bloom filters** for efficient reply filtering
- **Rate limiting** via `singlePartitionFanoutLimit` and batched queries
- **Multiple fanout paths**: local followers, remote followers, hashtag followers, lists

These patterns are essential for: social feeds, notifications, presence systems, real-time collaboration, and messaging.

---

## 1. Activity Feed Delivery (Twitter-style Timeline Fanout)

**Use Case:** When a user posts content, fan it out to all their followers' home timelines.

**Trigger Event:**
```typescript
type PostCreatedEvent = {
  postId: string;
  authorId: string;
  content: string;
  timestamp: number;
  visibility: 'public' | 'private' | 'unlisted';
};
```

**Schema:**
```typescript
// Partitioned followers for balanced fanout across workers
const schema = {
  // Control structure: which workers handle which author's followers
  partitionedFollowersControl: {
    // authorId -> list of worker/partition IDs
    [authorId: string]: number[];
  },

  // Actual follower data partitioned across workers
  partitionedFollowers: {
    // authorId -> followerId -> follower metadata
    [authorId: string]: {
      [followerId: string]: {
        followerId: string;
        showBoosts: boolean;
        languages?: string[];
        addedAt: number;
      };
    };
  },

  // Resumable fanout state (fault tolerance)
  postIdToFollowerFanouts: {
    [postId: string]: {
      authorId: string;
      nextFollowerIndex: number; // resume from here if interrupted
      status: Post;
      partitionId: number;
    }[];
  },

  // Bloom filter for efficient "is following" checks during reply fanout
  followerBloomFilters: {
    [userId: string]: BloomFilterData; // serialized bloom filter
  },

  // In-memory home timelines (reconstructed on failure)
  homeTimelines: {
    [userId: string]: {
      posts: Array<{ authorId: string; postId: string }>;
      maxSize: 600;
      lastFetch?: { postId: string; index: number };
    };
  }
};
```

**Fanout Implementation:**
```typescript
// Materializer for post fanout
async function fanoutPostToFollowers(event: PostCreatedEvent, ctx: MaterializerContext) {
  const { postId, authorId, visibility } = event;

  // Step 1: Get partition control (which workers handle this author's followers)
  const partitions = await ctx.select(
    ['partitionedFollowersControl', authorId]
  ) ?? [ctx.currentPartitionId];

  // Step 2: Fan out across all partitions
  for (const [index, partitionId] of partitions.entries()) {
    // Skip same partition (already there)
    if (index === 0) continue;

    // Jump to target partition
    await ctx.partitionHop(partitionId);

    // Step 3: Batch fetch followers from this partition
    const followerBatch = await fetchFollowersBatched(
      ctx,
      authorId,
      0, // startIndex
      1000 // batchSize
    );

    // Step 4: Process batch and check if more remain
    let nextIndex = await processFanoutBatch(
      ctx,
      postId,
      event,
      followerBatch
    );

    // Step 5: If not done, save resumable state
    if (nextIndex !== null) {
      await ctx.localTransform(
        ['postIdToFollowerFanouts', postId],
        (existing = []) => [
          ...existing,
          {
            authorId,
            nextFollowerIndex: nextIndex,
            status: event,
            partitionId,
          }
        ]
      );
    }
  }
}

// Helper: Fetch followers in batches with limit
async function fetchFollowersBatched(
  ctx: MaterializerContext,
  authorId: string,
  startIndex: number,
  limit: number
): Promise<Array<Follower & { followerId: string }>> {
  const followers: Follower[] = [];
  let currentIndex = startIndex;

  while (followers.length < limit) {
    // Query next range
    const batch = await ctx.localSelectRange(
      ['partitionedFollowers', authorId],
      { from: currentIndex, limit: 1000 }
    );

    if (!batch || batch.length === 0) break;

    followers.push(...batch);
    currentIndex = batch[batch.length - 1].followerId;

    if (batch.length < 1000) break; // No more followers
  }

  return followers.slice(0, limit);
}

// Helper: Process fanout batch with filtering
async function processFanoutBatch(
  ctx: MaterializerContext,
  postId: string,
  event: PostCreatedEvent,
  followers: Follower[]
): Promise<number | null> {
  const { authorId, content } = event;
  const isReply = content.startsWith('@');

  for (const follower of followers) {
    // Filter 1: Boost preference
    if (event.type === 'boost' && !follower.showBoosts) continue;

    // Filter 2: Language preference
    if (follower.languages && event.language &&
        !follower.languages.includes(event.language)) {
      continue;
    }

    // Filter 3: Reply filtering (only show if following parent author)
    if (isReply) {
      const parentAuthorId = extractParentAuthor(content);

      // Bloom filter check first (fast)
      const bloom = await ctx.select(['followerBloomFilters', follower.followerId]);
      if (!bloom?.contains(parentAuthorId)) continue;

      // Confirm with actual follow relationship
      const actuallyFollows = await ctx.select([
        'followerToFollowees',
        follower.followerId,
        parentAuthorId
      ]);
      if (!actuallyFollows) continue;
    }

    // Add to follower's home timeline (partition hop)
    await ctx.hashPartition(follower.followerId);
    await ctx.localTransform(
      ['homeTimelines', follower.followerId],
      (timeline = { posts: [], maxSize: 600 }) => ({
        ...timeline,
        posts: [
          { authorId, postId },
          ...timeline.posts.slice(0, timeline.maxSize - 1)
        ]
      })
    );
  }

  // Return next index if more followers exist
  return followers.length >= 1000
    ? followers[followers.length - 1].followerId
    : null;
}

// Resumable fanout continuation (runs on startup or retry)
async function continueFollowerFanout(ctx: MaterializerContext) {
  const pendingFanouts = await ctx.allPartitionSelect(['postIdToFollowerFanouts']);

  for (const [postId, fanouts] of Object.entries(pendingFanouts)) {
    for (const fanout of fanouts) {
      await ctx.directPartition(fanout.partitionId);

      const followerBatch = await fetchFollowersBatched(
        ctx,
        fanout.authorId,
        fanout.nextFollowerIndex,
        1000
      );

      const nextIndex = await processFanoutBatch(
        ctx,
        postId,
        fanout.status,
        followerBatch
      );

      // Remove or update continuation state
      if (nextIndex === null) {
        await ctx.localTransform(
          ['postIdToFollowerFanouts', postId],
          (existing) => existing.filter(f => f.partitionId !== fanout.partitionId)
        );
      } else {
        fanout.nextFollowerIndex = nextIndex;
      }
    }
  }
}
```

---

## 2. Notification Broadcast

**Use Case:** Deliver notifications to subscribers (e.g., "Your post was liked by 10 people").

**Trigger Event:**
```typescript
type NotificationEvent = {
  notificationId: string;
  targetUserId: string; // who receives the notification
  type: 'like' | 'comment' | 'mention' | 'follow';
  actorId: string; // who triggered it
  resourceId?: string; // post/comment ID
  timestamp: number;
};
```

**Schema:**
```typescript
const schema = {
  // Notification timelines per user (fixed size)
  userNotifications: {
    [userId: string]: {
      items: Array<{
        notificationId: string;
        type: string;
        actorId: string;
        resourceId?: string;
        timestamp: number;
        read: boolean;
      }>;
      maxSize: 800;
      unreadCount: number;
    };
  },

  // Fanout tracking for batch notifications
  notificationBatchFanouts: {
    [batchId: string]: {
      targetUserIds: string[];
      nextIndex: number;
      notification: NotificationEvent;
    };
  },

  // Notification delivery receipts
  notificationDeliveryStatus: {
    [notificationId: string]: {
      deliveredTo: Set<string>;
      pendingTo: Set<string>;
      failedTo: Set<string>;
    };
  }
};
```

**Fanout Implementation:**
```typescript
// Single notification to one user
async function deliverNotification(
  event: NotificationEvent,
  ctx: MaterializerContext
) {
  const { targetUserId, notificationId, type, actorId, resourceId, timestamp } = event;

  // Check suppression (blocked/muted)
  const isSuppressed = await checkSuppression(ctx, targetUserId, actorId);
  if (isSuppressed) {
    await recordDeliveryStatus(ctx, notificationId, targetUserId, 'suppressed');
    return;
  }

  // Partition to target user
  await ctx.hashPartition(targetUserId);

  // Add to notification timeline
  await ctx.localTransform(
    ['userNotifications', targetUserId],
    (notifications = { items: [], maxSize: 800, unreadCount: 0 }) => {
      const newNotif = {
        notificationId,
        type,
        actorId,
        resourceId,
        timestamp,
        read: false
      };

      return {
        ...notifications,
        items: [newNotif, ...notifications.items.slice(0, notifications.maxSize - 1)],
        unreadCount: notifications.unreadCount + 1
      };
    }
  );

  // Record delivery
  await recordDeliveryStatus(ctx, notificationId, targetUserId, 'delivered');
}

// Batch notification to many users (e.g., poll completed, fanned to all voters)
async function batchNotifyUsers(
  userIds: string[],
  notification: Omit<NotificationEvent, 'targetUserId'>,
  ctx: MaterializerContext
) {
  const batchId = generateId();
  const batchSize = 20000; // Rama's fanoutLimit

  // Process in batches
  for (let i = 0; i < userIds.length; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize);

    // Deliver to each user in batch
    for (const userId of batch) {
      await deliverNotification(
        { ...notification, targetUserId: userId },
        ctx
      );

      // Yield periodically to prevent timeout
      if (batch.indexOf(userId) % 100 === 0) {
        await ctx.yield();
      }
    }

    // Save continuation state if more batches remain
    if (i + batchSize < userIds.length) {
      await ctx.localTransform(
        ['notificationBatchFanouts', batchId],
        () => ({
          targetUserIds: userIds,
          nextIndex: i + batchSize,
          notification
        })
      );
    }
  }
}

// Poll completion example (fan out to all voters + author)
async function notifyPollCompletion(
  pollId: string,
  authorId: string,
  ctx: MaterializerContext
) {
  // Get all voters
  const voters = await ctx.select(['pollVotes', pollId, 'allVoters']);
  const voterIds = voters ? Object.keys(voters) : [];

  // Add author if they didn't vote
  const recipients = new Set([authorId, ...voterIds]);

  // Batch notify
  await batchNotifyUsers(
    Array.from(recipients),
    {
      notificationId: generateId(),
      type: 'poll_complete',
      actorId: authorId,
      resourceId: pollId,
      timestamp: Date.now()
    },
    ctx
  );
}

// Helper: Track delivery status
async function recordDeliveryStatus(
  ctx: MaterializerContext,
  notificationId: string,
  userId: string,
  status: 'delivered' | 'suppressed' | 'failed'
) {
  await ctx.localTransform(
    ['notificationDeliveryStatus', notificationId],
    (existing = { deliveredTo: new Set(), pendingTo: new Set(), failedTo: new Set() }) => {
      existing.pendingTo.delete(userId);

      if (status === 'delivered') {
        existing.deliveredTo.add(userId);
      } else if (status === 'failed') {
        existing.failedTo.add(userId);
      }

      return existing;
    }
  );
}
```

---

## 3. Presence Updates (Online/Offline Status)

**Use Case:** When a user goes online/offline, notify all their contacts.

**Trigger Event:**
```typescript
type PresenceEvent = {
  userId: string;
  status: 'online' | 'away' | 'offline';
  lastSeen: number;
  deviceId?: string;
};
```

**Schema:**
```typescript
const schema = {
  // User presence state
  userPresence: {
    [userId: string]: {
      status: 'online' | 'away' | 'offline';
      lastSeen: number;
      devices: Set<string>; // multi-device support
    };
  },

  // Bidirectional contact graph
  userContacts: {
    [userId: string]: Set<string>; // set of contact user IDs
  },

  // Partitioned contacts for balanced fanout
  partitionedContacts: {
    [userId: string]: {
      [contactId: string]: {
        contactId: string;
        partition: number;
      };
    };
  },

  // Presence subscriptions (who's watching whom)
  presenceSubscriptions: {
    [watchedUserId: string]: Set<string>; // set of subscriber IDs
  },

  // Resumable presence fanout
  presenceFanoutContinuations: {
    [fanoutId: string]: {
      userId: string;
      presenceUpdate: PresenceEvent;
      nextContactIndex: number;
    };
  }
};
```

**Fanout Implementation:**
```typescript
// Fan out presence update to all contacts
async function fanoutPresenceUpdate(
  event: PresenceEvent,
  ctx: MaterializerContext
) {
  const { userId, status, lastSeen, deviceId } = event;

  // Update own presence
  await ctx.hashPartition(userId);
  await ctx.localTransform(
    ['userPresence', userId],
    (presence = { status: 'offline', lastSeen: 0, devices: new Set() }) => {
      if (status === 'offline' && deviceId) {
        presence.devices.delete(deviceId);
        // Still online if other devices connected
        if (presence.devices.size > 0) {
          return { ...presence, lastSeen };
        }
      } else if (status === 'online' && deviceId) {
        presence.devices.add(deviceId);
      }

      return { status, lastSeen, devices: presence.devices };
    }
  );

  // Get all subscribers
  const subscribers = await ctx.select(['presenceSubscriptions', userId]) ?? new Set();

  // Rate-limited fanout
  const batchSize = 5000;
  const subscriberArray = Array.from(subscribers);

  for (let i = 0; i < subscriberArray.length; i += batchSize) {
    const batch = subscriberArray.slice(i, i + batchSize);

    // Deliver to each subscriber
    for (const subscriberId of batch) {
      await deliverPresenceUpdate(ctx, subscriberId, userId, status, lastSeen);

      // Yield every 100 updates
      if ((i + batch.indexOf(subscriberId)) % 100 === 0) {
        await ctx.yield();
      }
    }

    // Save continuation if more remain
    if (i + batchSize < subscriberArray.length) {
      const fanoutId = `${userId}-${lastSeen}`;
      await ctx.localTransform(
        ['presenceFanoutContinuations', fanoutId],
        () => ({
          userId,
          presenceUpdate: event,
          nextContactIndex: i + batchSize
        })
      );
    }
  }
}

// Deliver presence update to single subscriber
async function deliverPresenceUpdate(
  ctx: MaterializerContext,
  subscriberId: string,
  userId: string,
  status: string,
  lastSeen: number
) {
  await ctx.hashPartition(subscriberId);

  // Update subscriber's view of user's presence
  await ctx.localTransform(
    ['contactPresenceCache', subscriberId, userId],
    () => ({ status, lastSeen })
  );

  // Emit real-time event to connected clients
  await ctx.emit('presence-update', {
    userId,
    status,
    lastSeen
  });
}

// Optimize: Subscribe to contact's presence
async function subscribeToPresence(
  subscriberId: string,
  targetUserId: string,
  ctx: MaterializerContext
) {
  await ctx.hashPartition(targetUserId);

  await ctx.localTransform(
    ['presenceSubscriptions', targetUserId],
    (subs = new Set()) => {
      subs.add(subscriberId);
      return subs;
    }
  );

  // Return current presence
  const presence = await ctx.select(['userPresence', targetUserId]);
  return presence ?? { status: 'offline', lastSeen: 0 };
}
```

---

## 4. Collaborative Cursor Sync (Real-time Document)

**Use Case:** Broadcast cursor positions to all active viewers of a document.

**Trigger Event:**
```typescript
type CursorMoveEvent = {
  documentId: string;
  userId: string;
  position: { x: number; y: number };
  selection?: { start: number; end: number };
  timestamp: number;
};
```

**Schema:**
```typescript
const schema = {
  // Active document viewers (ephemeral, not persisted)
  documentViewers: {
    [documentId: string]: {
      viewers: Map<string, {
        userId: string;
        lastActivity: number;
        connectionId: string;
      }>;
    };
  },

  // Current cursor positions (ephemeral)
  documentCursors: {
    [documentId: string]: {
      [userId: string]: {
        position: { x: number; y: number };
        selection?: { start: number; end: number };
        timestamp: number;
      };
    };
  },

  // Rate limiting state
  cursorBroadcastThrottles: {
    [documentId: string]: {
      [userId: string]: {
        lastBroadcast: number;
        pendingUpdate?: CursorMoveEvent;
      };
    };
  }
};
```

**Fanout Implementation:**
```typescript
// Broadcast cursor update to all document viewers
async function broadcastCursorUpdate(
  event: CursorMoveEvent,
  ctx: MaterializerContext
) {
  const { documentId, userId, position, selection, timestamp } = event;

  // Throttle: Only broadcast every 50ms per user
  await ctx.hashPartition(documentId);

  const shouldThrottle = await ctx.localTransform(
    ['cursorBroadcastThrottles', documentId, userId],
    (throttle = { lastBroadcast: 0 }) => {
      const now = Date.now();
      const elapsed = now - throttle.lastBroadcast;

      if (elapsed < 50) {
        // Save pending update
        return { ...throttle, pendingUpdate: event };
      }

      // Broadcast now
      return { lastBroadcast: now, pendingUpdate: undefined };
    }
  );

  if (shouldThrottle?.pendingUpdate) {
    // Throttled, will broadcast later
    return;
  }

  // Update cursor position
  await ctx.localTransform(
    ['documentCursors', documentId, userId],
    () => ({ position, selection, timestamp })
  );

  // Get all viewers
  const viewers = await ctx.select(['documentViewers', documentId, 'viewers']);
  if (!viewers || viewers.size === 0) return;

  // Broadcast to all viewers except sender (no partition hopping needed)
  for (const [viewerId, viewer] of viewers.entries()) {
    if (viewerId === userId) continue;

    // Emit to connected client
    await ctx.emitToConnection(viewer.connectionId, 'cursor-update', {
      documentId,
      userId,
      position,
      selection,
      timestamp
    });
  }
}

// Batch broadcast: Send all cursor positions to new viewer
async function syncAllCursorsToNewViewer(
  documentId: string,
  newViewerId: string,
  connectionId: string,
  ctx: MaterializerContext
) {
  await ctx.hashPartition(documentId);

  // Add to viewers
  await ctx.localTransform(
    ['documentViewers', documentId, 'viewers'],
    (viewers = new Map()) => {
      viewers.set(newViewerId, {
        userId: newViewerId,
        lastActivity: Date.now(),
        connectionId
      });
      return viewers;
    }
  );

  // Get all current cursors
  const allCursors = await ctx.select(['documentCursors', documentId]) ?? {};

  // Batch send to new viewer
  await ctx.emitToConnection(connectionId, 'all-cursors', {
    documentId,
    cursors: allCursors
  });
}

// Cleanup: Remove inactive viewers
async function pruneInactiveViewers(
  documentId: string,
  ctx: MaterializerContext
) {
  await ctx.hashPartition(documentId);

  const inactiveTimeout = 5 * 60 * 1000; // 5 minutes
  const now = Date.now();

  await ctx.localTransform(
    ['documentViewers', documentId, 'viewers'],
    (viewers = new Map()) => {
      for (const [userId, viewer] of viewers.entries()) {
        if (now - viewer.lastActivity > inactiveTimeout) {
          viewers.delete(userId);

          // Notify others that user left
          for (const [otherUserId, otherViewer] of viewers.entries()) {
            ctx.emitToConnection(otherViewer.connectionId, 'viewer-left', {
              documentId,
              userId
            });
          }
        }
      }
      return viewers;
    }
  );
}
```

---

## 5. Mention Fanout (@username notifications)

**Use Case:** When a post mentions users, deliver notifications to all mentioned.

**Trigger Event:**
```typescript
type MentionEvent = {
  postId: string;
  authorId: string;
  mentionedUsernames: string[];
  content: string;
  timestamp: number;
};
```

**Schema:**
```typescript
const schema = {
  // Username to user ID mapping
  usernameToUserId: {
    [username: string]: string;
  },

  // Mention notifications (separate from regular notifications)
  userMentions: {
    [userId: string]: Array<{
      mentionId: string;
      postId: string;
      authorId: string;
      timestamp: number;
      read: boolean;
    }>;
  },

  // Suppression rules (don't notify if blocked/muted)
  userSuppressions: {
    [userId: string]: {
      blocked: Set<string>;
      muted: Set<string>;
    };
  }
};
```

**Fanout Implementation:**
```typescript
// Fan out mentions to all mentioned users
async function fanoutMentions(
  event: MentionEvent,
  ctx: MaterializerContext
) {
  const { postId, authorId, mentionedUsernames, content, timestamp } = event;

  // Resolve usernames to user IDs
  const mentionedUserIds = await Promise.all(
    mentionedUsernames.map(async (username) => {
      const userId = await ctx.select(['usernameToUserId', username]);
      return userId;
    })
  );

  // Filter out nulls and duplicates
  const validUserIds = [...new Set(mentionedUserIds.filter(Boolean))];

  // Deliver to each mentioned user
  for (const mentionedUserId of validUserIds) {
    // Skip self-mentions
    if (mentionedUserId === authorId) continue;

    // Check suppression
    await ctx.hashPartition(mentionedUserId);
    const suppressions = await ctx.select(['userSuppressions', mentionedUserId]);

    if (suppressions?.blocked.has(authorId) || suppressions?.muted.has(authorId)) {
      continue; // Suppressed
    }

    // Additional filter: If it's a reply, only notify if recipient follows parent author
    if (isReplyMention(content, mentionedUserId)) {
      const parentAuthorId = extractParentAuthor(content);
      if (parentAuthorId !== mentionedUserId) {
        // Check if mentioned user follows parent author
        const follows = await ctx.select([
          'followerToFollowees',
          mentionedUserId,
          parentAuthorId
        ]);

        if (!follows) continue; // Don't spam with reply mentions
      }
    }

    // Deliver mention notification
    await ctx.localTransform(
      ['userMentions', mentionedUserId],
      (mentions = []) => {
        const mention = {
          mentionId: `${postId}-${mentionedUserId}`,
          postId,
          authorId,
          timestamp,
          read: false
        };

        return [mention, ...mentions.slice(0, 499)]; // Keep last 500
      }
    );

    // Emit real-time notification
    await ctx.emit('mention-notification', {
      mentionedUserId,
      postId,
      authorId
    });
  }
}

// Helper: Check if mention is in a reply context
function isReplyMention(content: string, mentionedUserId: string): boolean {
  // If content starts with @mention, it's likely a reply
  return content.trimStart().startsWith(`@`);
}

// Helper: Extract parent author from reply
function extractParentAuthor(content: string): string | null {
  const match = content.match(/^@(\w+)/);
  return match ? match[1] : null;
}
```

---

## 6. Channel Message Delivery (Slack/Discord-style)

**Use Case:** Broadcast messages to all channel members efficiently.

**Trigger Event:**
```typescript
type ChannelMessageEvent = {
  messageId: string;
  channelId: string;
  authorId: string;
  content: string;
  threadId?: string;
  timestamp: number;
};
```

**Schema:**
```typescript
const schema = {
  // Channel membership (partitioned for large channels)
  channelMembers: {
    [channelId: string]: {
      [memberId: string]: {
        memberId: string;
        joinedAt: number;
        lastRead?: number;
        notificationPreference: 'all' | 'mentions' | 'none';
      };
    };
  },

  // Partitioned channel members (for channels with >10k members)
  partitionedChannelMembers: {
    [channelId: string]: {
      partitions: number[];
      members: {
        [partitionId: number]: {
          [memberId: string]: Member;
        };
      };
    };
  },

  // Unread message counters per user per channel
  userChannelUnreads: {
    [userId: string]: {
      [channelId: string]: {
        count: number;
        lastMessageId: string;
        lastMessageTimestamp: number;
      };
    };
  },

  // Fanout continuation for large channels
  messageFanoutContinuations: {
    [messageId: string]: {
      channelId: string;
      message: ChannelMessageEvent;
      nextMemberIndex: number;
      partitionId: number;
    }[];
  }
};
```

**Fanout Implementation:**
```typescript
// Broadcast message to all channel members
async function broadcastChannelMessage(
  event: ChannelMessageEvent,
  ctx: MaterializerContext
) {
  const { messageId, channelId, authorId, content, timestamp } = event;

  // Check if channel is partitioned (large channel)
  await ctx.hashPartition(channelId);
  const partitionInfo = await ctx.select(['partitionedChannelMembers', channelId]);

  if (partitionInfo?.partitions) {
    // Large channel: partitioned fanout
    await fanoutToPartitionedChannel(event, partitionInfo, ctx);
  } else {
    // Small channel: direct fanout
    await fanoutToSmallChannel(event, ctx);
  }
}

// Small channel fanout (< 10k members)
async function fanoutToSmallChannel(
  event: ChannelMessageEvent,
  ctx: MaterializerContext
) {
  const { channelId, authorId, messageId, timestamp } = event;

  const members = await ctx.select(['channelMembers', channelId]) ?? {};

  for (const [memberId, member] of Object.entries(members)) {
    // Skip author (they already have the message)
    if (memberId === authorId) continue;

    // Check notification preference
    if (member.notificationPreference === 'none') continue;
    if (member.notificationPreference === 'mentions' && !isMentioned(event.content, memberId)) {
      continue;
    }

    // Update unread counter
    await ctx.hashPartition(memberId);
    await ctx.localTransform(
      ['userChannelUnreads', memberId, channelId],
      (unread = { count: 0, lastMessageId: '', lastMessageTimestamp: 0 }) => ({
        count: unread.count + 1,
        lastMessageId: messageId,
        lastMessageTimestamp: timestamp
      })
    );

    // Emit real-time message
    await ctx.emit('channel-message', {
      channelId,
      messageId,
      authorId,
      content: event.content,
      timestamp
    });
  }
}

// Large channel fanout (partitioned across workers)
async function fanoutToPartitionedChannel(
  event: ChannelMessageEvent,
  partitionInfo: any,
  ctx: MaterializerContext
) {
  const { channelId, messageId } = event;
  const batchSize = 10000;

  for (const partitionId of partitionInfo.partitions) {
    await ctx.directPartition(partitionId);

    const partitionMembers = await ctx.localSelect([
      'partitionedChannelMembers',
      channelId,
      'members',
      partitionId
    ]) ?? {};

    const memberIds = Object.keys(partitionMembers);

    // Process in batches
    for (let i = 0; i < memberIds.length; i += batchSize) {
      const batch = memberIds.slice(i, i + batchSize);

      for (const memberId of batch) {
        const member = partitionMembers[memberId];

        // Apply filters
        if (memberId === event.authorId) continue;
        if (member.notificationPreference === 'none') continue;

        // Deliver message
        await deliverChannelMessage(ctx, memberId, event);

        // Yield periodically
        if ((i + batch.indexOf(memberId)) % 100 === 0) {
          await ctx.yield();
        }
      }

      // Save continuation if more batches remain
      if (i + batchSize < memberIds.length) {
        await ctx.localTransform(
          ['messageFanoutContinuations', messageId],
          (continuations = []) => [
            ...continuations,
            {
              channelId,
              message: event,
              nextMemberIndex: i + batchSize,
              partitionId
            }
          ]
        );
      }
    }
  }
}

// Deliver message to single member
async function deliverChannelMessage(
  ctx: MaterializerContext,
  memberId: string,
  event: ChannelMessageEvent
) {
  const { channelId, messageId, timestamp } = event;

  await ctx.hashPartition(memberId);

  // Update unread
  await ctx.localTransform(
    ['userChannelUnreads', memberId, channelId],
    (unread = { count: 0, lastMessageId: '', lastMessageTimestamp: 0 }) => ({
      count: unread.count + 1,
      lastMessageId: messageId,
      lastMessageTimestamp: timestamp
    })
  );

  // Emit real-time
  await ctx.emit('channel-message', event);
}

// Helper: Check if user is mentioned
function isMentioned(content: string, userId: string): boolean {
  return content.includes(`@${userId}`) || content.includes('@channel');
}
```

---

## Summary: Key Patterns

### 1. Partitioned Recipients
- Spread recipients across multiple partitions/workers
- Use `partitionedFollowersControl` to track which partitions handle which users
- Prevents any single worker from being overwhelmed

### 2. Resumable Fanout
- Save continuation state (`nextIndex`, `partitionId`) for incomplete fanouts
- Resume from saved state on retry or startup
- Critical for fault tolerance with large recipient lists

### 3. Bloom Filters for Efficient Filtering
- Cache "is following" checks in bloom filters
- Fast negative checks before expensive database lookups
- Especially useful for reply filtering

### 4. Rate Limiting & Batching
- Process recipients in batches (e.g., 1000-20000 per batch)
- Use `ctx.yield()` to prevent blocking
- Save state between batches for resumability

### 5. Delivery Tracking
- Track `deliveredTo`, `pendingTo`, `failedTo` sets
- Enable retry logic and observability
- Support idempotent delivery

### 6. Suppression & Filtering
- Check blocked/muted relationships before delivery
- Apply content filters (language, boost preference, mentions-only)
- Reduce unnecessary notifications

These patterns enable LiveStore to handle social-scale fanout scenarios efficiently while maintaining fault tolerance and consistency.
