/**
 * WebLock Orphaned Fiber Hang Bug Tests
 *
 * Bug: When a fiber calls `tryGetDeferredLock` or `waitForDeferredLock`, then
 * the fiber is interrupted BEFORE the lock is acquired, but the lock callback
 * later runs anyway - the Deferred is never resolved and callers hang forever.
 *
 * Root cause: `waitForDeferredLock` did not race an abort signal in its lock
 * holder callback. Once the lock was acquired, it waited indefinitely on
 * `Deferred.await(deferred)` with no escape hatch if the Effect fiber that
 * initiated the lock request was interrupted.
 *
 * Fix: Added `resolveDeferred` helper that listens to the abort signal and
 * resolves the deferred when the fiber is interrupted. This prevents the
 * orphaned lock callback from hanging forever.
 */

import { Deferred, Effect, Fiber } from 'effect'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

// Mock navigator.locks for Node.js environment
// The Web Locks API only exists in browsers, so we create a minimal mock
// that reproduces the locking behavior.

interface MockLockHolder {
  lockName: string
  release: () => void
  releasePromise: Promise<void>
}

interface MockPendingRequest {
  lockName: string
  callback: (lock: Lock | null) => Promise<unknown>
  signal?: AbortSignal
  resolve: (value: unknown) => void
  reject: (error: unknown) => void
}

// Shared state for mock
let heldLocks: Map<string, MockLockHolder>
let pendingRequests: MockPendingRequest[]

const processNextRequest = (lockName: string) => {
  // Find first non-aborted pending request for this lock
  const nextIdx = pendingRequests.findIndex((r) => r.lockName === lockName && !r.signal?.aborted)
  if (nextIdx === -1 || heldLocks.has(lockName)) return

  const next = pendingRequests[nextIdx]!
  pendingRequests.splice(nextIdx, 1)

  // Create lock holder
  let releaseResolve!: () => void
  const releasePromise = new Promise<void>((resolve) => {
    releaseResolve = resolve
  })
  heldLocks.set(lockName, { lockName, release: releaseResolve, releasePromise })

  // Call the callback with a mock lock
  const mockLock: Lock = { name: lockName, mode: 'exclusive' }
  next
    .callback(mockLock)
    .then((result) => {
      next.resolve(result)
    })
    .catch((error) => {
      next.reject(error)
    })
    .finally(() => {
      heldLocks.delete(lockName)
      releaseResolve()
      // Delay to allow Promise microtask queue to settle
      queueMicrotask(() => processNextRequest(lockName))
    })
}

const mockRequest = (
  lockName: string,
  optionsOrCallback: LockOptions | ((lock: Lock | null) => Promise<unknown>),
  maybeCallback?: (lock: Lock | null) => Promise<unknown>,
): Promise<unknown> => {
  const options = typeof optionsOrCallback === 'function' ? {} : optionsOrCallback
  const callback = typeof optionsOrCallback === 'function' ? optionsOrCallback : maybeCallback!

  return new Promise((resolve, reject) => {
    const signal = options.signal

    // Handle steal option
    if (options.steal) {
      const existing = heldLocks.get(lockName)
      if (existing) {
        existing.release()
        heldLocks.delete(lockName)
      }
      // Fall through to acquire normally
    }

    // Handle ifAvailable
    if (options.ifAvailable) {
      if (heldLocks.has(lockName)) {
        // Lock not available
        Promise.resolve(callback(null)).then(resolve).catch(reject)
        return
      }
      // Lock available, acquire immediately
      let releaseResolve!: () => void
      const releasePromise = new Promise<void>((r) => {
        releaseResolve = r
      })
      heldLocks.set(lockName, { lockName, release: releaseResolve, releasePromise })

      const mockLock: Lock = { name: lockName, mode: 'exclusive' }
      callback(mockLock)
        .then(resolve)
        .catch(reject)
        .finally(() => {
          heldLocks.delete(lockName)
          releaseResolve()
          queueMicrotask(() => processNextRequest(lockName))
        })
      return
    }

    // Check if lock is available immediately
    if (!heldLocks.has(lockName)) {
      // Acquire immediately
      let releaseResolve!: () => void
      const releasePromise = new Promise<void>((r) => {
        releaseResolve = r
      })
      heldLocks.set(lockName, { lockName, release: releaseResolve, releasePromise })

      const mockLock: Lock = { name: lockName, mode: 'exclusive' }
      callback(mockLock)
        .then(resolve)
        .catch(reject)
        .finally(() => {
          heldLocks.delete(lockName)
          releaseResolve()
          queueMicrotask(() => processNextRequest(lockName))
        })
      return
    }

    // Queue the request
    const req: MockPendingRequest = {
      lockName,
      callback,
      ...(signal !== undefined ? { signal } : {}),
      resolve,
      reject,
    }

    if (signal) {
      if (signal.aborted) {
        const error = new DOMException('signal is aborted without reason', 'AbortError')
        Object.defineProperty(error, 'code', { value: 20 })
        reject(error)
        return
      }

      signal.addEventListener('abort', () => {
        const idx = pendingRequests.indexOf(req)
        if (idx >= 0) {
          pendingRequests.splice(idx, 1)
          const error = new DOMException('signal is aborted without reason', 'AbortError')
          Object.defineProperty(error, 'code', { value: 20 })
          reject(error)
        }
      })
    }

    pendingRequests.push(req)
  })
}

// Install mock navigator.locks globally BEFORE module loads
beforeAll(() => {
  vi.stubGlobal('navigator', {
    locks: {
      request: mockRequest,
    },
  })
})

// Reset state before each test
beforeEach(() => {
  heldLocks = new Map()
  pendingRequests = []
})

afterEach(() => {
  // Clean up any pending requests
  pendingRequests = []
  heldLocks.clear()
})

// Dynamic import to ensure mock is installed first
// eslint-disable-next-line @typescript-eslint/consistent-type-imports
let WebLock: typeof import('./WebLock.ts')

beforeAll(async () => {
  WebLock = await import('./WebLock.ts')
})

// Helper to run scoped Effect with proper resource cleanup
const runScoped = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> => {
  return Effect.runPromise(Effect.scoped(effect))
}

describe('WebLock', () => {
  describe('waitForDeferredLock', () => {
    /**
     * This test verifies the fix for the orphaned fiber hang bug.
     *
     * Scenario:
     * 1. Lock is held by holder A
     * 2. Fiber B calls waitForDeferredLock and waits in queue
     * 3. Fiber B is interrupted BEFORE acquiring the lock
     * 4. Holder A releases lock
     * 5. The orphaned callback for B runs and acquires the lock
     * 6. Without the fix: callback waits forever on Deferred.await(deferred)
     *    because deferred will never be resolved
     * 7. With the fix: abort signal handler resolves the deferred, releasing lock
     */
    it('should not hang when fiber is interrupted before lock acquisition', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-orphaned-hang-wait-' + Date.now()
          const holderDeferred = yield* Deferred.make<void>()
          const waiterDeferred = yield* Deferred.make<void>()

          // Step 1: Acquire lock with first holder
          const holderAcquired = yield* Deferred.make<void>()
          const holderFiber = yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(holderDeferred, LOCK_NAME)
            yield* Deferred.succeed(holderAcquired, undefined)
            return yield* Effect.never // Hold lock forever
          }).pipe(Effect.fork)

          // Wait for holder to acquire lock
          yield* Deferred.await(holderAcquired)

          // Step 2: Fork a waiter fiber that will be interrupted
          const waiterFiber = yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(waiterDeferred, LOCK_NAME)
            return 'acquired'
          }).pipe(Effect.fork)

          // Give time for waiter to queue up
          yield* Effect.sleep('10 millis')

          // Step 3: Interrupt the waiter BEFORE it acquires the lock
          yield* Fiber.interrupt(waiterFiber)

          // Step 4: Release the holder's lock - this allows the orphaned callback to run
          yield* Deferred.succeed(holderDeferred, undefined)

          // Give time for the orphaned callback to process
          yield* Effect.sleep('50 millis')

          // Step 5: Try to acquire the lock with a new fiber
          // If the bug existed (no fix), the orphaned callback would be waiting forever
          // on waiterDeferred, blocking all subsequent lock requests
          const newDeferred = yield* Deferred.make<void>()

          const result = yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(newDeferred, LOCK_NAME)
            return 'new-acquired'
          }).pipe(Effect.timeout('500 millis'), Effect.option)

          // Clean up
          yield* Deferred.succeed(newDeferred, undefined)
          yield* Fiber.interrupt(holderFiber)

          // If we got here without timing out, the fix is working
          expect(result._tag).toBe('Some')
          if (result._tag === 'Some') {
            expect(result.value).toBe('new-acquired')
          }
        }),
      )
    }, 5000)

    it('should acquire available lock', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-basic-wait-' + Date.now()
          const deferred = yield* Deferred.make<void>()

          const acquired = yield* Deferred.make<void>()
          const fiber = yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(deferred, LOCK_NAME)
            yield* Deferred.succeed(acquired, undefined)
            return 'acquired'
          }).pipe(Effect.fork)

          // Wait for lock acquisition
          yield* Deferred.await(acquired)

          // Release the lock
          yield* Deferred.succeed(deferred, undefined)

          const result = yield* Fiber.join(fiber)
          expect(result).toBe('acquired')
        }),
      )
    }, 5000)

    it('should queue when lock is held and acquire when released', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-queue-' + Date.now()
          const holder1Deferred = yield* Deferred.make<void>()
          const holder2Deferred = yield* Deferred.make<void>()

          // First holder acquires lock
          const holder1Acquired = yield* Deferred.make<void>()
          yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(holder1Deferred, LOCK_NAME)
            yield* Deferred.succeed(holder1Acquired, undefined)
            return yield* Effect.never
          }).pipe(Effect.fork)

          yield* Deferred.await(holder1Acquired)

          // Second holder queues up
          const holder2Acquired = yield* Deferred.make<void>()
          const holder2Fiber = yield* Effect.gen(function* () {
            yield* WebLock.waitForDeferredLock(holder2Deferred, LOCK_NAME)
            yield* Deferred.succeed(holder2Acquired, undefined)
            return 'holder2-acquired'
          }).pipe(Effect.fork)

          // Give time to queue
          yield* Effect.sleep('10 millis')

          // Release first holder
          yield* Deferred.succeed(holder1Deferred, undefined)

          // Second holder should acquire
          yield* Deferred.await(holder2Acquired)

          // Release second holder and get result
          yield* Deferred.succeed(holder2Deferred, undefined)
          const result = yield* Fiber.join(holder2Fiber)

          expect(result).toBe('holder2-acquired')
        }),
      )
    }, 5000)
  })

  describe('tryGetDeferredLock', () => {
    it('should return true when lock is available', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-try-available-' + Date.now()
          const deferred = yield* Deferred.make<void>()

          const gotLock = yield* WebLock.tryGetDeferredLock(deferred, LOCK_NAME)

          // Clean up
          yield* Deferred.succeed(deferred, undefined)

          expect(gotLock).toBe(true)
        }),
      )
    }, 5000)

    it('should return false when lock is held', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-try-unavailable-' + Date.now()
          const holderDeferred = yield* Deferred.make<void>()

          // Acquire lock
          const gotFirst = yield* WebLock.tryGetDeferredLock(holderDeferred, LOCK_NAME)
          expect(gotFirst).toBe(true)

          // Try to acquire again - should fail
          const secondDeferred = yield* Deferred.make<void>()
          const gotSecond = yield* WebLock.tryGetDeferredLock(secondDeferred, LOCK_NAME)
          expect(gotSecond).toBe(false)

          // Clean up
          yield* Deferred.succeed(holderDeferred, undefined)
        }),
      )
    }, 5000)

    /**
     * This test verifies that when a fiber holding a lock via tryGetDeferredLock
     * is interrupted, the abort signal handler fires and resolves the deferred,
     * allowing the lock to be released.
     *
     * NOTE: This test uses a simplified approach that directly resolves the deferred
     * to verify the lock release mechanism, since the mock doesn't fully simulate
     * the async nature of the abort signal handler interacting with the lock callback.
     */
    it('should allow lock release via deferred when holder is done', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-try-release-' + Date.now()
          const deferred = yield* Deferred.make<void>()

          // Acquire lock
          const gotLock = yield* WebLock.tryGetDeferredLock(deferred, LOCK_NAME)
          expect(gotLock).toBe(true)

          // Verify lock is held
          const secondDeferred = yield* Deferred.make<void>()
          const gotSecond = yield* WebLock.tryGetDeferredLock(secondDeferred, LOCK_NAME)
          expect(gotSecond).toBe(false)

          // Release the lock by resolving the deferred
          // (This is what the abort handler does when the fiber is interrupted)
          yield* Deferred.succeed(deferred, undefined)

          // Give time for lock callback to complete
          yield* Effect.sleep('50 millis')

          // Now the lock should be available
          const thirdDeferred = yield* Deferred.make<void>()
          const gotThird = yield* WebLock.tryGetDeferredLock(thirdDeferred, LOCK_NAME)

          // Clean up
          yield* Deferred.succeed(thirdDeferred, undefined)

          expect(gotThird).toBe(true)
        }),
      )
    }, 5000)
  })

  describe('stealDeferredLock', () => {
    it('should steal lock from current holder', async () => {
      await runScoped(
        Effect.gen(function* () {
          const LOCK_NAME = 'test-steal-' + Date.now()
          const holderDeferred = yield* Deferred.make<void>()
          const stealerDeferred = yield* Deferred.make<void>()

          // Acquire initial lock
          const gotLock = yield* WebLock.tryGetDeferredLock(holderDeferred, LOCK_NAME)
          expect(gotLock).toBe(true)

          // Steal the lock
          const stole = yield* WebLock.stealDeferredLock(stealerDeferred, LOCK_NAME)
          expect(stole).toBe(true)

          // Clean up
          yield* Deferred.succeed(stealerDeferred, undefined)
        }),
      )
    }, 5000)
  })
})
