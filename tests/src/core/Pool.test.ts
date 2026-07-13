import { describe, expect, it } from 'vitest'
import { Pool } from '@src/core'
import {
	createErrorRecorder,
	createGate,
	createRecorder,
	createResourceFactory,
	recordEmitterEvents,
	waitForDelay,
} from '../../setup.js'

// src/core/workers/Pool.ts — the bounded resource pool. Real behaviour, no mocks:
// `create` hands out monotonically-numbered resources via a recorder so the test can
// assert exactly how many were made, and a real AbortController drives the
// abort-cancellation test (AGENTS §16). Beyond the per-feature cases, production-grade
// stress sections cover: high contention (waiters ≫ max — FIFO fairness + never over
// `max`), handoff validation under stress (no dead resource ever served), create /
// validate / destroy hooks THROWING (incl. the validate-throw fix that stops a parked
// waiter hanging), rapid acquire/release churn (count consistency), and teardown with
// active leases + parked waiters + an in-flight create at once. The shared
// `createResourceFactory` (tests/setup.ts) hands out the monotonically-numbered
// resources (its `created` recorder asserts the count); the pool ignores its `destroyed`.

describe('Pool — create up to max', () => {
	it('creates a fresh resource per acquire until max, never beyond', async () => {
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 2 })

		const first = await pool.acquire()
		const second = await pool.acquire()
		expect([first.value, second.value]).toEqual([0, 1])
		expect(created.count).toBe(2)
		expect(pool.size).toBe(2)
		expect(pool.active).toBe(2)
		expect(pool.idle).toBe(0)
	})
})

describe('Pool — idle reuse', () => {
	it('hands a released resource back to the next acquire (same value, no new create)', async () => {
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({ create })

		const token = await pool.acquire()
		expect(token.value).toBe(0)
		expect(pool.active).toBe(1)
		expect(pool.idle).toBe(0)

		token.release()
		expect(pool.active).toBe(0)
		expect(pool.idle).toBe(1)

		// The next acquire reuses the idle resource — no second create.
		const again = await pool.acquire()
		expect(again.value).toBe(0)
		expect(created.count).toBe(1)
		expect(pool.size).toBe(1)
	})
})

describe('Pool — max backpressure + FIFO handoff', () => {
	it('parks an acquire at max with none idle, then a release hands off FIFO', async () => {
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 1 })

		const first = await pool.acquire()
		expect(first.value).toBe(0)

		// Two more acquires must WAIT — the pool is at max with nothing idle.
		const order: string[] = []
		const second = pool.acquire().then((token) => {
			order.push('second')
			return token
		})
		const third = pool.acquire().then((token) => {
			order.push('third')
			return token
		})
		await waitForDelay(10)
		expect(order).toEqual([])
		expect(created.count).toBe(1)

		// Releasing hands the SAME resource to the oldest waiter (FIFO: second first).
		first.release()
		const secondToken = await second
		expect(secondToken.value).toBe(0)
		expect(order).toEqual(['second'])
		expect(created.count).toBe(1)

		// Releasing again serves the next waiter.
		secondToken.release()
		const thirdToken = await third
		expect(thirdToken.value).toBe(0)
		expect(order).toEqual(['second', 'third'])
		expect(created.count).toBe(1)
	})
})

describe('Pool — aborted waiting acquire', () => {
	it('rejects an aborted waiter and does not leak it (a later release still serves)', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 1 })

		const held = await pool.acquire()
		const controller = new AbortController()
		const waiting = pool.acquire(controller.signal)
		// A second waiter with no signal stays parked behind the aborted one.
		const survivor = pool.acquire()
		await waitForDelay(10)

		controller.abort(new Error('gave up'))
		await expect(waiting).rejects.toThrow('gave up')

		// The aborted waiter was removed, so releasing serves the survivor (not a ghost).
		held.release()
		const served = await survivor
		expect(served.value).toBe(0)
		expect(pool.active).toBe(1)
	})

	it('rejects immediately when acquire is called with an already-aborted signal', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create })
		const controller = new AbortController()
		controller.abort(new Error('already gone'))
		await expect(pool.acquire(controller.signal)).rejects.toThrow('already gone')
	})
})

describe('Pool — validate', () => {
	it('destroys an invalid idle resource and replaces it on the next acquire', async () => {
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		let healthy = true
		const pool = new Pool<number>({
			create,
			destroy: (value) => destroyed.handler(value),
			validate: () => healthy,
		})

		const token = await pool.acquire()
		expect(token.value).toBe(0)
		token.release()

		// The idle resource now fails validation — acquire destroys it and creates a fresh one.
		healthy = false
		const replacement = await pool.acquire()
		expect(replacement.value).toBe(1)
		expect(destroyed.calls).toEqual([[0]])
		expect(created.count).toBe(2)
	})
})

describe('Pool — validate on FIFO handoff', () => {
	it('destroys a resource that went invalid while leased and serves the waiter a fresh one', async () => {
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		// `validate` fails only for resource 0 — the one we taint while it is leased.
		const invalid = new Set<number>()
		const pool = new Pool<number>({
			create,
			destroy: (value) => destroyed.handler(value),
			validate: (value) => !invalid.has(value),
			max: 1,
		})

		// Lease the only slot, then park a second acquire as a FIFO waiter.
		const held = await pool.acquire()
		expect(held.value).toBe(0)
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		// Resource 0 went bad WHILE leased (e.g. a worker thread terminated on abort). The
		// release must NOT hand the dead resource to the waiter — it validates, destroys 0,
		// and serves a freshly-created resource instead.
		invalid.add(0)
		held.release()
		const served = await waiting
		expect(served.value).toBe(1) // a fresh resource, never the dead 0
		expect(destroyed.calls).toEqual([[0]]) // the invalid resource was torn down
		expect(created.count).toBe(2) // one extra create to replace the dead resource
		// The slot transferred from the dead resource to the fresh one — no leak, no overshoot.
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
	})

	it('rejects the waiter when the replacement create throws, and frees the slot', async () => {
		const destroyed = createRecorder<[number]>()
		let next = 0
		// The first create succeeds (resource 0); the replacement create (for the dead
		// resource's slot) throws — the waiter cannot be served and must reject.
		const pool = new Pool<number>({
			create: () => {
				if (next > 0) throw new Error('cannot replace')
				const value = next
				next += 1
				return value
			},
			destroy: (value) => destroyed.handler(value),
			validate: () => false, // every released resource is invalid on handoff
			max: 1,
		})

		const held = await pool.acquire()
		expect(held.value).toBe(0)
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		// Releasing 0 finds it invalid, destroys it, and tries to create a replacement —
		// which throws, so the waiter rejects with that error and the leased slot is freed.
		held.release()
		await expect(waiting).rejects.toThrow('cannot replace')
		expect(destroyed.calls).toEqual([[0]])
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
	})

	it('hands the SAME resource over a valid handoff (no behaviour change)', async () => {
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		// A validate hook that always passes — the released resource is still good, so the
		// waiter must receive the very same resource (the existing FIFO handoff, unchanged).
		const pool = new Pool<number>({
			create,
			destroy: (value) => destroyed.handler(value),
			validate: () => true,
			max: 1,
		})

		const held = await pool.acquire()
		expect(held.value).toBe(0)
		const waiting = pool.acquire()
		await waitForDelay(10)

		held.release()
		const served = await waiting
		expect(served.value).toBe(0) // same resource handed straight over
		expect(created.count).toBe(1) // no new resource created
		expect(destroyed.count).toBe(0) // nothing destroyed
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
	})
})

describe('Pool — clear', () => {
	it('destroys idle resources and leaves leased ones running', async () => {
		const { create } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({ create, destroy: (value) => destroyed.handler(value) })

		const a = await pool.acquire()
		const b = await pool.acquire()
		a.release() // 0 → idle
		expect(pool.idle).toBe(1)
		expect(pool.active).toBe(1)

		await pool.clear()
		// Only the idle resource (0) was destroyed; the leased one (1) is untouched.
		expect(destroyed.calls).toEqual([[0]])
		expect(pool.idle).toBe(0)
		expect(pool.active).toBe(1)
		expect(b.value).toBe(1)
	})
})

describe('Pool — destroy', () => {
	it('destroys idle resources and rejects parked waiters', async () => {
		const { create } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({ create, destroy: (value) => destroyed.handler(value), max: 1 })

		// One leased (kept, so the pool stays at max) + one genuinely parked waiter.
		await pool.acquire()
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		await pool.destroy()
		// The parked waiter is rejected by the teardown (no resource was ever handed).
		await expect(waiting).rejects.toThrow('destroyed')
		// A destroyed pool rejects further acquires.
		await expect(pool.acquire()).rejects.toThrow('destroyed')
	})

	it('destroys an idle resource left in the pool', async () => {
		const { create } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({ create, destroy: (value) => destroyed.handler(value) })
		const token = await pool.acquire()
		token.release()
		await pool.destroy()
		expect(destroyed.calls).toEqual([[0]])
	})
})

describe('Pool — double-release guard', () => {
	it('treats a second release of the same token as a no-op', async () => {
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({ create })
		const token = await pool.acquire()

		token.release()
		token.release() // no-op — must not double-return the resource
		expect(pool.idle).toBe(1)
		expect(pool.active).toBe(0)

		// Exactly one idle resource exists, so a single acquire drains it and the next creates.
		await pool.acquire()
		expect(pool.idle).toBe(0)
		const fresh = await pool.acquire()
		expect(fresh.value).toBe(1)
		expect(created.count).toBe(2)
	})
})

describe('Pool — destroy mid-create', () => {
	it('destroys the just-created resource and does not leak it when destroyed during create', async () => {
		// A gated async `create`, so we can destroy the pool while a creation is in flight.
		const created = createRecorder<[number]>()
		const destroyed = createRecorder<[number]>()
		const gate = createGate<number>()
		const pool = new Pool<number>({
			create: async () => {
				const value = await gate.promise
				created.handler(value)
				return value
			},
			destroy: (value) => destroyed.handler(value),
		})

		// Begin an acquire — it parks inside the awaited `create`.
		const acquiring = pool.acquire()
		await waitForDelay(10)
		expect(created.count).toBe(0)

		// Destroy the pool while create() is still pending, THEN let create() resolve.
		const destroying = pool.destroy()
		gate.resolve(7)
		// The acquire rejects (the resource must not be handed into a destroyed pool)…
		await expect(acquiring).rejects.toThrow('destroyed')
		await destroying
		await waitForDelay(10)

		// …and the resource that create() ultimately produced was destroyed, not leaked.
		expect(created.calls).toEqual([[7]])
		expect(destroyed.calls).toEqual([[7]])
		// Counts settle to zero — the reserved active slot was released on the destroy path.
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
	})
})

describe('Pool — clear with a parked waiter', () => {
	it('clear (idle-only) leaves a parked waiter, which a later release still serves', async () => {
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({ create, destroy: (value) => destroyed.handler(value), max: 1 })

		// Lease the only slot, then a second acquire PARKS (at max, nothing idle).
		const held = await pool.acquire()
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)
		expect(pool.idle).toBe(0)

		// clear() touches only IDLE resources — there are none, so it's a no-op here and must
		// NOT disturb the parked waiter or the leased resource.
		await pool.clear()
		expect(destroyed.count).toBe(0)
		expect(pool.active).toBe(1)

		// Releasing the leased resource hands it (FIFO) to the still-parked waiter.
		held.release()
		const served = await waiting
		expect(served.value).toBe(0)
		expect(created.count).toBe(1) // reused — no new resource created for the waiter
		expect(pool.active).toBe(1)
	})
})

describe('Pool — counts', () => {
	it('reports size / idle / active accurately through a lease cycle', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 3 })
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])

		const a = await pool.acquire()
		const b = await pool.acquire()
		expect([pool.size, pool.idle, pool.active]).toEqual([2, 0, 2])

		a.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([2, 1, 1])

		const c = await pool.acquire() // reuses the idle one
		expect([pool.size, pool.idle, pool.active]).toEqual([2, 0, 2])

		b.release()
		c.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([2, 2, 0])
	})
})

// ── High contention: waiters ≫ max, FIFO fairness, never-overshoot ───────────
//
// PRODUCTION GAP: the existing tests cap at max 1 with two waiters. A real pool
// under load has many more acquirers than slots; the invariants that must hold are
// (a) `active` NEVER exceeds `max` at any observable moment, (b) every parked waiter
// is eventually served exactly once (no leak, no double-serve), and (c) the handoff
// preserves FIFO order. These prove the wake-park + slot accounting survive heavy
// queue churn rather than just a single handoff.

describe('Pool — high contention (waiters ≫ max)', () => {
	it('serves 30 acquirers through 3 slots without ever exceeding max, FIFO order preserved', async () => {
		const { create, created } = createResourceFactory()
		const max = 3
		const total = 30
		const pool = new Pool<number>({ create, max })

		let liveLeases = 0
		let peak = 0
		const completionOrder: number[] = []
		// Each task acquires, briefly holds (yielding so others contend), records its
		// order, then releases — so all `total` tasks funnel through `max` slots.
		const run = async (index: number): Promise<void> => {
			const token = await pool.acquire()
			liveLeases += 1
			peak = Math.max(peak, liveLeases)
			// `active` is the pool's own view of leases out — it must never exceed max.
			expect(pool.active).toBeLessThanOrEqual(max)
			await waitForDelay(1)
			completionOrder.push(index)
			liveLeases -= 1
			token.release()
		}
		await Promise.all(Array.from({ length: total }, (_unused, index) => run(index)))

		// Never more than `max` leased at once — the cap held under heavy contention.
		expect(peak).toBe(max)
		// At most `max` resources were ever created (the rest reused through handoff).
		expect(created.count).toBeLessThanOrEqual(max)
		// All tasks completed, and the first `max` (which acquired immediately) plus the
		// FIFO-parked remainder completed in monotonically non-decreasing index order —
		// fairness held (no waiter was starved or served out of turn).
		expect(completionOrder).toHaveLength(total)
		expect([...completionOrder].sort((a, b) => a - b)).toEqual(completionOrder)
		// Fully drained: every lease returned, nothing leaked.
		expect([pool.size, pool.idle, pool.active]).toEqual([max, max, 0])
	})
})

// ── Handoff-validation under stress (the dead-resource-to-waiter bug class) ───
//
// PRODUCTION GAP: the last real bug here was the pool handing a resource that went
// invalid WHILE leased to a parked waiter. The existing test proves the single-waiter
// case; this proves it under stress — many releases to many waiters where resources
// go invalid INTERMITTENTLY (every other release). No waiter may ever receive an
// invalid resource, the lease count must stay balanced across every swap, and `active`
// must never overshoot `max` even as dead resources are destroyed + replaced.

describe('Pool — handoff validation under stress', () => {
	it('never serves a dead resource across many releases to many waiters; counts stay balanced', async () => {
		const max = 2
		const total = 24
		const created = createRecorder<[number]>()
		const destroyed = createRecorder<[number]>()
		const servedValues: number[] = []
		// A resource is a unique number. We taint resources so that on release roughly
		// half are invalid on handoff — forcing the destroy-and-replace path repeatedly.
		const invalid = new Set<number>()
		let next = 0
		const pool = new Pool<number>({
			create: () => {
				const value = next
				next += 1
				created.handler(value)
				return value
			},
			destroy: (value) => destroyed.handler(value),
			validate: (value) => !invalid.has(value),
			max,
		})

		let taintToggle = false
		const run = async (): Promise<void> => {
			const token = await pool.acquire()
			// No served resource may ever be one we already marked dead — the load-bearing
			// invariant: a dead resource is NEVER handed to a waiter, even under stress.
			expect(invalid.has(token.value)).toBe(false)
			servedValues.push(token.value)
			expect(pool.active).toBeLessThanOrEqual(max)
			await waitForDelay(1)
			// Taint every other resource right before releasing it, so the handoff path
			// must validate, destroy it, and create a replacement for the next waiter.
			taintToggle = !taintToggle
			if (taintToggle) invalid.add(token.value)
			token.release()
		}
		await Promise.all(Array.from({ length: total }, () => run()))
		await waitForDelay(5)

		// Every acquirer was served exactly once with a live (never-tainted-at-serve) value.
		expect(servedValues).toHaveLength(total)
		// At least one dead resource was torn down on a handoff (the destroy-and-replace
		// path was genuinely exercised, not vacuously skipped).
		expect(destroyed.count).toBeGreaterThan(0)
		// The pool never overshot max: idle + active ≤ max at the end, active back to 0.
		expect(pool.active).toBe(0)
		expect(pool.size).toBeLessThanOrEqual(max)
		expect(pool.idle).toBe(pool.size)

		// Pool validation is LAZY: a tainted resource that landed back on the idle list
		// (released with no waiter parked) is destroyed + replaced only on the NEXT acquire,
		// never served while dead. Drain any such straggler to prove the idle path also
		// refuses a dead resource.
		const finalDrain = await pool.acquire()
		expect(invalid.has(finalDrain.value)).toBe(false)
		finalDrain.release()
	})
})

// ── Hooks throwing under contention ──────────────────────────────────────────
//
// PRODUCTION GAP: `create` / `validate` / `destroy` are user code and may throw.
// The existing suite covers a throwing replacement-`create` on handoff and a throwing
// `create` mid-grow; it does NOT cover a `create` that throws while OTHER acquirers
// are parked behind it, nor a `validate` that throws (vs returning false), nor a
// `destroy` that throws under churn. Each must fail only the right acquirer / free the
// right slot and leave the pool usable for everyone else.

describe('Pool — create hook throwing under contention', () => {
	it('rejects only the acquirer whose create threw and frees the slot for the next', async () => {
		// The first create throws; subsequent creates succeed. With max 1 and three
		// queued acquires, the first must reject and the slot must free so the next
		// acquire can create successfully (no permanent slot leak from the failed grow).
		let attempt = 0
		const pool = new Pool<number>({
			create: () => {
				attempt += 1
				if (attempt === 1) throw new Error('create failed')
				return attempt
			},
			max: 1,
		})

		// All three race for the single slot; only the first triggers the throwing create.
		const first = pool.acquire()
		const second = pool.acquire()
		const third = pool.acquire()

		await expect(first).rejects.toThrow('create failed')
		// The failed grow released its reserved slot — `active` is back to 0, not stuck at 1.
		const secondToken = await second
		expect(secondToken.value).toBe(2) // the second create succeeded
		expect(pool.active).toBe(1)
		secondToken.release()
		const thirdToken = await third
		expect(thirdToken.value).toBe(2) // reused the released resource (no new create)
		thirdToken.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])
	})

	it('coerces a non-Error create throw to an Error on the grow path', async () => {
		const pool = new Pool<number>({
			create: () => {
				throw 'plain string failure'
			},
		})
		// A non-Error rejection is wrapped so callers always catch an Error (matches #grow).
		await expect(pool.acquire()).rejects.toThrow('plain string failure')
		await expect(pool.acquire()).rejects.toBeInstanceOf(Error)
		// The slot was freed both times — the pool is not wedged at active 1.
		expect(pool.active).toBe(0)
	})
})

describe('Pool — validate hook throwing (treated as invalid, FIX)', () => {
	it('a throwing validate on the idle path destroys the resource and creates a fresh one', async () => {
		// FIX: a `validate` that THROWS is treated exactly like one returning false — the
		// idle resource is "not usable", so it is destroyed and the acquire creates a fresh
		// one instead of propagating the throw (or leaving a phantom slot / lost resource).
		let healthy = true
		const destroyed = createRecorder<[number]>()
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({
			create,
			destroy: (value) => destroyed.handler(value),
			validate: () => {
				if (!healthy) throw new Error('validate exploded')
				return true
			},
		})

		const token = await pool.acquire()
		expect(token.value).toBe(0)
		token.release()
		expect(pool.idle).toBe(1)

		// The idle resource now makes validate throw — acquire destroys 0 and creates 1.
		healthy = false
		const replacement = await pool.acquire()
		expect(replacement.value).toBe(1)
		expect(destroyed.calls).toEqual([[0]]) // the resource whose validate threw was torn down
		expect(created.count).toBe(2)
		// No phantom lease: exactly the one fresh resource is leased.
		expect(pool.active).toBe(1)
	})

	it('a throwing validate on the handoff path destroys + replaces, serving the waiter a fresh resource (no hang)', async () => {
		// FIX (the dead-resource-to-waiter bug class): on handoff `#handoff` awaits
		// `validate(resource)`. Before the fix, a THROW (vs returning false) escaped as an
		// unhandled rejection and the parked waiter hung forever. Now a throw is treated as
		// invalid — the resource is destroyed and the waiter is served a fresh replacement.
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({
			create,
			destroy: (value) => destroyed.handler(value),
			validate: () => {
				throw new Error('validate threw on handoff')
			},
			max: 1,
		})

		// Acquire the only slot WITHOUT triggering validate (idle is empty on first grow).
		const held = await pool.acquire()
		expect(held.value).toBe(0)
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		// Releasing routes through the handoff, whose validate throws — the dead resource is
		// destroyed and the waiter is served a freshly-created one (NOT rejected, NOT hung).
		held.release()
		const served = await waiting
		expect(served.value).toBe(1) // a fresh replacement, never the resource whose validate threw
		expect(destroyed.calls).toEqual([[0]])
		expect(created.count).toBe(2)
		// The slot transferred cleanly from the dead resource to its replacement — no overshoot.
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
	})
})

describe('Pool — destroy hook throwing under churn', () => {
	it('swallows a throwing destroy and keeps the pool consistent through clear', async () => {
		// `#release` swallows destroy errors by design (a failed destroy abandons the
		// resource — nothing to recover). Under repeated acquire/release/clear this must
		// not corrupt counts or wedge the pool, even though destroy always throws.
		const { create } = createResourceFactory()
		const pool = new Pool<number>({
			create,
			destroy: () => {
				throw new Error('destroy failed')
			},
		})

		const a = await pool.acquire()
		const b = await pool.acquire()
		a.release()
		b.release()
		expect(pool.idle).toBe(2)

		// clear() destroys both idle resources; each destroy throws but is swallowed.
		await expect(pool.clear()).resolves.toBeUndefined()
		// The idle resources were dropped regardless of the destroy failure.
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
		// The pool still works after the failed destroys.
		const fresh = await pool.acquire()
		expect(typeof fresh.value).toBe('number')
		fresh.release()
	})
})

// ── Rapid acquire/release churn — counts always consistent, no leak ──────────
//
// PRODUCTION GAP: a single resource hammered by serialized acquire→release cycles
// must keep `size` / `idle` / `active` consistent every cycle and never leak. This is
// the steady-state hot path of a connection pool.

describe('Pool — rapid acquire/release churn', () => {
	it('keeps counts consistent across 100 serialized acquire/release cycles (one resource reused)', async () => {
		const { create, created } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 1 })

		for (let cycle = 0; cycle < 100; cycle += 1) {
			const token = await pool.acquire()
			expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
			token.release()
			expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])
		}
		// The single resource was reused every cycle — exactly one create across 100 cycles.
		expect(created.count).toBe(1)
	})

	it('keeps counts consistent across interleaved multi-resource churn', async () => {
		const { create } = createResourceFactory()
		const max = 4
		const pool = new Pool<number>({ create, max })
		const held: Array<{ release(): void }> = []

		// Grab all four, release two, grab two more, release everything — counts must
		// reconcile to a fully-idle pool of exactly `max` resources at the end.
		for (let index = 0; index < max; index += 1) held.push(await pool.acquire())
		expect(pool.active).toBe(max)
		held.splice(0, 2).forEach((token) => token.release())
		expect([pool.idle, pool.active]).toEqual([2, 2])
		held.push(await pool.acquire(), await pool.acquire())
		expect(pool.active).toBe(max)
		held.forEach((token) => token.release())
		expect([pool.size, pool.idle, pool.active]).toEqual([max, max, 0])
	})
})

// ── clear / destroy with active leases + parked waiters + in-flight create ────
//
// PRODUCTION GAP: teardown is the most dangerous moment. The existing suite covers
// destroy-mid-create and destroy-with-one-waiter separately. Here destroy fires while
// (a) a lease is active, (b) waiters are parked, and (c) a create is in flight — all
// at once — and we assert every waiter rejects, the in-flight resource is destroyed
// not leaked, and the pool is left fully torn down.

describe('Pool — destroy with active leases + parked waiters + in-flight create', () => {
	it('rejects every parked waiter, destroys the in-flight resource, and tears fully down', async () => {
		const created = createRecorder<[number]>()
		const destroyed = createRecorder<[number]>()
		const gate = createGate<number>()
		// max 1: the first acquire grows (parking inside the gated create); the next two
		// acquires queue as waiters behind it.
		const pool = new Pool<number>({
			create: async () => {
				const value = await gate.promise
				created.handler(value)
				return value
			},
			destroy: (value) => destroyed.handler(value),
			max: 1,
		})

		const growing = pool.acquire() // parks inside the awaited create
		const waiterA = pool.acquire() // queues (pool reports size 1 = at max during grow)
		const waiterB = pool.acquire()
		await waitForDelay(10)

		// Destroy while the create is still pending and two waiters are parked.
		const destroying = pool.destroy()
		gate.resolve(42) // the in-flight create now resolves into a destroyed pool

		// The growing acquire rejects (its resource must not be handed into a dead pool)…
		await expect(growing).rejects.toThrow('destroyed')
		// …and both parked waiters are rejected by the teardown.
		await expect(waiterA).rejects.toThrow('destroyed')
		await expect(waiterB).rejects.toThrow('destroyed')
		await destroying
		await waitForDelay(5)

		// The resource the create ultimately produced was destroyed, not leaked.
		expect(created.calls).toEqual([[42]])
		expect(destroyed.calls).toEqual([[42]])
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
		// A destroyed pool rejects all further acquires.
		await expect(pool.acquire()).rejects.toThrow('destroyed')
	})

	it('clear with an active lease + parked waiter leaves both intact (idle-only teardown)', async () => {
		// clear() touches only idle resources. With one resource leased and a waiter
		// parked behind it (nothing idle), clear must be a no-op on both, and a later
		// release must still serve the parked waiter — clear must not strand it.
		const { create, created } = createResourceFactory()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool<number>({ create, destroy: (value) => destroyed.handler(value), max: 1 })

		const held = await pool.acquire()
		const waiting = pool.acquire()
		await waitForDelay(10)

		await pool.clear()
		expect(destroyed.count).toBe(0) // nothing idle to destroy
		expect(pool.active).toBe(1) // the lease is untouched

		held.release()
		const served = await waiting
		expect(served.value).toBe(0) // the parked waiter was still served the released resource
		expect(created.count).toBe(1)
		served.release()
		expect([pool.idle, pool.active]).toEqual([1, 0])
	})
})

// ── Emitter — the PUSH observation surface (AGENTS §13) ──────────────────────
//
// The Pool exposes a typed `emitter` (`PoolEventMap`) carrying its resource lifecycle —
// `create` / `acquire` / `release` / `destroy` — for fire-and-forget observers. Every event
// is emitted directly; the emitter isolates a listener throw (it never escapes into the
// validated FIFO handoff-eviction path, AGENTS §13, routing it to the emitter's own `error`
// handler — the `error` option), each placed AFTER its create / acquire / release / destroy
// transition. These pin: each event
// fires at the right moment; `on?` wires initial listeners; and the LOAD-BEARING emit-safety
// guarantee — a throwing observer cannot break the handoff/eviction machinery (no waiter
// stranded, lease counts balanced), yet the `error` handler fires.

// The PoolEventMap event names recorded across the emitter tests — fed to the shared
// `recordEmitterEvents` (AGENTS §16.1: the per-event wiring is centralized; this file
// keeps only the names its scenarios observe).
const POOL_EVENTS = ['create', 'acquire', 'release', 'destroy'] as const

describe('Pool — emitter (push observation surface)', () => {
	it('fires create + acquire on a fresh lease, then release on return', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create })
		const events = recordEmitterEvents(pool.emitter, POOL_EVENTS)

		const token = await pool.acquire()
		// A fresh resource was created (none idle) and then leased.
		expect(events.create.count).toBe(1)
		expect(events.acquire.count).toBe(1)
		expect(events.release.count).toBe(0)

		token.release()
		// Returning it to idle (no waiter parked) fires `release`, no `destroy` (it lives on).
		expect(events.release.count).toBe(1)
		expect(events.destroy.count).toBe(0)
	})

	it('a reused idle resource fires acquire WITHOUT a second create', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create })
		const events = recordEmitterEvents(pool.emitter, POOL_EVENTS)

		const first = await pool.acquire()
		first.release()
		expect(events.create.count).toBe(1)
		expect(events.acquire.count).toBe(1)

		// The next acquire reuses the idle one — `acquire` fires again, `create` does not.
		const again = await pool.acquire()
		expect(again.value).toBe(0)
		expect(events.create.count).toBe(1) // still one — reused, not recreated
		expect(events.acquire.count).toBe(2)
	})

	it('fires destroy when clear tears down an idle resource', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create, destroy: () => {} })
		const events = recordEmitterEvents(pool.emitter, POOL_EVENTS)
		const token = await pool.acquire()
		token.release()
		await pool.clear()
		// The idle resource was destroyed by `clear`.
		expect(events.destroy.count).toBe(1)
	})

	it('a FIFO handoff fires acquire for the served waiter (the resource stays leased, no release)', async () => {
		const { create } = createResourceFactory()
		const pool = new Pool<number>({ create, max: 1 })
		const events = recordEmitterEvents(pool.emitter, POOL_EVENTS)

		const held = await pool.acquire() // create + acquire (1)
		const waiting = pool.acquire() // parks
		await waitForDelay(10)
		expect(events.acquire.count).toBe(1) // only the first lease so far

		// Releasing hands the SAME resource straight to the waiter — it stays leased, so the
		// served waiter fires `acquire` (2) and NO `release` (it never returned to idle).
		held.release()
		const served = await waiting
		expect(served.value).toBe(0)
		expect(events.acquire.count).toBe(2)
		expect(events.release.count).toBe(0)
		expect(events.create.count).toBe(1) // reused — no second create
	})

	it('an invalid-on-handoff resource fires destroy + create + acquire (eviction observed)', async () => {
		const { create } = createResourceFactory()
		const invalid = new Set<number>()
		const pool = new Pool<number>({
			create,
			destroy: () => {},
			validate: (value) => !invalid.has(value),
			max: 1,
		})
		const events = recordEmitterEvents(pool.emitter, POOL_EVENTS)

		const held = await pool.acquire() // create(0) + acquire
		const waiting = pool.acquire()
		await waitForDelay(10)

		// Resource 0 went bad while leased — the handoff destroys it, creates a replacement,
		// and serves the waiter the fresh one.
		invalid.add(0)
		held.release()
		const served = await waiting
		expect(served.value).toBe(1)
		expect(events.destroy.count).toBe(1) // the dead resource was torn down
		expect(events.create.count).toBe(2) // one initial + one replacement
		expect(events.acquire.count).toBe(2) // the initial lease + the served waiter
	})

	it('wires initial listeners from the `on` option at construction', async () => {
		const create = createRecorder<[]>()
		const acquire = createRecorder<[]>()
		const { create: make } = createResourceFactory()
		const pool = new Pool<number>({
			create: make,
			on: { create: create.handler, acquire: acquire.handler },
		})
		const token = await pool.acquire()
		token.release()
		expect(create.count).toBe(1)
		expect(acquire.count).toBe(1)
	})

	it('EMIT SAFETY: a throwing acquire listener cannot break the handoff eviction, and routes to the error handler', async () => {
		const max = 2
		const total = 24
		const { create } = createResourceFactory()
		const invalid = new Set<number>()
		const servedValues: number[] = []
		const errors = createErrorRecorder()
		const pool = new Pool<number>({
			create,
			destroy: () => {},
			validate: (value) => !invalid.has(value),
			max,
			error: errors.handler,
		})
		// A buggy `acquire` observer that throws on EVERY lease — including the audited handoff
		// path where a served waiter fires `acquire`. A throw escaping here could strand the
		// waiter; the emitter must isolate it so the handoff completes and counts stay balanced.
		pool.emitter.on('acquire', () => {
			throw new Error('acquire observer blew up')
		})

		let taintToggle = false
		const run = async (): Promise<void> => {
			const token = await pool.acquire()
			expect(invalid.has(token.value)).toBe(false) // never served a dead resource
			servedValues.push(token.value)
			expect(pool.active).toBeLessThanOrEqual(max)
			await waitForDelay(1)
			taintToggle = !taintToggle
			if (taintToggle) invalid.add(token.value) // force the destroy-and-replace handoff
			token.release()
		}
		await Promise.all(Array.from({ length: total }, () => run()))
		await waitForDelay(5)

		// THE LOAD-BEARING ASSERTION: every acquirer was served exactly once despite the
		// throwing observer — no waiter was stranded by an escaped throw on the handoff path.
		expect(servedValues).toHaveLength(total)
		// Lease counts stayed balanced through every eviction/handoff (no overshoot, no leak).
		expect(pool.active).toBe(0)
		expect(pool.size).toBeLessThanOrEqual(max)
		// EVERY throw routed to the emitter's error handler for the `acquire` event — (error, event).
		expect(errors.count).toBeGreaterThan(0)
		expect(errors.calls.every(([, event]) => event === 'acquire')).toBe(true)
		// The pool still leases after the storm of throwing observers.
		const after = await pool.acquire()
		expect(typeof after.value).toBe('number')
		after.release()
	})

	it('EMIT SAFETY: a throwing error handler neither escapes nor recurses', async () => {
		const { create } = createResourceFactory()
		const errors = createErrorRecorder()
		const pool = new Pool<number>({
			create,
			error: (error, event) => {
				errors.handler(error, event)
				throw new Error('error handler blew up too')
			},
		})
		pool.emitter.on('acquire', () => {
			throw new Error('acquire listener blew up')
		})
		// The lease STILL completes cleanly — neither throw escaped into the pool.
		const token = await pool.acquire()
		expect(typeof token.value).toBe('number')
		token.release()
		expect(pool.active).toBe(0)
		// The error handler fired exactly once (its own throw was swallowed).
		expect(errors.count).toBe(1)
		expect(errors.calls[0]?.[1]).toBe('acquire')
	})
})
