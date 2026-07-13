import type { EmitterInterface } from '@orkestrel/emitter'
import type { PoolEventMap, PoolInterface, PoolOptions, PoolToken, PoolWaiter } from './types.js'
import { Emitter } from '@orkestrel/emitter'

/**
 * A bounded resource pool with idle reuse + FIFO waiting.
 *
 * @remarks
 * - **Idle reuse.** `acquire` first takes an idle resource (validating it when a
 *   `validate` hook is set — an invalid one is destroyed and the next idle / a fresh
 *   one is tried). When no usable idle resource exists and the pool is below `max`, it
 *   `create`s a new one. At `max` with none idle, the acquire PARKS on a FIFO waiter
 *   list until a `release` hands it a resource.
 * - **FIFO handoff (validated).** `release` (on the token) hands the resource to the next
 *   parked waiter (oldest first) — the resource stays leased, the lessee just changes —
 *   or returns it to idle when no one is waiting. With a waiter parked the resource is
 *   re-validated first (the same `validate` hook the idle path uses), so a resource that
 *   went invalid WHILE leased (e.g. a terminated worker thread) is destroyed and the
 *   waiter is served a fresh/valid one instead — a dead resource is never handed on. The
 *   no-waiter path stays synchronous; releasing the same token twice is a no-op (an
 *   idempotent token guard).
 * - **`validate` is total.** A `validate` hook that THROWS is treated exactly like one
 *   returning `false` — the resource is "not usable", so it is destroyed and replaced —
 *   rather than escaping. This holds on both the idle reuse and the FIFO handoff paths, so
 *   a throwing validator can never strand a parked waiter on an unhandled rejection.
 * - **Abort-cancellable waiting.** A parked `acquire` given an `AbortSignal` rejects
 *   when that signal fires and removes its waiter from the queue — no leaked waiter, so
 *   a later `release` still serves the next live waiter. The signal is supplied by the
 *   caller (a worker, for example, passes its per-attempt execution signal); the pool adds no abort
 *   of its own.
 * - **Counts.** `size` = idle + leased; `idle` = available now; `active` = leased out.
 * - **Teardown.** `clear` destroys every IDLE resource (leased ones keep running);
 *   `destroy` destroys ALL resources and rejects any parked waiters. Both await the
 *   `destroy` hook.
 * - **Observable (§13).** The owned {@link emitter} ({@link PoolEventMap}) carries the
 *   resource lifecycle — `create` / `acquire` / `release` / `destroy` — for fire-and-forget
 *   observers. Every event is emitted directly, strictly AFTER the relevant transition —
 *   OUTSIDE the `#handoff` / `#serve` await-chain, never across a waiter's resolve; the
 *   emitter isolates a listener throw and routes it to its `error` handler (the `error`
 *   option), so a buggy observer can NEVER corrupt the validated FIFO handoff-eviction
 *   machinery (it cannot strand a parked waiter or unbalance the lease count). Observation is
 *   purely a side-channel.
 * - **De-bloated.** No warm-floor / `min`, no eviction timers — lean.
 */
export class Pool<T> implements PoolInterface<T> {
	readonly #create: () => Promise<T> | T
	readonly #destroy: ((value: T) => Promise<void> | void) | undefined
	readonly #validate: ((value: T) => Promise<boolean> | boolean) | undefined
	readonly #max: number
	// The PUSH observation surface (§13) — owned, never inherited. The emitter isolates a
	// listener throw (routing it to the `error` handler), so it can never escape into the
	// validated FIFO handoff-eviction path.
	readonly #emitter: Emitter<PoolEventMap>

	readonly #idle: T[] = []
	readonly #waiters: PoolWaiter<T>[] = []
	#active = 0
	#destroyed = false

	constructor(options: PoolOptions<T>) {
		this.#create = options.create
		this.#destroy = options.destroy
		this.#validate = options.validate
		this.#max = Math.max(1, options.max ?? Number.POSITIVE_INFINITY)
		this.#emitter = new Emitter<PoolEventMap>({ on: options?.on, error: options?.error })
	}

	get emitter(): EmitterInterface<PoolEventMap> {
		return this.#emitter
	}

	get size(): number {
		return this.#idle.length + this.#active
	}

	get idle(): number {
		return this.#idle.length
	}

	get active(): number {
		return this.#active
	}

	async acquire(signal?: AbortSignal): Promise<PoolToken<T>> {
		if (this.#destroyed) throw new Error('pool is destroyed')
		if (signal?.aborted === true) throw signal.reason
		// Reuse a validated idle resource if one is available.
		const reused = await this.#reuse()
		if (reused !== undefined) {
			// Observe the lease — AFTER `#reuse` resolved the token (the resource is already
			// leased; emit only OBSERVES it).
			this.#emitter.emit('acquire')
			return reused
		}
		// Otherwise grow the pool when below the cap.
		if (this.size < this.#max) {
			const grown = await this.#grow()
			// Observe the lease — AFTER `#grow` resolved the fresh token.
			this.#emitter.emit('acquire')
			return grown
		}
		// At capacity with nothing idle — park until a release hands us a resource. The
		// `acquire` for THIS lease is emitted by `#serve` once a `release` hands it a resource
		// (after `waiter.resolve`), NOT here — so a served waiter emits `acquire` exactly once.
		return await this.#wait(signal)
	}

	async clear(): Promise<void> {
		const resources = this.#idle.splice(0)
		await Promise.all(resources.map((resource) => this.#release(resource)))
	}

	async destroy(): Promise<void> {
		if (this.#destroyed) return
		this.#destroyed = true
		const waiters = this.#waiters.splice(0)
		const error = new Error('pool is destroyed')
		for (const waiter of waiters) {
			waiter.clear()
			waiter.reject(error)
		}
		const resources = this.#idle.splice(0)
		await Promise.all(resources.map((resource) => this.#release(resource)))
	}

	// Take + validate the next idle resource; destroy + skip invalid ones. Returns a
	// token for the first valid idle resource, or `undefined` when none is usable.
	async #reuse(): Promise<PoolToken<T> | undefined> {
		while (this.#idle.length > 0) {
			const resource = this.#idle.shift()
			if (resource === undefined) continue
			if (await this.#valid(resource)) {
				this.#active += 1
				return this.#token(resource)
			}
			await this.#release(resource)
		}
		return undefined
	}

	// Create a fresh resource and lease it. Reserve the active slot before awaiting
	// `create` so a concurrent acquire sees the pool at capacity (no overshoot of `max`).
	async #grow(): Promise<PoolToken<T>> {
		this.#active += 1
		let resource: T
		try {
			resource = await this.#create()
		} catch (error: unknown) {
			this.#active -= 1
			throw error instanceof Error ? error : new Error(String(error))
		}
		// The pool may have been destroyed during `create` — never hand a live resource into
		// a torn-down pool. Free the active slot, destroy the just-created resource, and throw.
		if (this.#destroyed) {
			this.#active -= 1
			await this.#release(resource)
			throw new Error('pool is destroyed')
		}
		// Observe the fresh resource — AFTER `create` resolved and the not-destroyed re-check
		// passed (the resource is here to stay), BEFORE the token is built.
		this.#emitter.emit('create')
		return this.#token(resource)
	}

	// Park a resolver on the FIFO waiter list until a release hands it a resource; an
	// abort on `signal` rejects the acquire and removes its waiter (no leak).
	#wait(signal: AbortSignal | undefined): Promise<PoolToken<T>> {
		return new Promise<PoolToken<T>>((resolve, reject) => {
			const waiter: PoolWaiter<T> = { resolve, reject, clear: () => {} }
			this.#waiters.push(waiter)
			if (signal !== undefined) {
				const onAbort = (): void => {
					const index = this.#waiters.indexOf(waiter)
					if (index >= 0) this.#waiters.splice(index, 1)
					reject(signal.reason)
				}
				signal.addEventListener('abort', onAbort, { once: true })
				waiter.clear = (): void => signal.removeEventListener('abort', onAbort)
			}
		})
	}

	// Build an idempotent token; its `release` returns the resource exactly once.
	#token(resource: T): PoolToken<T> {
		let released = false
		return {
			value: resource,
			release: (): void => {
				if (released) return
				released = true
				this.#return(resource)
			},
		}
	}

	// Return a leased resource. No waiter parked → synchronously drop it to idle (or destroy
	// it if torn down), `#active -= 1`. A waiter IS parked → hand off asynchronously, after
	// re-validating the resource so an invalid one (e.g. a terminated thread) is never served.
	#return(resource: T): void {
		if (this.#waiters.length === 0) {
			this.#active -= 1
			if (this.#destroyed) {
				void this.#release(resource)
				return
			}
			this.#idle.push(resource)
			// Observe the resource dropping to idle — AFTER `#active` was balanced down and it
			// is back on the idle list (the synchronous no-waiter path). A waiter-handoff
			// instead keeps the resource LEASED, so it emits no `release` here.
			this.#emitter.emit('release')
			return
		}
		void this.#handoff(resource)
	}

	// Hand a released resource to the oldest parked waiter, re-validating it first. A VALID
	// resource is served as-is (it stays leased — `#active` unchanged). An INVALID one is
	// destroyed and the waiter is served a fresh resource that REUSES the dead one's `#active`
	// slot (the count never dips below the lease, so a concurrent acquire can't overshoot
	// `max`). The waiter is re-shifted after every `await` (the queue can change underneath
	// us), and the pool is re-checked for teardown — so no waiter is leaked or double-settled
	// and no resource is handed into a destroyed pool.
	async #handoff(resource: T): Promise<void> {
		// VALID release → serve the released resource directly (the common, unchanged path).
		if (await this.#valid(resource)) {
			this.#serve(resource)
			return
		}
		// INVALID release → destroy it and replace it, holding its `#active` slot steady.
		void this.#release(resource)
		let replacement: T
		try {
			replacement = await this.#create()
		} catch (error: unknown) {
			// The dead resource cannot be replaced — free its slot and reject the oldest waiter
			// (it can't be served), or drop the error if the queue raced empty.
			this.#active -= 1
			const waiter = this.#waiters.shift()
			waiter?.clear()
			waiter?.reject(error instanceof Error ? error : new Error(String(error)))
			return
		}
		// Observe the fresh replacement — AFTER `create` resolved, BEFORE it is served.
		this.#emitter.emit('create')
		this.#serve(replacement)
	}

	// Hand a validated resource (the released one, or its fresh replacement) to the oldest
	// live waiter, holding its `#active` slot. If the queue raced empty across the awaits, the
	// resource is idle (or destroyed when torn down) and its slot is freed.
	#serve(resource: T): void {
		const waiter = this.#destroyed ? undefined : this.#waiters.shift()
		if (waiter !== undefined) {
			waiter.clear()
			waiter.resolve(this.#token(resource))
			// Observe the served-waiter lease — strictly AFTER `waiter.resolve(...)` (the token
			// is already handed off; the emit only OBSERVES it and cannot sit across the resolve
			// or perturb the served acquirer's continuation). This is the `acquire` for a parked
			// `acquire`, so the public `acquire` deliberately does NOT emit on its `#wait` branch.
			this.#emitter.emit('acquire')
			return
		}
		this.#active -= 1
		if (this.#destroyed) {
			void this.#release(resource)
			return
		}
		this.#idle.push(resource)
		// The waiter queue raced empty across the handoff awaits — the resource lands back on
		// idle instead of being leased. Observe that as a `release` (the return-to-idle that
		// `#return` would have emitted had no waiter been parked when the token was released).
		this.#emitter.emit('release')
	}

	// Is this resource usable? No validator trusts it; otherwise run the hook. A validator
	// that THROWS is treated exactly like one returning `false` — the resource is "not
	// usable", so it is destroyed + replaced rather than escaping as an unhandled rejection
	// that would strand a parked waiter on the handoff path (the same total-function spirit
	// as a guard, AGENTS §12 / §14). The single gate for both the idle (`#reuse`) and the
	// handoff (`#handoff`) validation, so the two paths can never diverge.
	async #valid(resource: T): Promise<boolean> {
		if (this.#validate === undefined) return true
		try {
			return await this.#validate(resource)
		} catch {
			return false
		}
	}

	// Destroy one resource via the optional hook, swallowing destruction failures. Observe the
	// drop with a `destroy` event AFTER the hook ran (or after the no-hook drop) — the resource
	// is gone from the pool either way; a swallowed `destroy`-hook failure still emits (the pool
	// abandoned it). The emit is the LAST thing here, strictly after the teardown transition.
	async #release(resource: T): Promise<void> {
		if (this.#destroy === undefined) {
			this.#emitter.emit('destroy')
			return
		}
		try {
			await this.#destroy(resource)
		} catch {
			// A destroy failure abandons the resource — there is nothing to recover.
		}
		this.#emitter.emit('destroy')
	}
}
