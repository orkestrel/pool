import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'

/**
 * The push observation surface of a {@link PoolInterface} (AGENTS §13) — the resource
 * lifecycle moments a fire-and-forget observer subscribes to.
 *
 * @remarks
 * Pure signals (no `T` payload — `Pool<T>` carries no resource value on its events, so a
 * non-generic map stays lean). Listener isolation is the emitter's (AGENTS §13): every event
 * is emitted directly and a listener throw is routed to the emitter's `error` handler (the
 * `error` option), never onto this map, and sits AFTER the relevant create / acquire /
 * release / destroy transition — so a throwing observer can never corrupt the FIFO
 * handoff-eviction machinery (it cannot strand a parked waiter or unbalance the lease count).
 * Subscribe via `pool.emitter.on(...)`. Declared as a `type` alias (§4.5 — `EventMap` is a
 * `type` kind).
 */
export type PoolEventMap = {
	/** A fresh resource was created (`create` resolved) and leased. */
	readonly create: readonly []
	/** A token was handed to a lessee (a reused idle one, a fresh one, or a served waiter). */
	readonly acquire: readonly []
	/** A leased resource returned to idle (no waiter was parked). */
	readonly release: readonly []
	/** A resource was destroyed (`clear` / `destroy`, or a failed `validate`). */
	readonly destroy: readonly []
}

/**
 * A leased resource from a {@link PoolInterface} — `value` is the live resource and
 * `release()` returns it to the pool for reuse (or hands it to the next waiter).
 */
export interface PoolToken<T> {
	/** The leased resource. */
	readonly value: T
	/** Return the resource to the pool; calling more than once is a no-op. */
	release(): void
}

/**
 * A parked acquirer on a {@link PoolInterface}'s FIFO waiter list — its promise resolvers
 * plus the cleanup that detaches its abort listener.
 *
 * @remarks
 * Held only inside the {@link PoolInterface} engine (a resource at `max` parks the acquirer
 * here until a `release` hands it a token); not part of the public call surface, but
 * centralized here per AGENTS §5. `resolve` hands the waiter its leased {@link PoolToken};
 * `reject` fails its `acquire` (a teardown or an aborted wait); `clear` detaches the abort
 * listener so a settled waiter leaks nothing.
 *
 * @typeParam T - The resource the pool leases
 */
export interface PoolWaiter<T> {
	readonly resolve: (token: PoolToken<T>) => void
	readonly reject: (error: unknown) => void
	clear(): void
}

/**
 * Options for `createPool` — the resource lifecycle hooks.
 *
 * @remarks
 * - `create` — make a fresh resource; called when no idle resource is reusable and
 *   the pool is below `max`. May be async.
 * - `destroy` — tear a resource down when the pool drops it (`clear` / `destroy`, or
 *   a failed `validate`); optional and awaited.
 * - `validate` — check an idle resource is still usable before leasing it; an invalid
 *   resource is destroyed and replaced. Optional (an absent validator trusts idle).
 * - `max` — the most resources that may exist at once (idle + leased); defaults to
 *   unbounded. A surplus `acquire` waits (FIFO) for a `release`.
 * - `on` — the reserved {@link EmitterHooks} key (§8): initial listeners for the pool's
 *   {@link PoolEventMap}, wired at construction (e.g. `{ create: () => count() }`).
 */
export interface PoolOptions<T> {
	readonly on?: EmitterHooks<PoolEventMap>
	/** The emitter's listener-error handler (AGENTS §13) — a listener throw routes here, not to a domain event. */
	readonly error?: EmitterErrorHandler
	readonly create: () => Promise<T> | T
	readonly destroy?: (value: T) => Promise<void> | void
	readonly validate?: (value: T) => Promise<boolean> | boolean
	readonly max?: number
}

/**
 * A bounded resource pool with idle reuse + FIFO waiting.
 *
 * @remarks
 * Exposes a typed {@link emitter} (AGENTS §13) carrying its resource lifecycle moments
 * ({@link PoolEventMap}) for fire-and-forget observers. Emitting is observation-only —
 * every event fires AFTER the relevant create / acquire / release / destroy transition, so a
 * buggy observer can never corrupt the FIFO handoff-eviction machinery: the emitter isolates
 * a listener throw and routes it to its `error` handler (the `error` option), never the pool.
 */
export interface PoolInterface<T> {
	readonly emitter: EmitterInterface<PoolEventMap>
	readonly size: number
	readonly idle: number
	readonly active: number
	acquire(signal?: AbortSignal): Promise<PoolToken<T>>
	clear(): Promise<void>
	destroy(): Promise<void>
}
