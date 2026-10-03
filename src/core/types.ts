import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'

/**
 * Names the machine-readable failure codes produced by {@link PoolError}.
 */
export type PoolCode = 'invalid' | 'destroyed' | 'create' | 'cleanup'

/**
 * Represents the structured context attached to a {@link PoolError}: the rejected input, or the
 * distinct destroy-hook failures an aggregate cleanup collected.
 */
export interface PoolContext {
	/** Holds the rejected public input, when the failure is an input-validation error. */
	readonly value?: unknown
	/** Holds distinct destroy-hook failures collected by `clear()` or `destroy()`. */
	readonly failures?: readonly unknown[]
}

/**
 * Represents the construction options for {@link PoolError}: the stable code, an optional cause,
 * and optional structured context.
 */
export interface PoolErrorOptions {
	/** Holds the stable machine-readable failure category. */
	readonly code: PoolCode
	/** Holds the original thrown value, retained without unsafe string coercion. */
	readonly cause?: unknown
	/** Holds optional structured failure details. */
	readonly context?: PoolContext
}

/**
 * Represents the observable resource lifecycle events emitted by a {@link PoolInterface}.
 */
export type PoolEventMap = {
	/** Signals that a created resource entered pool ownership. */
	readonly create: readonly []
	/** Signals that a token settled successfully and its exact resource became leased. */
	readonly acquire: readonly []
	/** Signals that a released, orphaned, or refilled resource became immediately idle. */
	readonly release: readonly []
	/** Signals that a resource destroy hook completed or was attempted when absent. */
	readonly destroy: readonly []
}

/**
 * Represents a unique lease over one pool-owned resource record, exposing that record as a readonly
 * `value` and ending it through an idempotent `release` or `destroy`.
 */
export interface PoolToken<T> {
	/** Holds the leased value. Duplicate values still belong to independent records. */
	readonly value: T
	/**
	 * Gives this exact record back to the pool once; a repeat call, and a call after loss or teardown
	 * took ownership, are no-ops.
	 */
	release(): void
	/**
	 * Destroys this exact record instead of returning it; a repeat call, and a call after release,
	 * are no-ops.
	 *
	 * @returns A promise for this record's cleanup attempt, including an attempt already in progress
	 * @throws {@link PoolError} Thrown as a rejection with `code: 'cleanup'` when disposal fails.
	 */
	destroy(): Promise<void>
}

/**
 * Represents the resource lifecycle options for {@link Pool} and `createPool`: creation,
 * destruction, validation, capacity, a bounded warm floor, and loss observation.
 *
 * @remarks
 * `create` produces resources on demand, or only to restore `min` after `start()`.
 * `destroy` tears down a claimed resource. `validate` checks an owned resource before reuse.
 * `min` and `max` are positive safe integers and must be equal when both are set; `max`
 * defaults to `min`. Omitting both leaves capacity unbounded. `restarts` is required with
 * `min`, is refused without `min`, has no default, and is a non-negative safe integer.
 * A failed refill or loss of a never-leased record adds a strike; a lease grant resets the
 * strikes. Exceeding `restarts` stops refills until `start()`, except for one attempt owed
 * to each lost leased record.
 * `watch` settles on loss and receives a signal aborted when disposal begins. Its rejection
 * reaches `error` with event `watch` only while the record is live. Any settlement after
 * the signal aborts is ignored. `on` installs initial emitter listeners; `error`
 * also receives isolated listener failures.
 */
export interface PoolOptions<T> {
	readonly on?: EmitterHooks<PoolEventMap>
	readonly error?: EmitterErrorHandler
	readonly create: () => Promise<T> | T
	readonly destroy?: (value: T) => Promise<void> | void
	readonly validate?: (value: T) => Promise<boolean> | boolean
	readonly watch?: (value: T, signal: AbortSignal) => Promise<unknown>
	readonly max?: number
	readonly min?: number
	readonly restarts?: number
}

/**
 * Represents a FIFO resource pool with optional bounded capacity, a warm floor, loss recovery,
 * and deterministic teardown, exposing its record counts and a typed lifecycle emitter.
 */
export interface PoolInterface<T> {
	/** Holds the typed synchronous lifecycle observation surface. */
	readonly emitter: EmitterInterface<PoolEventMap>
	/** Counts all owned records, including records validating, destroying, or retained after failed cleanup. */
	readonly size: number
	/** Counts the records immediately available without validation work. */
	readonly idle: number
	/** Counts the records represented by unsettled released-once lease tokens. */
	readonly active: number
	/**
	 * Fills the warm floor and resets the strikes of a spent floor; without a floor, resolves immediately.
	 *
	 * @returns A promise that resolves when the floor owns `min` live records
	 * @throws {@link PoolError} Thrown as a rejection with `code: 'create'` and the last cause
	 * when the bound is spent, `code: 'cleanup'` when retained failed cleanup prevents filling,
	 * or `code: 'destroyed'` when teardown begins.
	 */
	start(): Promise<void>
	/**
	 * Queues the caller in FIFO order, validates an idle record or waits for a floor refill, and
	 * creates on demand only without a floor.
	 *
	 * @param signal - Optional native cancellation signal
	 * @returns A promise for the unique resource lease
	 * @throws {@link PoolError} Thrown when `signal` is present and is not a native `AbortSignal`,
	 * with `code: 'invalid'`. This throw is synchronous rather than a rejected promise, so a caller
	 * that handles failures with `.catch()` alone misses it.
	 * @throws {@link PoolError} Thrown as a rejection when `destroy()` has already begun, with
	 * `code: 'destroyed'`; when the create hook fails or the floor is spent without an idle record,
	 * with `code: 'create'` and the last cause; and when an invalid record's cleanup fails or retained
	 * records block a floor with nothing idle, no refill running, and no disposal pending,
	 * with `code: 'cleanup'`. A `signal` that aborts rejects with the caller's exact `signal.reason` instead.
	 */
	acquire(signal?: AbortSignal): Promise<PoolToken<T>>
	/**
	 * Destroys the records that are idle at this call's synchronous snapshot and restores a started floor.
	 *
	 * @returns A promise that settles after every snapshot cleanup attempt
	 * @throws {@link PoolError} Thrown when `destroy()` has already begun, with `code: 'destroyed'`.
	 * @throws {@link PoolError} Thrown when a claimed record's destroy hook fails, with
	 * `code: 'cleanup'` and every distinct failure in `context.failures`. Each arrives as a rejected
	 * promise rather than a synchronous throw.
	 */
	clear(): Promise<void>
	/**
	 * Tears down the pool permanently and returns its stable completion barrier.
	 *
	 * @returns The exact promise shared by every destroy call
	 * @throws {@link PoolError} Thrown when a destroy hook failure was retained or occurred during teardown, with
	 * `code: 'cleanup'` and every distinct failure in `context.failures`. The barrier rejects; it
	 * never throws synchronously, and a repeat call receives the same rejected promise.
	 */
	destroy(): Promise<void>
}
