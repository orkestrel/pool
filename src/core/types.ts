import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'

/** Names the machine-readable failure codes produced by {@link PoolError}. */
export type PoolCode = 'invalid' | 'destroyed' | 'create' | 'cleanup'

/** Represents the structured context attached to a {@link PoolError}. */
export interface PoolContext {
	/** Holds the rejected public input, when the failure is an input-validation error. */
	readonly value?: unknown
	/** Holds distinct destroy-hook failures collected by `clear()` or `destroy()`. */
	readonly failures?: readonly unknown[]
}

/** Represents the construction options for {@link PoolError}. */
export interface PoolErrorOptions {
	/** Holds the stable machine-readable failure category. */
	readonly code: PoolCode
	/** Holds the original thrown value, retained without unsafe string coercion. */
	readonly cause?: unknown
	/** Holds optional structured failure details. */
	readonly context?: PoolContext
}

/** Represents the observable resource lifecycle events emitted by a {@link PoolInterface}. */
export type PoolEventMap = {
	/** Signals that a created resource entered pool ownership. */
	readonly create: readonly []
	/** Signals that a token settled successfully and its exact resource became leased. */
	readonly acquire: readonly []
	/** Signals that a released resource became immediately idle. */
	readonly release: readonly []
	/** Signals that a resource destroy hook completed or was attempted when absent. */
	readonly destroy: readonly []
}

/** Represents a unique lease over one pool-owned resource record. */
export interface PoolToken<T> {
	/** Holds the leased value. Duplicate values still belong to independent records. */
	readonly value: T
	/** Gives this exact lease back once; subsequent calls are no-ops. */
	release(): void
}

/**
 * Represents the resource lifecycle options for {@link Pool} and `createPool`.
 *
 * @remarks
 * `create` lazily produces resources. `destroy` tears down a claimed resource.
 * `validate` checks a previously owned resource before reuse. `max` is a positive
 * safe integer; omission is the only unbounded form. `on` installs initial emitter
 * listeners and `error` receives isolated listener failures.
 */
export interface PoolOptions<T> {
	readonly on?: EmitterHooks<PoolEventMap>
	readonly error?: EmitterErrorHandler
	readonly create: () => Promise<T> | T
	readonly destroy?: (value: T) => Promise<void> | void
	readonly validate?: (value: T) => Promise<boolean> | boolean
	readonly max?: number
}

/** Represents a FIFO resource pool with optional bounded capacity and deterministic teardown. */
export interface PoolInterface<T> {
	/** Holds the typed synchronous lifecycle observation surface. */
	readonly emitter: EmitterInterface<PoolEventMap>
	/** Counts all owned records, including records validating or destroying. */
	readonly size: number
	/** Counts the records immediately available without validation work. */
	readonly idle: number
	/** Counts the records represented by unsettled released-once lease tokens. */
	readonly active: number
	/**
	 * Queues and leases one resource in FIFO settlement order.
	 *
	 * @param signal - Optional native cancellation signal
	 * @returns A promise for the unique resource lease
	 * @throws {@link PoolError} Thrown when `signal` is present and is not a native `AbortSignal`,
	 * with `code: 'invalid'`. This throw is synchronous rather than a rejected promise, so a caller
	 * that handles failures with `.catch()` alone misses it.
	 */
	acquire(signal?: AbortSignal): Promise<PoolToken<T>>
	/**
	 * Destroys the records that are idle at this call's synchronous snapshot.
	 *
	 * @returns A promise that settles after every snapshot cleanup attempt
	 */
	clear(): Promise<void>
	/**
	 * Tears down the pool permanently and returns its stable completion barrier.
	 *
	 * @returns The exact promise shared by every destroy call
	 */
	destroy(): Promise<void>
}
