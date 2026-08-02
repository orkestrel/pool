import type { EmitterErrorHandler, EmitterHooks, EmitterInterface } from '@orkestrel/emitter'

/** Machine-readable failure codes produced by {@link PoolError}. */
export type PoolCode = 'invalid' | 'destroyed' | 'create' | 'cleanup'

/** Structured context attached to a {@link PoolError}. */
export interface PoolContext {
	/** The rejected public input, when the failure is an input-validation error. */
	readonly value?: unknown
	/** Distinct cleanup failures collected by `clear()` or `destroy()`. */
	readonly failures?: readonly unknown[]
}

/** Construction options for {@link PoolError}. */
export interface PoolErrorOptions {
	/** The stable machine-readable failure category. */
	readonly code: PoolCode
	/** The original thrown value, retained without unsafe string coercion. */
	readonly cause?: unknown
	/** Optional structured failure details. */
	readonly context?: PoolContext
}

/** Observable resource lifecycle events emitted by a {@link PoolInterface}. */
export type PoolEventMap = {
	/** A created resource entered pool ownership. */
	readonly create: readonly []
	/** A token settled successfully and its exact resource became leased. */
	readonly acquire: readonly []
	/** A released resource became immediately idle. */
	readonly release: readonly []
	/** A resource cleanup hook completed or was attempted when absent. */
	readonly destroy: readonly []
}

/** A unique lease over one pool-owned resource record. */
export interface PoolToken<T> {
	/** The leased value. Duplicate values still belong to independent records. */
	readonly value: T
	/** Return this exact lease once; subsequent calls are no-ops. */
	release(): void
}

/**
 * Resource lifecycle options for {@link Pool} and `createPool`.
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

/** A FIFO resource pool with optional bounded capacity and deterministic teardown. */
export interface PoolInterface<T> {
	/** The typed synchronous lifecycle observation surface. */
	readonly emitter: EmitterInterface<PoolEventMap>
	/** All owned records, including records validating or destroying. */
	readonly size: number
	/** Records immediately available without validation work. */
	readonly idle: number
	/** Records represented by unsettled released-once lease tokens. */
	readonly active: number
	/**
	 * Queue and lease one resource in FIFO settlement order.
	 *
	 * @param signal - Optional native cancellation signal
	 * @returns A promise for the unique resource lease
	 */
	acquire(signal?: AbortSignal): Promise<PoolToken<T>>
	/**
	 * Destroy the records that are idle at this call's synchronous snapshot.
	 *
	 * @returns A promise that settles after every snapshot cleanup attempt
	 */
	clear(): Promise<void>
	/**
	 * Permanently tear down the pool and return its stable completion barrier.
	 *
	 * @returns The exact promise shared by every destroy call
	 */
	destroy(): Promise<void>
}
