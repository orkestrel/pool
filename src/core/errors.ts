import type { PoolErrorOptions } from './types.js'

/**
 * Represents a stable, machine-readable pool failure with the original cause and structured
 * context.
 *
 * @example
 * ```ts
 * import { PoolError, isPoolError } from '@orkestrel/pool'
 *
 * try {
 * 	await pool.acquire()
 * } catch (error: unknown) {
 * 	if (isPoolError(error)) console.error(error.code, error.cause)
 * }
 * ```
 */
export class PoolError extends Error {
	/** Holds the stable machine-readable failure category. */
	readonly code
	/** Holds optional structured input or aggregate destroy-hook failure details. */
	readonly context

	/**
	 * Creates a pool failure without coercing a hostile thrown value.
	 *
	 * @param options - Stable code plus optional cause and structured context
	 */
	constructor(options: PoolErrorOptions) {
		let message = 'pool input is invalid'
		if (options.code === 'destroyed') message = 'pool is destroyed'
		if (options.code === 'create') message = 'pool create failed'
		if (options.code === 'cleanup') message = 'pool cleanup failed'
		try {
			if (
				options.cause instanceof Error &&
				typeof options.cause.message === 'string' &&
				options.cause.message.length > 0
			) {
				message = `${message}: ${options.cause.message}`
			}
		} catch {}
		super(message, options.cause === undefined ? undefined : { cause: options.cause })
		this.name = 'PoolError'
		this.code = options.code
		this.context = options.context
	}
}

/**
 * Tests whether an unknown value is a {@link PoolError}, returning `false` for hostile proxies.
 *
 * @param value - The unknown boundary value
 * @returns True if the value is a real `PoolError` instance; false otherwise
 *
 * @example
 * ```ts
 * isPoolError(new PoolError({ code: 'destroyed' })) // true
 * isPoolError(new Error('other')) // false
 * ```
 */
export function isPoolError(value: unknown): value is PoolError {
	try {
		return value instanceof PoolError
	} catch {
		return false
	}
}
