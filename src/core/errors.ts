import type { PoolErrorOptions } from './types.js'
import {
	holds,
	INTRINSICS,
	isArray,
	isError,
	isFunction,
	isObject,
	isString,
} from '@orkestrel/contract'

/**
 * Represents a stable, machine-readable pool failure that retains the original thrown value as
 * its cause without unsafe coercion, alongside structured context.
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
		const destroyed = options.code === 'destroyed' ? 'pool is destroyed' : 'pool input is invalid'
		const created = options.code === 'create' ? 'pool create failed' : destroyed
		const reason = options.code === 'cleanup' ? 'pool cleanup failed' : created
		const message =
			isError(options.cause) && isString(options.cause.message) && options.cause.message.length > 0
				? `${reason}: ${options.cause.message}`
				: reason
		super(message, options.cause === undefined ? undefined : { cause: options.cause })
		INTRINSICS.define(this, Symbol.for('@orkestrel/pool.PoolError'), { value: this })
		this.name = 'PoolError'
		this.code = options.code
		this.context = options.context
	}
}

/**
 * Tests whether an unknown value is a branded {@link PoolError} with declared code and context.
 *
 * @remarks
 * Accepts subclasses and errors from another installed copy without reading `name`. Refuses
 * transparent proxies, unbranded look-alikes, and plain branded objects.
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
	if (!isObject(value)) return false
	return holds(() => {
		if (!(value instanceof INTRINSICS.error)) return false
		if (INTRINSICS.prototype(value) === INTRINSICS.error.prototype) return false
		const brand = INTRINSICS.describe(value, Symbol.for('@orkestrel/pool.PoolError'))
		if (brand?.value !== value) return false
		const code: unknown = INTRINSICS.reflect.read(value, 'code')
		if (code !== 'invalid' && code !== 'destroyed' && code !== 'create' && code !== 'cleanup')
			return false
		const context: unknown = INTRINSICS.reflect.read(value, 'context')
		if (context === undefined) return true
		if (!isObject(context) && !isFunction(context)) return false
		const failures: unknown = INTRINSICS.reflect.read(context, 'failures')
		return failures === undefined || isArray(failures)
	})
}
