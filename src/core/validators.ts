/**
 * Tests whether a value is a positive safe integer, the only valid explicit pool maximum.
 *
 * @param value - The unknown maximum candidate
 * @returns True if the value is a positive safe integer; false otherwise
 *
 * @example
 * ```ts
 * isPoolMax(8) // true
 * isPoolMax(Infinity) // false
 * ```
 */
export function isPoolMax(value: unknown): value is number {
	return typeof value === 'number' && Number.isSafeInteger(value) && value > 0
}

/**
 * Tests whether a value is a native `AbortSignal` for the acquire boundary, returning `false`
 * for hostile proxies.
 *
 * @param value - The unknown signal candidate
 * @returns True if the value is a native `AbortSignal`; false otherwise
 *
 * @example
 * ```ts
 * isPoolSignal(new AbortController().signal) // true
 * isPoolSignal({ aborted: false }) // false
 * ```
 */
export function isPoolSignal(value: unknown): value is AbortSignal {
	try {
		const getter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')?.get
		if (getter === undefined) return false
		Reflect.apply(getter, value, [])
		return true
	} catch {
		return false
	}
}
