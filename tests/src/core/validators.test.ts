import { describe, expect, it } from 'vitest'
import { isPoolLimit, isPoolSignal } from '@src/core'

describe('isPoolLimit', () => {
	it('accepts a positive safe integer', () => {
		expect(isPoolLimit(1)).toBe(true)
		expect(isPoolLimit(Number.MAX_SAFE_INTEGER)).toBe(true)
	})

	it('rejects zero, a negative, a fraction, NaN, Infinity, and an unsafe integer', () => {
		for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 53]) {
			expect(isPoolLimit(value)).toBe(false)
		}
	})

	it('rejects a candidate that is not a number', () => {
		expect(isPoolLimit('4')).toBe(false)
		expect(isPoolLimit(undefined)).toBe(false)
	})
})

describe('isPoolSignal', () => {
	it('accepts a native AbortSignal', () => {
		expect(isPoolSignal(new AbortController().signal)).toBe(true)
	})

	it('rejects a proxy whose get trap throws and a plain aborted-shaped object', () => {
		const proxied = new Proxy(new AbortController().signal, {
			get() {
				throw new Error('signal trap')
			},
		})

		expect(isPoolSignal(proxied)).toBe(false)
		expect(isPoolSignal({ aborted: false })).toBe(false)
	})
})
