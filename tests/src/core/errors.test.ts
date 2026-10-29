import type { PoolContext } from '@src/core'
import { PoolError, isPoolError } from '@src/core'
import { describe, expect, it } from 'vitest'

describe('isPoolError', () => {
	it('accepts a class-instance context', () => {
		class Context {
			readonly [key: string]: unknown
			readonly failures: readonly unknown[] = ['cleanup failed']
		}

		const context: PoolContext = new Context()
		const error = new PoolError({ code: 'cleanup', context })
		expect(isPoolError(error)).toBe(true)
	})

	it('accepts a callable context', () => {
		const members: PoolContext = { failures: ['cleanup failed'] }
		const context = Object.assign(() => undefined, members)
		const error = new PoolError({ code: 'cleanup', context })
		expect(isPoolError(error)).toBe(true)
	})
})
