import { describe, expect, it } from 'vitest'
import { Pool, createPool } from '@src/core'

describe('createPool', () => {
	it('constructs a distinct Pool instance for each call', () => {
		const first = createPool({ create: () => 1 })
		const second = createPool({ create: () => 1 })

		expect(first).toBeInstanceOf(Pool)
		expect(second).toBeInstanceOf(Pool)
		expect(first).not.toBe(second)
	})
})
