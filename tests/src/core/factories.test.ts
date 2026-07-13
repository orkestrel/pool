import { describe, expect, it } from 'vitest'
import { createPool } from '@src/core'
import { waitForDelay } from '../../setup.js'

// src/core/workers/factories.ts — createQueue / createPool / createWorker each wire up
// a working, typed interface end to end (AGENTS §16).

describe('createPool', () => {
	it('returns a working pool that leases, reuses on release, and reports counts', async () => {
		let next = 0
		const pool = createPool<number>({ create: () => next++, max: 1 })
		expect(pool.size).toBe(0)

		const token = await pool.acquire()
		expect(token.value).toBe(0)
		expect(pool.active).toBe(1)

		token.release()
		expect(pool.idle).toBe(1)
		// Reuses the idle resource — no second create.
		const again = await pool.acquire()
		expect(again.value).toBe(0)
	})

	it('parks an acquire at max and serves it on release (FIFO)', async () => {
		const pool = createPool<number>({ create: () => 0, max: 1 })
		const held = await pool.acquire()
		const waiting = pool.acquire()
		await waitForDelay(10)
		expect(pool.active).toBe(1)

		held.release()
		const served = await waiting
		expect(served.value).toBe(0)
	})
})
