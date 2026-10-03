import { describe, expect, it } from 'vitest'
import { POOL_EVENTS, createFloorFixture } from './setup.js'

describe('createFloorFixture', () => {
	it('records real hook calls, resource identity, and disposal signals', async () => {
		const fixture = createFloorFixture()
		await fixture.pool.start()
		const token = await fixture.pool.acquire()
		expect(fixture.resources).toEqual([token.value])
		expect(fixture.signals.get(token.value)?.aborted).toBe(false)
		await fixture.pool.destroy()
		expect(fixture.destroyed).toEqual([token.value])
		expect(fixture.signals.get(token.value)?.aborted).toBe(true)
		expect(fixture.attempts).toBe(1)
	})
})

describe('POOL_EVENTS', () => {
	it('is frozen so a consumer cannot mutate the shared table', () => {
		expect(Object.isFrozen(POOL_EVENTS)).toBe(true)
	})

	it('carries no duplicate event name', () => {
		expect(new Set(POOL_EVENTS).size).toBe(POOL_EVENTS.length)
	})

	it('covers exactly the lifecycle events a Pool emitter recorder must bind, derived independently from the emitted payload shape', () => {
		// Second route: a literal keyed by the same lifecycle events `PoolEventMap` names,
		// each carrying its emitted (empty) payload tuple, so the membership check does not
		// read `POOL_EVENTS` itself back through the type it is meant to prove.
		const emittedPayloads: Readonly<Record<(typeof POOL_EVENTS)[number], readonly []>> =
			Object.freeze({
				create: [],
				acquire: [],
				release: [],
				destroy: [],
			})

		expect([...POOL_EVENTS].sort()).toEqual(Object.keys(emittedPayloads).sort())
	})
})
