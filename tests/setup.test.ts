import { describe, expect, it } from 'vitest'
import { POOL_EVENTS } from './setup.js'

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
