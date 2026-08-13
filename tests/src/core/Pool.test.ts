import { describe, expect, it } from 'vitest'
import { Pool, PoolError, isPoolError, isPoolMax, isPoolSignal } from '@src/core'
import { createRecorder } from '@orkestrel/test'
import { createErrorRecorder, createGate, recordEmitterEvents } from '../../setup.js'

describe('Pool validation and errors', () => {
	it('accepts only positive safe integer maxima and omission remains unbounded', () => {
		expect(isPoolMax(1)).toBe(true)
		expect(isPoolMax(Number.MAX_SAFE_INTEGER)).toBe(true)
		for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 53]) {
			expect(isPoolMax(value)).toBe(false)
			expect(() => new Pool({ create: () => 0, max: value })).toThrow(PoolError)
		}
		expect(() => new Pool({ create: () => 0 })).not.toThrow()
	})

	it('snapshots a volatile maximum and observation hooks exactly once', async () => {
		const initialCreate = createRecorder<[]>()
		const laterCreate = createRecorder<[]>()
		const initialErrors = createErrorRecorder()
		const laterErrors = createErrorRecorder()
		const listenerFailure = new Error('volatile listener failed')
		let maxReads = 0
		let onReads = 0
		let errorReads = 0
		const pool = new Pool({
			create: () => 0,
			get max() {
				maxReads += 1
				return maxReads === 1 ? 1 : 0
			},
			get on() {
				onReads += 1
				return onReads === 1 ? { create: initialCreate.handler } : { create: laterCreate.handler }
			},
			get error() {
				errorReads += 1
				return errorReads === 1 ? initialErrors.handler : laterErrors.handler
			},
		})

		try {
			expect([maxReads, onReads, errorReads]).toEqual([1, 1, 1])
			pool.emitter.on('acquire', () => {
				throw listenerFailure
			})
			const first = await pool.acquire()
			try {
				expect([maxReads, pool.size, pool.active]).toEqual([1, 1, 1])
				expect(initialCreate.count).toBe(1)
				expect(laterCreate.count).toBe(0)
				expect(initialErrors.calls).toEqual([[listenerFailure, 'acquire']])
				expect(laterErrors.count).toBe(0)
				const second = pool.acquire()
				void second.catch(() => {})
				first.release()
				const next = await second
				try {
					expect(next.value).toBe(0)
				} finally {
					next.release()
				}
			} finally {
				first.release()
			}
		} finally {
			await pool.destroy()
		}
	})

	it('recognizes only native signals and synchronously throws for an invalid acquire signal', () => {
		const pool = new Pool({ create: () => 0 })
		const signal = new AbortController().signal
		const proxied = new Proxy(signal, {
			get() {
				throw new Error('signal trap')
			},
		})
		expect(isPoolSignal(signal)).toBe(true)
		expect(isPoolSignal(proxied)).toBe(false)
		expect(isPoolSignal({ aborted: false })).toBe(false)
		expect(() => Reflect.apply(pool.acquire, pool, [{ aborted: false }])).toThrow(PoolError)
		expect(() => pool.acquire(proxied)).toThrow(PoolError)
	})

	it('reads native abort state and reason without traversing hostile own accessors', async () => {
		const controller = new AbortController()
		const reason = new Error('native reason')
		const hostile = new Error('hostile getter')
		controller.abort(reason)
		Object.defineProperties(controller.signal, {
			aborted: {
				get: () => {
					throw hostile
				},
			},
			reason: {
				get: () => {
					throw hostile
				},
			},
		})
		const pool = new Pool({ create: () => 0 })

		expect(isPoolSignal(controller.signal)).toBe(true)
		await expect(pool.acquire(controller.signal)).rejects.toBe(reason)
	})

	it('narrows PoolError without traversing hostile values or coercing hostile hook failures', async () => {
		const hostile = new Proxy(
			{},
			{
				getPrototypeOf() {
					throw new Error('prototype trap')
				},
			},
		)
		const error = new PoolError({ code: 'create', cause: hostile, context: { value: hostile } })

		expect(error.message).toBe('pool create failed')
		expect(error.cause).toBe(hostile)
		expect(isPoolError(error)).toBe(true)
		expect(isPoolError(new Error('other'))).toBe(false)
		expect(isPoolError(hostile)).toBe(false)

		const pool = new Pool({
			create: () => {
				throw hostile
			},
		})
		const [result] = await Promise.allSettled([pool.acquire()])
		if (result === undefined || result.status === 'fulfilled' || !isPoolError(result.reason)) {
			throw new Error('expected a PoolError create failure')
		}
		expect(result.reason.code).toBe('create')
		expect(result.reason.cause).toBe(hostile)
		expect(result.reason.message).toBe('pool create failed')
	})
})

describe('Pool ownership and counts', () => {
	it('owns duplicate undefined, NaN, primitive, and object values as distinct records', async () => {
		const shared = {}
		const values: readonly unknown[] = [
			undefined,
			undefined,
			Number.NaN,
			Number.NaN,
			1,
			1,
			shared,
			shared,
		]
		let index = 0
		const destroyed = createRecorder<[unknown]>()
		const pool = new Pool<unknown>({
			create: () => {
				const value = values[index]
				index += 1
				return value
			},
			destroy: destroyed.handler,
			max: values.length,
		})

		const tokens = await Promise.all(values.map(() => pool.acquire()))
		expect(pool.size).toBe(values.length)
		expect(pool.active).toBe(values.length)
		for (const token of tokens) token.release()
		expect(pool.idle).toBe(values.length)
		await pool.clear()
		expect(destroyed.count).toBe(values.length)
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
	})

	it('reports reservations, validation, leases, idle records, and cleanup exactly', async () => {
		const creation = createGate<number>()
		const validation = createGate<boolean>()
		const cleanup = createGate<void>()
		const pool = new Pool({
			create: () => creation.promise,
			validate: () => validation.promise,
			destroy: () => cleanup.promise,
			max: 1,
		})

		const growing = pool.acquire()
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
		creation.resolve(4)
		const first = await growing
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
		first.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])

		const validating = pool.acquire()
		await Promise.resolve()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 0])
		validation.resolve(true)
		const second = await validating
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 1])
		second.release()

		const clearing = pool.clear()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 0])
		cleanup.resolve()
		await clearing
		expect([pool.size, pool.idle, pool.active]).toEqual([0, 0, 0])
	})

	it('releases each exact token once and ignores release after teardown ownership transfers', async () => {
		const cleanup = createGate<void>()
		const pool = new Pool({ create: () => 1, destroy: () => cleanup.promise })
		const token = await pool.acquire()

		token.release()
		token.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])
		const destroying = pool.destroy()
		token.release()
		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 0])
		cleanup.resolve()
		await destroying
		expect(pool.size).toBe(0)
	})
})

describe('Pool FIFO acquisition', () => {
	it('overlaps creates up to max without letting out-of-order completion overtake the head', async () => {
		const gates = [createGate<number>(), createGate<number>(), createGate<number>()]
		const settled = createRecorder<[number]>()
		let created = 0
		const pool = new Pool({ create: () => gates[created++]?.promise ?? Promise.reject(), max: 3 })
		const first = pool.acquire()
		const second = pool.acquire()
		const third = pool.acquire()
		void first.then(() => settled.handler(0))
		void second.then(() => settled.handler(1))
		void third.then(() => settled.handler(2))

		await Promise.resolve()
		expect(created).toBe(3)
		expect(pool.size).toBe(0)
		gates[1]?.resolve(20)
		gates[2]?.resolve(30)
		await Promise.resolve()
		await Promise.resolve()
		expect(settled.count).toBe(0)
		expect([pool.size, pool.idle, pool.active]).toEqual([2, 0, 0])
		gates[0]?.resolve(10)
		const tokens = await Promise.all([first, second, third])
		expect(tokens.map((token) => token.value)).toEqual([10, 20, 30])
		expect(settled.calls).toEqual([[0], [1], [2]])
		expect(pool.size).toBe(3)
	})

	it('does not create beyond capacity while records are leased or reserved', async () => {
		let created = 0
		const pool = new Pool({ create: () => created++, max: 1 })
		const first = await pool.acquire()
		const second = pool.acquire()
		const third = pool.acquire()

		await Promise.resolve()
		expect(created).toBe(1)
		first.release()
		const next = await second
		expect([next.value, created]).toEqual([0, 1])
		next.release()
		await expect(third).resolves.toMatchObject({ value: 0 })
		expect(created).toBe(1)
	})

	it('overlaps validations while retaining FIFO settlement', async () => {
		const gates = [createGate<boolean>(), createGate<boolean>()]
		const settled = createRecorder<[number]>()
		let created = 0
		let validated = 0
		const pool = new Pool({
			create: () => created++,
			validate: () => gates[validated++]?.promise ?? false,
			max: 2,
		})
		const held = await Promise.all([pool.acquire(), pool.acquire()])
		for (const token of held) token.release()
		const first = pool.acquire()
		const second = pool.acquire()
		void first.then(() => settled.handler(0))
		void second.then(() => settled.handler(1))
		await Promise.resolve()
		expect(validated).toBe(2)
		gates[1]?.resolve(true)
		await Promise.resolve()
		await Promise.resolve()
		expect(settled.count).toBe(0)
		gates[0]?.resolve(true)
		await Promise.all([first, second])
		expect(settled.calls).toEqual([[0], [1]])
	})

	it('rejects a failed create with code and continues later FIFO waiters', async () => {
		const gates = [createGate<number>(), createGate<number>()]
		let created = 0
		const pool = new Pool({ create: () => gates[created++]?.promise ?? Promise.reject(), max: 2 })
		const first = pool.acquire()
		const second = pool.acquire()
		const failure = new Error('creation failed')

		gates[1]?.resolve(2)
		await Promise.resolve()
		gates[0]?.reject(failure)
		await expect(first).rejects.toMatchObject({ code: 'create', cause: failure })
		await expect(second).resolves.toMatchObject({ value: 2 })
	})

	it('validates a released handoff and emits release only when a record actually stays idle', async () => {
		let healthy = true
		let created = 0
		const pool = new Pool({ create: () => created++, validate: () => healthy, max: 1 })
		const events = recordEmitterEvents(pool.emitter, ['create', 'acquire', 'release', 'destroy'])
		const held = await pool.acquire()
		const waiting = pool.acquire()
		held.release()
		const served = await waiting
		expect(served.value).toBe(0)
		expect(events.release.count).toBe(0)
		served.release()
		expect(events.release.count).toBe(1)

		healthy = false
		const replacing = pool.acquire()
		const replacement = await replacing
		expect(replacement.value).toBe(1)
		expect(events.destroy.count).toBe(1)
	})
})

describe('Pool cancellation', () => {
	it('preserves a caller reason when already aborted or parked', async () => {
		const reason = { reason: 'stop' }
		const before = new AbortController()
		before.abort(reason)
		const pool = new Pool({ create: () => 0, max: 1 })
		await expect(pool.acquire(before.signal)).rejects.toBe(reason)

		const held = await pool.acquire()
		const parked = new AbortController()
		const waiting = pool.acquire(parked.signal)
		parked.abort(reason)
		await expect(waiting).rejects.toBe(reason)
		held.release()
		expect(pool.idle).toBe(1)
	})

	it('aborts an assigned create and recycles its late resource without leaking the listener', async () => {
		const creation = createGate<number>()
		const controller = new AbortController()
		const reason = new Error('assigned abort')
		const pool = new Pool({ create: () => creation.promise, max: 1 })
		const acquiring = pool.acquire(controller.signal)
		controller.abort(reason)
		await expect(acquiring).rejects.toBe(reason)
		creation.resolve(7)
		await Promise.resolve()
		await Promise.resolve()
		const next = await pool.acquire()
		expect(next.value).toBe(7)
		controller.abort(new Error('late abort'))
		expect(pool.active).toBe(1)
	})

	it('aborts a ready result behind a slow head and returns that record to idle', async () => {
		const gates = [createGate<number>(), createGate<number>()]
		let created = 0
		const controller = new AbortController()
		const reason = new Error('ready abort')
		const pool = new Pool({ create: () => gates[created++]?.promise ?? Promise.reject(), max: 2 })
		const first = pool.acquire()
		const second = pool.acquire(controller.signal)

		gates[1]?.resolve(2)
		await Promise.resolve()
		await Promise.resolve()
		controller.abort(reason)
		await expect(second).rejects.toBe(reason)
		expect(pool.idle).toBe(1)
		gates[0]?.resolve(1)
		await expect(first).resolves.toMatchObject({ value: 1 })
		expect(pool.size).toBe(2)
	})

	it('aborts assigned validation and recycles its valid late result', async () => {
		const validation = createGate<boolean>()
		const controller = new AbortController()
		const reason = new Error('validation abort')
		let validations = 0
		let created = 0
		const pool = new Pool({
			create: () => created++,
			validate: () => {
				validations += 1
				return validations === 1 ? validation.promise : true
			},
			max: 1,
		})
		const held = await pool.acquire()
		held.release()
		const acquiring = pool.acquire(controller.signal)
		await Promise.resolve()
		controller.abort(reason)
		await expect(acquiring).rejects.toBe(reason)

		const next = pool.acquire()
		validation.resolve(true)
		await expect(next).resolves.toMatchObject({ value: 0 })
		expect([created, pool.size, pool.active]).toEqual([1, 1, 1])
	})
})

describe('Pool validation and cleanup ownership', () => {
	it('treats false and thrown validation as invalid and replaces only after cleanup', async () => {
		const cleanup = createGate<void>()
		let created = 0
		let mode = 0
		const pool = new Pool({
			create: () => created++,
			validate: () => {
				mode += 1
				if (mode === 1) return false
				throw new Error('invalid')
			},
			destroy: () => cleanup.promise,
			max: 1,
		})
		const held = await pool.acquire()
		held.release()
		const replacing = pool.acquire()
		await Promise.resolve()
		expect(created).toBe(1)
		expect(pool.size).toBe(1)
		cleanup.resolve()
		const replacement = await replacing
		expect(replacement.value).toBe(1)
		replacement.release()
		await expect(pool.acquire()).resolves.toMatchObject({ value: 2 })
	})

	it('routes invalid-handoff cleanup failure to its bound waiter and continues the next waiter', async () => {
		const failure = new Error('cleanup failed')
		let created = 0
		const pool = new Pool({
			create: () => created++,
			validate: () => false,
			destroy: () => {
				if (created === 1) throw failure
			},
			max: 1,
		})
		const held = await pool.acquire()
		const first = pool.acquire()
		const second = pool.acquire()
		held.release()
		await expect(first).rejects.toMatchObject({ code: 'cleanup', cause: failure })
		await expect(second).resolves.toMatchObject({ value: 1 })
	})

	it('keeps delayed invalid-cleanup failure bound while one later waiter gets replacement capacity', async () => {
		for (const maximum of [2, undefined]) {
			const cleanup = createGate<void>()
			const entered = createGate<void>()
			const replacement = createGate<void>()
			const failure = new Error('invalid cleanup failed')
			let created = 0
			let destroyed = 0
			const pool = new Pool({
				create: () => {
					const value = created
					created += 1
					if (created === 2) replacement.resolve()
					return value
				},
				validate: () => false,
				destroy: () => {
					destroyed += 1
					if (destroyed === 1) {
						entered.resolve()
						return cleanup.promise
					}
				},
				...(maximum === undefined ? {} : { max: maximum }),
			})
			const held = await pool.acquire()
			held.release()
			const first = pool.acquire()
			await entered.promise
			const second = pool.acquire()
			void first.then(
				(token) => token.release(),
				() => {},
			)
			void second.then(
				(token) => token.release(),
				() => {},
			)
			const outcomes = Promise.allSettled([first, second])

			try {
				await replacement.promise
				cleanup.reject(failure)
				const [firstResult, secondResult] = await outcomes
				if (firstResult === undefined || firstResult.status !== 'rejected') {
					throw new Error('expected the invalid-cleanup waiter to reject')
				}
				if (!isPoolError(firstResult.reason)) throw new Error('expected a PoolError')
				expect(firstResult.reason.code).toBe('cleanup')
				expect(firstResult.reason.cause).toBe(failure)
				if (secondResult === undefined || secondResult.status !== 'fulfilled') {
					throw new Error('expected the later waiter to receive the replacement')
				}
				expect(secondResult.value.value).toBe(1)
				expect([created, destroyed, pool.size, pool.idle, pool.active]).toEqual([2, 1, 1, 1, 0])
			} finally {
				held.release()
				cleanup.resolve()
				await Promise.allSettled([outcomes, pool.destroy()])
			}
		}
	})

	it('preserves the FIFO head assignment through reentrant invalid cleanup', async () => {
		const replacement = createGate<void>()
		const settled = createRecorder<[number, number]>()
		let created = 0
		let validated = 0
		let reentered: ReturnType<Pool<number>['acquire']> | undefined
		const pool = new Pool({
			create: () => created++,
			validate: () => {
				validated += 1
				return validated > 1
			},
			destroy: () => {},
			on: {
				create: () => {
					if (created === 2) replacement.resolve()
				},
				destroy: () => {
					if (reentered === undefined) {
						reentered = pool.acquire()
						void reentered.catch(() => {})
					}
				},
			},
			max: 1,
		})
		const held = await pool.acquire()
		const first = pool.acquire()
		const second = pool.acquire()
		const firstUse = first.then((token) => {
			try {
				settled.handler(0, token.value)
			} finally {
				token.release()
			}
		})
		const secondUse = second.then((token) => {
			try {
				settled.handler(1, token.value)
			} finally {
				token.release()
			}
		})
		void firstUse.catch(() => {})
		void secondUse.catch(() => {})
		let thirdUse: Promise<void> | undefined

		try {
			held.release()
			await replacement.promise
			const third = reentered
			if (third === undefined) throw new Error('destroy listener did not reenter acquisition')
			thirdUse = third.then((token) => {
				try {
					settled.handler(2, token.value)
				} finally {
					token.release()
				}
			})
			await Promise.all([firstUse, secondUse, thirdUse])
			expect(settled.calls).toEqual([
				[0, 1],
				[1, 1],
				[2, 1],
			])
			expect([created, validated]).toEqual([2, 3])
			expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])
			await pool.destroy()
		} finally {
			held.release()
			await Promise.allSettled([
				firstUse,
				secondUse,
				...(thirdUse === undefined ? [] : [thirdUse]),
				pool.destroy(),
			])
		}
	})

	it('establishes invalid-cleanup failure before synchronous destroy observation reenters', async () => {
		const observed = createGate<void>()
		const settled = createRecorder<[number, number]>()
		const rejected = createRecorder<[unknown]>()
		const cleanupFailure = new Error('reentrant cleanup failed')
		const abortReason = new Error('reentrant abort must be too late')
		const controller = new AbortController()
		let created = 0
		let validated = 0
		let destroyed = 0
		let reentered: ReturnType<Pool<number>['acquire']> | undefined
		const pool = new Pool({
			create: () => created++,
			validate: () => {
				validated += 1
				return validated > 1
			},
			destroy: () => {
				destroyed += 1
				if (destroyed === 1) throw cleanupFailure
			},
			on: {
				destroy: () => {
					if (reentered !== undefined) return
					controller.abort(abortReason)
					reentered = pool.acquire()
					void reentered.catch(() => {})
					observed.resolve()
				},
			},
			max: 1,
		})
		const held = await pool.acquire()
		held.release()
		const first = pool.acquire(controller.signal)
		const second = pool.acquire()
		const firstUse = first.then(
			(token) => {
				settled.handler(0, token.value)
				token.release()
			},
			(error: unknown) => rejected.handler(error),
		)
		const secondUse = second.then((token) => {
			try {
				settled.handler(1, token.value)
			} finally {
				token.release()
			}
		})
		void secondUse.catch(() => {})
		let thirdUse: Promise<void> | undefined

		try {
			await observed.promise
			const third = reentered
			if (third === undefined) throw new Error('destroy listener did not reenter acquisition')
			thirdUse = third.then((token) => {
				try {
					settled.handler(2, token.value)
				} finally {
					token.release()
				}
			})
			await Promise.all([firstUse, secondUse, thirdUse])
			expect(rejected.count).toBe(1)
			const failure = rejected.calls[0]?.[0]
			if (!isPoolError(failure)) throw new Error('expected a cleanup PoolError')
			expect(failure.code).toBe('cleanup')
			expect(failure.cause).toBe(cleanupFailure)
			expect(failure).not.toBe(abortReason)
			expect(settled.calls).toEqual([
				[1, 1],
				[2, 1],
			])
			expect([created, validated, destroyed]).toEqual([2, 2, 1])
			expect([pool.size, pool.idle, pool.active]).toEqual([1, 1, 0])
		} finally {
			held.release()
			await Promise.allSettled([
				firstUse,
				secondUse,
				...(thirdUse === undefined ? [] : [thirdUse]),
				pool.destroy(),
			])
		}
	})

	it('deduplicates repeated clear failure identities while removing every claimed record', async () => {
		const shared = new Error('shared')
		const pool = new Pool({
			create: () => 0,
			destroy: () => {
				throw shared
			},
		})
		const tokens = await Promise.all([pool.acquire(), pool.acquire()])
		for (const token of tokens) token.release()

		const clearing = pool.clear()
		await expect(clearing).rejects.toMatchObject({
			code: 'cleanup',
			context: { failures: [shared] },
		})
		expect(pool.size).toBe(0)
	})

	it('repumps a capacity-blocked acquire after clear cleanup completes', async () => {
		const cleanup = createGate<void>()
		let created = 0
		const pool = new Pool({ create: () => created++, destroy: () => cleanup.promise, max: 1 })
		const held = await pool.acquire()
		held.release()
		const clearing = pool.clear()
		const acquiring = pool.acquire()

		await Promise.resolve()
		expect(created).toBe(1)
		cleanup.resolve()
		await clearing
		await expect(acquiring).resolves.toMatchObject({ value: 1 })
		expect(created).toBe(2)
	})

	it('frees capacity after failed clear cleanup and preserves the cleanup error', async () => {
		const cleanup = createGate<void>()
		const failure = new Error('clear failed')
		let created = 0
		const pool = new Pool({ create: () => created++, destroy: () => cleanup.promise, max: 1 })
		const held = await pool.acquire()
		held.release()
		const clearing = pool.clear()
		const acquiring = pool.acquire()

		cleanup.reject(failure)
		await expect(clearing).rejects.toMatchObject({ code: 'cleanup', cause: failure })
		await expect(acquiring).resolves.toMatchObject({ value: 1 })
		expect(created).toBe(2)
	})

	it('retains cleanup failure after cancelled invalid validation for eventual teardown', async () => {
		const validation = createGate<boolean>()
		const cleaned = createGate<void>()
		const failure = new Error('orphan cleanup failed')
		const reason = new Error('cancel validation')
		const controller = new AbortController()
		const pool = new Pool({
			create: () => 1,
			validate: () => validation.promise,
			destroy: () => {
				throw failure
			},
			max: 1,
		})
		pool.emitter.on('destroy', () => cleaned.resolve())
		const held = await pool.acquire()
		held.release()
		const acquiring = pool.acquire(controller.signal)
		await Promise.resolve()
		controller.abort(reason)
		await expect(acquiring).rejects.toBe(reason)

		validation.resolve(false)
		await cleaned.promise
		await expect(pool.destroy()).rejects.toMatchObject({ code: 'cleanup', cause: failure })
	})

	it('retains cleanup failure when cancellation interrupts invalid cleanup', async () => {
		const cleanup = createGate<void>()
		const entered = createGate<void>()
		const cleaned = createGate<void>()
		const failure = new Error('interrupted cleanup failed')
		const reason = new Error('cancel cleanup')
		const controller = new AbortController()
		const pool = new Pool({
			create: () => 1,
			validate: () => false,
			destroy: () => {
				entered.resolve()
				return cleanup.promise
			},
			max: 1,
		})
		pool.emitter.on('destroy', () => cleaned.resolve())
		const held = await pool.acquire()
		held.release()
		const acquiring = pool.acquire(controller.signal)
		await entered.promise

		controller.abort(reason)
		await expect(acquiring).rejects.toBe(reason)
		cleanup.reject(failure)
		await cleaned.promise
		await Promise.resolve()
		await expect(pool.destroy()).rejects.toMatchObject({ code: 'cleanup', cause: failure })
	})

	it('gives concurrent clears disjoint snapshots and excludes a lease released after the first snapshot', async () => {
		const gates = [createGate<void>(), createGate<void>(), createGate<void>()]
		const destroyed = createRecorder<[number]>()
		let created = 0
		const pool = new Pool({
			create: () => created++,
			destroy: (value) => {
				destroyed.handler(value)
				return gates[value]?.promise
			},
		})
		const tokens = await Promise.all([pool.acquire(), pool.acquire(), pool.acquire()])
		tokens[0]?.release()
		tokens[1]?.release()
		const first = pool.clear()
		tokens[2]?.release()
		const second = pool.clear()
		expect(destroyed.calls).toEqual([[0], [1], [2]])
		for (const gate of gates) gate.resolve()
		await Promise.all([first, second])
		expect(pool.size).toBe(0)
	})
})

describe('Pool destruction', () => {
	it('returns one exact stable promise and remains reentrant through synchronous destroy events', async () => {
		const cleanup = createGate<void>()
		const pool = new Pool({ create: () => 1, destroy: () => cleanup.promise })
		const token = await pool.acquire()
		token.release()
		let reentered: Promise<void> | undefined
		pool.emitter.on('destroy', () => {
			reentered = pool.destroy()
		})

		const first = pool.destroy()
		const second = pool.destroy()
		expect(first).toBe(second)
		cleanup.resolve()
		await first
		expect(reentered).toBe(first)
		expect(pool.emitter.destroyed).toBe(true)
		await expect(pool.acquire()).rejects.toMatchObject({ code: 'destroyed' })
	})

	it('waits for a late create, emits create after ledger insertion, then destroys its resource', async () => {
		const creation = createGate<number>()
		const events: string[] = []
		const pool = new Pool({
			create: () => creation.promise,
			destroy: () => {
				events.push('hook')
			},
			on: {
				create: () => events.push(`create:${pool.size}`),
				destroy: () => events.push(`destroy:${pool.size}`),
			},
		})
		const acquiring = pool.acquire()
		const destroying = pool.destroy()
		creation.resolve(9)
		await expect(acquiring).rejects.toMatchObject({ code: 'destroyed' })
		await destroying
		expect(events).toEqual(['create:1', 'hook', 'destroy:0'])
	})

	it('waits for a create rejection during teardown without treating it as cleanup failure', async () => {
		const creation = createGate<number>()
		const failure = new Error('late create failed')
		const pool = new Pool({ create: () => creation.promise })
		const acquiring = pool.acquire()
		const destroying = pool.destroy()

		await expect(acquiring).rejects.toMatchObject({ code: 'destroyed' })
		creation.reject(failure)
		await expect(destroying).resolves.toBeUndefined()
		expect(pool.emitter.destroyed).toBe(true)
	})

	it('waits for validation before cleaning the claimed record', async () => {
		const validation = createGate<boolean>()
		const destroyed = createRecorder<[number]>()
		const pool = new Pool({
			create: () => 1,
			validate: () => validation.promise,
			destroy: destroyed.handler,
		})
		const first = await pool.acquire()
		first.release()
		const acquiring = pool.acquire()
		await Promise.resolve()
		const destroying = pool.destroy()
		expect(destroyed.count).toBe(0)
		validation.resolve(true)
		await expect(acquiring).rejects.toMatchObject({ code: 'destroyed' })
		await destroying
		expect(destroyed.calls).toEqual([[1]])
	})

	it('reports cleanup failure when destruction intersects validation', async () => {
		const validation = createGate<boolean>()
		const failure = new Error('validation cleanup failed')
		const pool = new Pool({
			create: () => 1,
			validate: () => validation.promise,
			destroy: () => {
				throw failure
			},
		})
		const held = await pool.acquire()
		held.release()
		const acquiring = pool.acquire()
		await Promise.resolve()
		const destroying = pool.destroy()

		await expect(acquiring).rejects.toMatchObject({ code: 'destroyed' })
		validation.resolve(true)
		await expect(destroying).rejects.toMatchObject({ code: 'cleanup', cause: failure })
		expect([pool.size, pool.emitter.destroyed]).toEqual([0, true])
	})

	it('invalidates leased tokens synchronously and waits for unresolved cleanup', async () => {
		const cleanup = createGate<void>()
		const settled = createRecorder<[]>()
		const pool = new Pool({ create: () => 1, destroy: () => cleanup.promise })
		const token = await pool.acquire()
		const destroying = pool.destroy()
		void destroying.then(() => settled.handler())

		expect([pool.size, pool.idle, pool.active]).toEqual([1, 0, 0])
		expect(pool.emitter.destroyed).toBe(false)
		token.release()
		await Promise.resolve()
		expect(settled.count).toBe(0)
		cleanup.resolve()
		await destroying
		expect(settled.count).toBe(1)
		expect(pool.emitter.destroyed).toBe(true)
	})

	it('shares cleanup with an overlapping clear and reports its failure to both owners', async () => {
		const cleanup = createGate<void>()
		const failure = new Error('overlap failed')
		const pool = new Pool({ create: () => 1, destroy: () => cleanup.promise })
		const token = await pool.acquire()
		token.release()
		const clearing = pool.clear()
		const destroying = pool.destroy()
		cleanup.reject(failure)

		await expect(clearing).rejects.toMatchObject({ code: 'cleanup', cause: failure })
		await expect(destroying).rejects.toMatchObject({ code: 'cleanup', cause: failure })
	})

	it('aggregates distinct teardown failures and destroys the emitter last', async () => {
		const failures = [new Error('first'), new Error('second')]
		let created = 0
		const pool = new Pool({
			create: () => created++,
			destroy: (value) => {
				throw failures[value]
			},
		})
		const tokens = await Promise.all([pool.acquire(), pool.acquire()])
		const destroying = pool.destroy()

		expect(pool.emitter.destroyed).toBe(false)
		await expect(destroying).rejects.toMatchObject({
			code: 'cleanup',
			context: { failures },
		})
		expect(pool.emitter.destroyed).toBe(true)
		for (const token of tokens) token.release()
	})

	it('rejects clear after terminal destruction begins', async () => {
		const pool = new Pool({ create: () => 1 })
		const destroying = pool.destroy()

		await expect(pool.clear()).rejects.toMatchObject({ code: 'destroyed' })
		await destroying
	})
})

describe('Pool observation and pressure', () => {
	it('threads initial hooks and isolates listener failures through the emitter error handler', async () => {
		const created = createRecorder<[]>()
		const continued = createRecorder<[]>()
		const errors = createErrorRecorder()
		const pool = new Pool({
			create: () => 1,
			on: { create: created.handler },
			error: errors.handler,
		})
		pool.emitter.on('acquire', () => {
			throw new Error('observer failed')
		})
		pool.emitter.on('acquire', continued.handler)

		const token = await pool.acquire()
		expect(token.value).toBe(1)
		expect(created.count).toBe(1)
		expect(errors.calls).toHaveLength(1)
		expect(errors.calls[0]?.[1]).toBe('acquire')
		expect(continued.count).toBe(1)
	})

	it('emits each transition only after its ledger state', async () => {
		const recorded: number[][] = []
		const pool = new Pool({
			create: () => 1,
			on: {
				create: () => recorded.push([pool.size, pool.idle, pool.active]),
				acquire: () => recorded.push([pool.size, pool.idle, pool.active]),
				release: () => recorded.push([pool.size, pool.idle, pool.active]),
			},
		})
		const token = await pool.acquire()
		token.release()
		await pool.clear()

		expect(recorded).toEqual([
			[1, 0, 0],
			[1, 0, 1],
			[1, 1, 0],
		])
	})

	it('serves high contention without exceeding max or stranding waiters', async () => {
		const max = 3
		let created = 0
		let observed = 0
		const pool = new Pool({ create: () => created++, max })

		await Promise.all(
			Array.from({ length: 60 }, async () => {
				const token = await pool.acquire()
				observed = Math.max(observed, pool.active)
				await Promise.resolve()
				token.release()
			}),
		)
		expect(created).toBe(max)
		expect(observed).toBeLessThanOrEqual(max)
		expect([pool.size, pool.idle, pool.active]).toEqual([max, max, 0])
	})
})
