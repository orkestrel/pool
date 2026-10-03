import type { EmitterErrorHandler, EmitterInterface } from '@orkestrel/emitter'
import type { PoolCode, PoolEventMap, PoolInterface, PoolOptions, PoolToken } from './types.js'
import { Emitter } from '@orkestrel/emitter'
import { attempt, isFunction, isNumber } from '@orkestrel/contract'
import { PoolError } from './errors.js'
import { isPoolMax, isPoolSignal } from './validators.js'

/**
 * Represents a capacity-aware resource pool whose opaque ownership records preserve FIFO
 * settlement, cancellation, exact lease release, and deterministic teardown under concurrent
 * hooks.
 *
 * @typeParam T - The pooled resource value
 *
 * @example
 * ```ts
 * import { Pool } from '@orkestrel/pool'
 *
 * const pool = new Pool({ create: () => new Uint8Array(64), max: 2 })
 * const token = await pool.acquire()
 * try {
 * 	consume(token.value)
 * } finally {
 * 	token.release()
 * }
 * await pool.destroy()
 * ```
 */
export class Pool<T> implements PoolInterface<T> {
	readonly #create: () => Promise<T> | T
	readonly #destroy: ((value: T) => Promise<void> | void) | undefined
	readonly #validate: ((value: T) => Promise<boolean> | boolean) | undefined
	readonly #max: number | undefined
	readonly #min: number | undefined
	readonly #restarts: number | undefined
	readonly #watch: PoolOptions<T>['watch']
	readonly #error: EmitterErrorHandler | undefined
	readonly #watches = new Map<object, AbortController>()
	readonly #used = new WeakSet<object>()
	readonly #settled = new WeakSet<object>()
	readonly #survivors = new Set<object>()
	readonly #emitter: Emitter<PoolEventMap>
	readonly #resources = new Map<object, T>()
	readonly #available: object[] = []
	readonly #validating = new Set<object>()
	readonly #leased = new Set<object>()
	readonly #destroying = new Map<object, Promise<void>>()
	readonly #waiters: Array<PromiseWithResolvers<PoolToken<T>>> = []
	readonly #assigned = new Set<PromiseWithResolvers<PoolToken<T>>>()
	readonly #reservations = new Set<PromiseWithResolvers<PoolToken<T>>>()
	readonly #signals = new Map<
		PromiseWithResolvers<PoolToken<T>>,
		{ readonly signal: AbortSignal; readonly listener: () => void }
	>()
	readonly #ready = new Map<
		PromiseWithResolvers<PoolToken<T>>,
		| { readonly success: true; readonly record: object; readonly token: PoolToken<T> }
		| { readonly success: false; readonly error: unknown }
	>()
	readonly #operations = new Set<Promise<void>>()
	readonly #owned = new Set<Promise<void>>()
	readonly #failures: unknown[] = []
	#ending: PromiseWithResolvers<void> | undefined
	#pumping = false
	#repump = false
	#started = false
	#starting: PromiseWithResolvers<void> | undefined
	#refilling = false
	#strikes = 0
	#owed = 0
	#cause: unknown

	/**
	 * Constructs a pool and synchronously validates its capacity contract.
	 *
	 * @param options - Resource hooks, observation hooks, capacity, and optional bounded warm floor
	 * @throws {@link PoolError} Thrown with `code: 'invalid'` when `min` or `max` is not a positive
	 * safe integer, their explicit values differ, `restarts` is absent with `min`, present without
	 * `min`, or not a non-negative safe integer, or `watch` is not callable.
	 */
	constructor(options: PoolOptions<T>) {
		const max = options.max
		const min = options.min
		const restarts = options.restarts
		const watch = options.watch
		if (max !== undefined && !isPoolMax(max)) {
			throw new PoolError({ code: 'invalid', context: { value: max } })
		}
		if (min !== undefined && (!isPoolMax(min) || (max !== undefined && min !== max))) {
			throw new PoolError({ code: 'invalid', context: { value: min } })
		}
		if (
			(min !== undefined && restarts === undefined) ||
			(restarts !== undefined &&
				(min === undefined ||
					!isNumber(restarts) ||
					!Number.isSafeInteger(restarts) ||
					restarts < 0))
		) {
			throw new PoolError({ code: 'invalid', context: { value: restarts } })
		}
		if (watch !== undefined && !isFunction(watch)) {
			throw new PoolError({ code: 'invalid', context: { value: watch } })
		}
		const on = options.on
		const error = options.error
		this.#create = options.create
		this.#destroy = options.destroy
		this.#validate = options.validate
		this.#max = max ?? min
		this.#min = min
		this.#restarts = restarts
		this.#watch = watch
		this.#error = error
		this.#emitter = new Emitter({
			...(on === undefined ? {} : { on }),
			...(error === undefined ? {} : { error }),
		})
	}

	/** Holds the typed synchronous lifecycle observation surface. */
	get emitter(): EmitterInterface<PoolEventMap> {
		return this.#emitter
	}

	/** Counts all owned records, including records validating, destroying, or retained after failed cleanup. */
	get size(): number {
		return this.#resources.size
	}

	/** Counts the records immediately available without validation work. */
	get idle(): number {
		return this.#available.length
	}

	/** Counts the records represented by unsettled released-once lease tokens. */
	get active(): number {
		return this.#leased.size
	}

	/**
	 * Fills the warm floor and restarts a spent refill bound; without a floor, resolves immediately.
	 *
	 * @returns A promise that resolves when the floor owns `min` live records
	 * @throws {@link PoolError} Thrown as a rejection with `code: 'create'` and the last cause
	 * when the bound is spent, `code: 'cleanup'` when retained failed cleanup prevents filling,
	 * or `code: 'destroyed'` when teardown begins.
	 */
	start(): Promise<void> {
		if (this.#ending !== undefined) return Promise.reject(new PoolError({ code: 'destroyed' }))
		if (this.#min === undefined) return Promise.resolve()
		if (this.#starting !== undefined) return this.#starting.promise
		const starting = Promise.withResolvers<void>()
		this.#starting = starting
		this.#started = true
		if (this.#spent()) this.#strikes = 0
		this.#pump()
		return starting.promise
	}

	/**
	 * Queues the caller in FIFO order, validates an idle record or waits for a floor refill, and
	 * creates on demand only without a floor.
	 *
	 * @param signal - Optional native cancellation signal
	 * @returns A promise for the unique resource lease
	 * @throws {@link PoolError} Thrown when `signal` is present and is not a native `AbortSignal`,
	 * with `code: 'invalid'`. This throw is synchronous rather than a rejected promise, so a caller
	 * that handles failures with `.catch()` alone misses it.
	 * @throws {@link PoolError} Thrown as a rejection when `destroy()` has already begun, with
	 * `code: 'destroyed'`; when the create hook fails or the floor is spent without an idle record,
	 * with `code: 'create'` and the last cause; and when an invalid record's cleanup fails,
	 * with `code: 'cleanup'`. A
	 * `signal` that aborts rejects with the caller's exact `signal.reason` instead.
	 */
	acquire(signal?: AbortSignal): Promise<PoolToken<T>> {
		if (signal !== undefined && !isPoolSignal(signal)) {
			throw new PoolError({ code: 'invalid', context: { value: signal } })
		}
		if (this.#ending !== undefined) return Promise.reject(new PoolError({ code: 'destroyed' }))
		if (signal !== undefined) {
			const state = this.#state(signal)
			if (state[0]) return Promise.reject(state[1])
		}

		const waiter = Promise.withResolvers<PoolToken<T>>()
		this.#waiters.push(waiter)
		if (signal !== undefined) {
			const listener = this.#createAbort(waiter, signal)
			AbortSignal.prototype.addEventListener.call(signal, 'abort', listener, { once: true })
			this.#signals.set(waiter, { signal, listener })
			const state = this.#state(signal)
			if (state[0]) this.#abort(waiter, state[1])
		}
		this.#pump()
		return waiter.promise
	}

	/**
	 * Destroys the records that are idle at this call's synchronous snapshot and restores an active floor.
	 *
	 * @returns A promise that settles after every snapshot cleanup attempt
	 * @throws {@link PoolError} Thrown when `destroy()` has already begun, with `code: 'destroyed'`.
	 * @throws {@link PoolError} Thrown when a claimed record's destroy hook fails, with
	 * `code: 'cleanup'` and every distinct failure in `context.failures`. Each arrives as a rejected
	 * promise rather than a synchronous throw.
	 */
	clear(): Promise<void> {
		if (this.#ending !== undefined) return Promise.reject(new PoolError({ code: 'destroyed' }))
		const records = this.#available.splice(0)
		const cleanups: Array<Promise<void>> = []
		for (const record of records) {
			const cleanup = this.#dispose(record)
			cleanups.push(cleanup)
			void cleanup.then(
				() => this.#pump(),
				() => this.#pump(),
			)
		}
		return this.#settleClear(cleanups)
	}

	/**
	 * Tears down the pool permanently and returns its stable completion barrier.
	 *
	 * @returns The exact promise shared by every destroy call
	 * @throws {@link PoolError} Thrown when a destroy hook failure was retained or occurred during teardown, with
	 * `code: 'cleanup'` and every distinct failure in `context.failures`. The barrier rejects; it
	 * never throws synchronously, and a repeat call receives the same rejected promise.
	 */
	destroy(): Promise<void> {
		if (this.#ending !== undefined) return this.#ending.promise

		const ending = Promise.withResolvers<void>()
		this.#ending = ending
		this.#starting?.reject(new PoolError({ code: 'destroyed' }))
		this.#starting = undefined
		const waiters = this.#waiters.splice(0)
		for (const waiter of waiters) {
			this.#detach(waiter)
			waiter.reject(new PoolError({ code: 'destroyed' }))
		}
		this.#ready.clear()
		this.#assigned.clear()
		this.#available.splice(0)
		for (const cleanup of this.#destroying.values()) this.#own(cleanup)
		for (const record of this.#resources.keys()) {
			if (!this.#validating.has(record)) this.#own(this.#dispose(record))
		}
		this.#finish()
		return ending.promise
	}

	#createAbort(waiter: PromiseWithResolvers<PoolToken<T>>, signal: AbortSignal): () => void {
		return (): void => {
			let reason: unknown
			try {
				reason = this.#state(signal)[1]
			} catch (error: unknown) {
				reason = error
			}
			this.#abort(waiter, reason)
		}
	}

	#state(signal: AbortSignal): readonly [aborted: boolean, reason: unknown] {
		const aborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')?.get
		const reason = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'reason')?.get
		if (aborted === undefined || reason === undefined) {
			throw new PoolError({ code: 'invalid', context: { value: signal } })
		}
		try {
			const stopped = Reflect.apply(aborted, signal, []) === true
			return [stopped, stopped ? Reflect.apply(reason, signal, []) : undefined]
		} catch (error: unknown) {
			throw new PoolError({ code: 'invalid', cause: error, context: { value: signal } })
		}
	}

	#token(record: object, value: T): PoolToken<T> {
		const lease = {}
		return {
			value,
			release: this.#createRelease(record, lease),
			destroy: this.#createDestroy(record, lease),
		}
	}

	#createRelease(record: object, lease: object): () => void {
		return (): void => {
			if (this.#settled.has(lease)) return
			this.#settled.add(lease)
			this.#release(record)
		}
	}

	#createDestroy(record: object, lease: object): () => Promise<void> {
		return (): Promise<void> => {
			if (this.#settled.has(lease)) return Promise.resolve()
			this.#settled.add(lease)
			return this.#lose(record).catch((error: unknown) => {
				throw new PoolError({ code: 'cleanup', cause: error })
			})
		}
	}

	#abort(waiter: PromiseWithResolvers<PoolToken<T>>, reason: unknown): void {
		const index = this.#waiters.indexOf(waiter)
		if (index < 0) return
		this.#waiters.splice(index, 1)
		this.#detach(waiter)
		const ready = this.#ready.get(waiter)
		this.#ready.delete(waiter)
		this.#assigned.delete(waiter)
		if (ready?.success === true) {
			this.#recycle(ready.record)
		}
		waiter.reject(reason)
		this.#commit()
		this.#pump()
	}

	#detach(waiter: PromiseWithResolvers<PoolToken<T>>): void {
		const entry = this.#signals.get(waiter)
		if (entry === undefined) return
		AbortSignal.prototype.removeEventListener.call(entry.signal, 'abort', entry.listener)
		this.#signals.delete(waiter)
	}

	#pump(): void {
		if (this.#ending !== undefined) return
		if (this.#pumping) {
			this.#repump = true
			return
		}

		this.#pumping = true
		do {
			this.#repump = false
			for (const waiter of this.#waiters) {
				if (this.#assigned.has(waiter) || this.#ready.has(waiter)) continue
				const record = this.#available.shift()
				if (record !== undefined) {
					this.#assigned.add(waiter)
					this.#validating.add(record)
					this.#startValidation(waiter, record)
					continue
				}
				if (this.#min !== undefined) {
					if (
						this.#spent() &&
						!this.#refilling &&
						this.#destroying.size === 0 &&
						this.#owed === 0
					) {
						this.#ready.set(waiter, {
							success: false,
							error: new PoolError({ code: 'create', cause: this.#cause }),
						})
						continue
					}
					break
				}
				if (this.#max === undefined || this.#resources.size + this.#reservations.size < this.#max) {
					this.#assigned.add(waiter)
					this.#reservations.add(waiter)
					this.#startCreate(waiter)
					continue
				}
				break
			}
			this.#commit()
		} while (this.#repump)
		this.#pumping = false
		this.#fill()
	}

	#spent(): boolean {
		return this.#restarts !== undefined && this.#strikes > this.#restarts
	}

	#fill(): void {
		if (this.#ending !== undefined || !this.#started || this.#min === undefined) return
		if (this.#refilling) return
		const live = this.#resources.size - this.#destroying.size - this.#survivors.size
		if (live >= this.#min) {
			this.#starting?.resolve()
			this.#starting = undefined
		}
		if (this.#resources.size < this.#min && (!this.#spent() || this.#owed > 0)) {
			if (this.#owed > 0) this.#owed -= 1
			this.#refilling = true
			const operation = Promise.resolve().then(() => this.#refill())
			this.#operations.add(operation)
			void operation.then(() => {
				this.#refilling = false
				this.#completeOperation(operation)
			})
			return
		}
		if (this.#destroying.size > 0) return
		if (this.#spent() || this.#survivors.size > 0) {
			this.#starting?.reject(
				this.#spent()
					? new PoolError({ code: 'create', cause: this.#cause })
					: this.#cleanupError(this.#failures),
			)
			this.#starting = undefined
		}
	}

	async #refill(): Promise<void> {
		let value: T
		try {
			value = await this.#create()
		} catch (error: unknown) {
			this.#strikes += 1
			this.#cause = error
			return
		}
		const record = this.#insert(value)
		this.#recycle(record)
	}

	#insert(value: T): object {
		const record = {}
		this.#resources.set(record, value)
		if (this.#watch !== undefined) {
			const controller = new AbortController()
			this.#watches.set(record, controller)
			const watching = attempt(() => this.#watch?.(value, controller.signal))
			const settled = watching.success
				? Promise.resolve(watching.value)
				: Promise.reject(watching.error)
			void settled.then(
				() => this.#own(this.#lose(record)),
				(error: unknown) => {
					this.#own(this.#lose(record, error))
					attempt(() => this.#error?.(error, 'watch'))
				},
			)
		}
		this.#emitter.emit('create')
		return record
	}

	async #lose(record: object, cause?: unknown): Promise<void> {
		if (
			this.#ending !== undefined ||
			!this.#resources.has(record) ||
			this.#destroying.has(record) ||
			this.#survivors.has(record)
		)
			return
		const leased = this.#leased.has(record)
		this.#strike(record, cause)
		try {
			await this.#dispose(record)
		} catch (error: unknown) {
			this.#pump()
			throw error
		}
		if (leased && this.#min !== undefined) this.#owed += 1
		this.#pump()
	}

	#strike(record: object, cause?: unknown): void {
		if (this.#min !== undefined && !this.#used.has(record)) {
			this.#strikes += 1
			this.#cause = cause
		}
	}

	#startCreate(waiter: PromiseWithResolvers<PoolToken<T>>): void {
		const operation = Promise.resolve().then(() => this.#createResource(waiter))
		this.#operations.add(operation)
		void operation.then(
			() => this.#completeOperation(operation),
			(error: unknown) => this.#failOperation(operation, waiter, error, 'create'),
		)
	}

	#startValidation(waiter: PromiseWithResolvers<PoolToken<T>>, record: object): void {
		for (const [owned, value] of this.#resources) {
			if (owned !== record) continue
			if (this.#validate === undefined) {
				this.#validating.delete(record)
				this.#prepare(waiter, record, value)
				return
			}
			const operation = Promise.resolve().then(() => this.#validateResource(waiter, record, value))
			this.#operations.add(operation)
			void operation.then(
				() => this.#completeOperation(operation),
				(error: unknown) => this.#failOperation(operation, waiter, error, 'invalid'),
			)
			return
		}
		this.#validating.delete(record)
		this.#assigned.delete(waiter)
		this.#repump = true
	}

	async #createResource(waiter: PromiseWithResolvers<PoolToken<T>>): Promise<void> {
		let value: T
		try {
			value = await this.#create()
		} catch (error: unknown) {
			this.#reservations.delete(waiter)
			if (this.#waiters.includes(waiter)) {
				this.#ready.set(waiter, {
					success: false,
					error: new PoolError({ code: 'create', cause: error }),
				})
			} else {
				this.#assigned.delete(waiter)
			}
			this.#commit()
			return
		}

		this.#reservations.delete(waiter)
		const record = this.#insert(value)
		if (this.#ending !== undefined) {
			this.#assigned.delete(waiter)
			try {
				await this.#dispose(record)
			} catch {}
			return
		}
		if (!this.#waiters.includes(waiter)) {
			this.#assigned.delete(waiter)
			this.#recycle(record)
			return
		}
		this.#ready.set(waiter, {
			success: true,
			record,
			token: this.#token(record, value),
		})
		this.#commit()
	}

	async #validateResource(
		waiter: PromiseWithResolvers<PoolToken<T>>,
		record: object,
		value: T,
	): Promise<void> {
		let valid = false
		try {
			valid = (await this.#validate?.(value)) === true
		} catch {}
		this.#validating.delete(record)
		if (
			!this.#resources.has(record) ||
			this.#destroying.has(record) ||
			this.#survivors.has(record)
		) {
			this.#assigned.delete(waiter)
			this.#pump()
			return
		}

		if (this.#ending !== undefined) {
			this.#assigned.delete(waiter)
			try {
				await this.#dispose(record)
			} catch {}
			return
		}
		if (!valid) this.#strike(record)
		if (!this.#waiters.includes(waiter)) {
			this.#assigned.delete(waiter)
			if (valid) this.#recycle(record)
			else {
				try {
					await this.#dispose(record)
				} catch (error: unknown) {
					this.#record(error)
				}
			}
			return
		}
		if (valid) {
			this.#prepare(waiter, record, value)
			return
		}

		try {
			await this.#dispose(record)
		} catch (error: unknown) {
			this.#assigned.delete(waiter)
			if (this.#waiters.includes(waiter)) {
				this.#ready.set(waiter, {
					success: false,
					error: new PoolError({ code: 'cleanup', cause: error }),
				})
				this.#commit()
				return
			}
			this.#record(error)
			this.#pump()
			return
		}
		this.#assigned.delete(waiter)
		this.#pump()
	}

	#prepare(waiter: PromiseWithResolvers<PoolToken<T>>, record: object, value: T): void {
		if (!this.#waiters.includes(waiter) || this.#ending !== undefined) {
			this.#assigned.delete(waiter)
			this.#recycle(record)
			return
		}
		this.#ready.set(waiter, {
			success: true,
			record,
			token: this.#token(record, value),
		})
		this.#commit()
	}

	#commit(): void {
		while (this.#ending === undefined) {
			const waiter = this.#waiters[0]
			if (waiter === undefined) return
			const result = this.#ready.get(waiter)
			if (result === undefined) return
			if (
				result.success &&
				(!this.#resources.has(result.record) ||
					this.#destroying.has(result.record) ||
					this.#survivors.has(result.record))
			) {
				this.#ready.delete(waiter)
				this.#assigned.delete(waiter)
				this.#repump = true
				return
			}
			this.#waiters.shift()
			this.#repump = true
			this.#ready.delete(waiter)
			this.#assigned.delete(waiter)
			this.#detach(waiter)
			if (!result.success) {
				waiter.reject(result.error)
				continue
			}
			this.#leased.add(result.record)
			this.#used.add(result.record)
			this.#strikes = 0
			waiter.resolve(result.token)
			this.#emitter.emit('acquire')
		}
	}

	#completeOperation(operation: Promise<void>): void {
		this.#operations.delete(operation)
		this.#commit()
		this.#pump()
		this.#finish()
	}

	#failOperation(
		operation: Promise<void>,
		waiter: PromiseWithResolvers<PoolToken<T>>,
		error: unknown,
		code: PoolCode,
	): void {
		this.#operations.delete(operation)
		this.#reservations.delete(waiter)
		this.#assigned.delete(waiter)
		if (this.#waiters.includes(waiter)) {
			this.#ready.set(waiter, { success: false, error: new PoolError({ code, cause: error }) })
		}
		this.#commit()
		this.#pump()
		this.#finish()
	}

	#release(record: object): void {
		if (!this.#leased.delete(record)) return
		this.#recycle(record)
	}

	#recycle(record: object): void {
		if (!this.#resources.has(record) || this.#destroying.has(record) || this.#survivors.has(record))
			return
		if (this.#ending !== undefined) {
			this.#own(this.#dispose(record))
			return
		}
		this.#available.push(record)
		this.#pump()
		if (this.#available.includes(record)) this.#emitter.emit('release')
	}

	#dispose(record: object): Promise<void> {
		if (this.#survivors.has(record)) return Promise.resolve()
		const existing = this.#destroying.get(record)
		if (existing !== undefined) return existing
		for (const [owned, value] of this.#resources) {
			if (owned !== record) continue
			const cleanup = Promise.withResolvers<void>()
			this.#destroying.set(record, cleanup.promise)
			const index = this.#available.indexOf(record)
			if (index >= 0) this.#available.splice(index, 1)
			this.#validating.delete(record)
			this.#leased.delete(record)
			this.#watches.get(record)?.abort()
			this.#watches.delete(record)
			if (this.#ending !== undefined) this.#own(cleanup.promise)
			void this.#clean(record, value, cleanup)
			return cleanup.promise
		}
		return Promise.resolve()
	}

	async #clean(record: object, value: T, cleanup: PromiseWithResolvers<void>): Promise<void> {
		let disposal: Promise<void>
		try {
			disposal = Promise.resolve(this.#destroy?.(value))
		} catch (error: unknown) {
			disposal = Promise.reject(error)
		}
		let failure: unknown
		let failed = false
		try {
			await disposal
		} catch (error: unknown) {
			failure = error
			failed = true
		}
		if (failed && this.#min !== undefined) {
			this.#survivors.add(record)
			this.#record(failure)
		} else this.#resources.delete(record)
		if (failed) cleanup.reject(failure)
		else cleanup.resolve()
		// Cleanup owners must bind outcomes before destroy observers can reenter.
		await Promise.resolve()
		this.#emitter.emit('destroy')
		this.#destroying.delete(record)
		if (this.#min !== undefined) this.#pump()
		this.#finish()
	}

	async #settleClear(cleanups: ReadonlyArray<Promise<void>>): Promise<void> {
		const settled = await Promise.allSettled(cleanups)
		const failures: unknown[] = []
		for (const result of settled) {
			if (
				result.status === 'rejected' &&
				!failures.some((failure) => Object.is(failure, result.reason))
			) {
				failures.push(result.reason)
			}
		}
		if (failures.length > 0) throw this.#cleanupError(failures)
	}

	#own(cleanup: Promise<void>): void {
		if (this.#owned.has(cleanup)) return
		this.#owned.add(cleanup)
		void cleanup.then(
			() => this.#completeCleanup(cleanup),
			(error: unknown) => this.#failCleanup(cleanup, error),
		)
	}

	#completeCleanup(cleanup: Promise<void>): void {
		this.#owned.delete(cleanup)
		this.#finish()
	}

	#failCleanup(cleanup: Promise<void>, error: unknown): void {
		this.#owned.delete(cleanup)
		this.#record(error)
		this.#finish()
	}

	#record(error: unknown): void {
		if (!this.#failures.some((failure) => Object.is(failure, error))) this.#failures.push(error)
	}

	#cleanupError(failures: readonly unknown[]): PoolError {
		return new PoolError({
			code: 'cleanup',
			...(failures[0] === undefined ? {} : { cause: failures[0] }),
			context: { failures: [...failures] },
		})
	}

	#finish(): void {
		const ending = this.#ending
		if (ending === undefined || this.#operations.size > 0 || this.#owned.size > 0) return
		let claimed = false
		for (const record of this.#resources.keys()) {
			if (
				this.#validating.has(record) ||
				this.#destroying.has(record) ||
				this.#survivors.has(record)
			)
				continue
			claimed = true
			this.#own(this.#dispose(record))
		}
		if (claimed || this.#resources.size > this.#survivors.size || this.#destroying.size > 0) return
		this.#emitter.destroy()
		if (this.#failures.length > 0) ending.reject(this.#cleanupError(this.#failures))
		else ending.resolve()
	}
}
