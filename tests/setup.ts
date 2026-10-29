import type { PoolEventMap, PoolOptions, PoolToken } from '@src/core'
import { Pool, PoolError } from '@src/core'

// ── Environment-agnostic base setup ───────────────────────────────────────────
//
// Loaded first by every test project (`vite.config.ts` `setupFiles[0]`). Holds ONLY
// helpers with no `node:*` / DOM dependency, so it is safe for `src:core` alike.
//
// `@orkestrel/test` supplies this package's shared recorder helpers. What remains here
// is this package's own event vocabulary, which names the second type argument
// `createRecorders` cannot infer from an emitter.

/** Names one observable lifecycle event of a {@link PoolEventMap}. */
export type PoolEvent = keyof PoolEventMap

/** Lists every Pool lifecycle event, so a recorder bundle covers the whole event map. */
export const POOL_EVENTS: readonly PoolEvent[] = Object.freeze([
	'create',
	'acquire',
	'release',
	'destroy',
])

/** Holds an inert resource and its externally controlled loss notification. */
export interface FloorResource {
	readonly loss: PromiseWithResolvers<void>
}

/** Exposes a real floor pool and readonly observations of its boundary hooks. */
export interface FloorFixture {
	readonly pool: Pool<FloorResource>
	readonly resources: readonly FloorResource[]
	readonly destroyed: readonly FloorResource[]
	readonly signals: ReadonlyMap<FloorResource, AbortSignal>
	readonly attempts: number
}

/** Owns the hook state of one real floor pool. */
class FloorObserver implements FloorFixture {
	readonly pool: Pool<FloorResource>
	readonly #resources: FloorResource[] = []
	readonly #destroyed: FloorResource[] = []
	readonly #signals = new Map<FloorResource, AbortSignal>()
	#attempts = 0

	constructor(options: Partial<PoolOptions<FloorResource>>) {
		this.pool = new Pool<FloorResource>({
			min: 1,
			restarts: 1,
			...options,
			create: async () => {
				this.#attempts += 1
				const resource = await (options.create?.() ?? { loss: Promise.withResolvers<void>() })
				this.#resources.push(resource)
				return resource
			},
			destroy: (resource) => {
				this.#destroyed.push(resource)
				return options.destroy?.(resource)
			},
			watch: (resource, signal) => {
				this.#signals.set(resource, signal)
				return options.watch?.(resource, signal) ?? resource.loss.promise
			},
		})
	}

	get resources(): readonly FloorResource[] {
		return this.#resources
	}

	get destroyed(): readonly FloorResource[] {
		return this.#destroyed
	}

	get signals(): ReadonlyMap<FloorResource, AbortSignal> {
		return this.#signals
	}

	get attempts(): number {
		return this.#attempts
	}
}

/** Creates a real pool with recorded boundary hooks and deferred loss notifications.
 * @param options - Overrides for the pool's resource hooks and floor settings
 * @returns The pool and the observations made by its hooks
 */
export function createFloorFixture(
	options: Partial<PoolOptions<FloorResource>> = {},
): FloorFixture {
	return new FloorObserver(options)
}

/** Provides a derived pool error for recognition tests. */
export class SpecializedPoolError extends PoolError {}

/** Tracks sequential hook invocations. */
export class HookSequence {
	#count = 0

	get count(): number {
		return this.#count
	}

	next(): number {
		const value = this.#count
		this.#count += 1
		return value
	}

	advance(): number {
		this.#count += 1
		return this.#count
	}
}

/** Holds a condition that a test changes between hook invocations. */
export class HookCondition {
	#value: boolean

	constructor(value: boolean) {
		this.#value = value
	}

	get value(): boolean {
		return this.#value
	}

	set(value: boolean): void {
		this.#value = value
	}
}

/** Acquires and consumes a pool token from reentrant callbacks. */
export class ReentrantClient {
	#pending: Promise<PoolToken<number>> | undefined
	#completion: Promise<void> | undefined

	get pending(): Promise<PoolToken<number>> | undefined {
		return this.#pending
	}

	get completion(): Promise<void> | undefined {
		return this.#completion
	}

	acquire(pool: Pool<number>): void {
		if (this.#pending !== undefined) throw new Error('reentrant acquisition already started')
		this.#pending = pool.acquire()
		void this.#pending.catch(() => {})
	}

	consume(observe: (value: number) => void): Promise<void> {
		if (this.#completion !== undefined) throw new Error('reentrant consumption already started')
		if (this.#pending === undefined) throw new Error('destroy listener did not reenter acquisition')
		this.#completion = this.#pending.then((token) => {
			try {
				observe(token.value)
			} finally {
				token.release()
			}
		})
		return this.#completion
	}
}
