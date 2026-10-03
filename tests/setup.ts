import type { PoolEventMap, PoolOptions } from '@src/core'
import { Pool } from '@src/core'

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

/** Creates a real pool with recorded boundary hooks and deferred loss notifications.
 * @param options - Overrides for the pool's resource hooks and floor settings
 * @returns The pool and the observations made by its hooks
 */
export function createFloorFixture(
	options: Partial<PoolOptions<FloorResource>> = {},
): FloorFixture {
	const resources: FloorResource[] = []
	const destroyed: FloorResource[] = []
	const signals = new Map<FloorResource, AbortSignal>()
	let attempts = 0
	const pool = new Pool<FloorResource>({
		min: 1,
		restarts: 1,
		...options,
		create: async () => {
			attempts += 1
			const resource = await (options.create?.() ?? { loss: Promise.withResolvers<void>() })
			resources.push(resource)
			return resource
		},
		destroy: (resource) => {
			destroyed.push(resource)
			return options.destroy?.(resource)
		},
		watch: (resource, signal) => {
			signals.set(resource, signal)
			return options.watch?.(resource, signal) ?? resource.loss.promise
		},
	})
	return {
		pool,
		resources,
		destroyed,
		signals,
		get attempts() {
			return attempts
		},
	}
}
