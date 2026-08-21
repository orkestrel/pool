import type { PoolEventMap } from '@src/core'

// ── Environment-agnostic base setup (AGENTS §16.1) ────────────────────────────
//
// Loaded first by every test project (`vite.config.ts` `setupFiles[0]`). Holds ONLY
// helpers with no `node:*` / DOM dependency, so it is safe for `src:core` alike.
//
// The fleet-wide helpers live in `@orkestrel/test`: `createRecorders` replaces the
// local emitter recorder bundle and `createResourceFactory` the local Pool resource
// fixture. What remains here is this package's own event vocabulary, which names the
// second type argument `createRecorders` cannot infer from an emitter.

/** One observable lifecycle event name of a {@link PoolEventMap}. */
export type PoolEvent = keyof PoolEventMap

/** Every Pool lifecycle event, so a recorder bundle covers the whole event map. */
export const POOL_EVENTS: readonly PoolEvent[] = Object.freeze([
	'create',
	'acquire',
	'release',
	'destroy',
])
