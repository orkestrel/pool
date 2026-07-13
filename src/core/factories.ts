import type { PoolInterface, PoolOptions } from './types.js'
import { Pool } from './Pool.js'

/**
 * Create a bounded resource pool with idle reuse and FIFO waiting — `acquire` leases a
 * resource (reusing a validated idle one, growing up to `max`, or parking until a
 * `release` frees one) and the returned token's `release` returns it for reuse.
 *
 * @remarks
 * A parked `acquire` given an `AbortSignal` rejects + de-queues itself when the signal
 * fires (no leaked waiter). `clear` destroys idle resources (leased ones keep running);
 * `destroy` destroys all and rejects waiters. The pool is lean — no warm-floor (`min`),
 * no eviction timers — and observable (§13): a typed `emitter` surfaces
 * `create` / `acquire` / `release` / `destroy`.
 *
 * @typeParam T - The pooled resource type
 * @param options - The `create` hook plus optional `destroy` / `validate` / `max`
 * @returns A working {@link PoolInterface}
 *
 * @example
 * ```ts
 * import { createPool } from '@src/core'
 *
 * const pool = createPool<Connection>({
 * 	create: () => connect(),
 * 	destroy: (connection) => connection.close(),
 * 	validate: (connection) => connection.alive,
 * 	max: 8,
 * })
 *
 * const token = await pool.acquire()
 * try {
 * 	await token.value.query('select 1')
 * } finally {
 * 	token.release()
 * }
 * ```
 */
export function createPool<T>(options: PoolOptions<T>): PoolInterface<T> {
	return new Pool(options)
}
