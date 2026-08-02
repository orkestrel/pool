import type { PoolInterface, PoolOptions } from './types.js'
import { Pool } from './Pool.js'

/**
 * Create a resource pool with optional bounded capacity, unique ownership, and FIFO settlement.
 *
 * @remarks
 * Concurrent create and validation hooks may overlap, while acquire promises settle in
 * request order. `clear` owns its idle snapshot; `destroy` returns one stable barrier and
 * waits for every in-flight hook and cleanup before destroying the emitter last.
 *
 * @typeParam T - The pooled resource type
 * @param options - Lifecycle hooks, optional positive safe `max`, and observation hooks
 * @returns A working {@link PoolInterface}
 *
 * @example
 * ```ts
 * import { createPool } from '@orkestrel/pool'
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
