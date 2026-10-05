import type { PoolInterface, PoolOptions } from './types.js'
import { Pool } from './Pool.js'

/**
 * Creates a distinct {@link PoolInterface} from resource lifecycle hooks, with optional bounded
 * capacity, exclusive leases by default, and FIFO settlement.
 *
 * @remarks
 * Concurrent create and validation hooks may overlap, while acquire promises settle in
 * request order. `clear` owns its idle snapshot; `destroy` returns one stable barrier and
 * waits for every in-flight create, validation, and destroy attempt before destroying the
 * emitter last.
 * Set `min` with its required `restarts` bound and call `start()` to warm resources and
 * refill losses independently of queued acquires. `watch` observes loss and its signal
 * aborts at disposal; a token can declare loss through `destroy()`.
 *
 * @typeParam T - The pooled resource type
 * @param options - Lifecycle hooks, capacity, optional bounded warm floor, and observation hooks
 * @returns A working {@link PoolInterface}
 * @throws {@link PoolError} Thrown with `code: 'invalid'` when capacity, restart bounds, or
 * the watch hook are invalid. Construction validates them synchronously, before the pool exists.
 *
 * @example Create a pool
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
