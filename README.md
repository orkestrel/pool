# @orkestrel/pool

A bounded, typed **resource pool**: idle reuse + FIFO waiting. `acquire` leases
a resource — reusing a validated idle one, growing up to `max`, or parking on
a FIFO waiter list until a `release` frees one — and the returned token's
`release()` returns it for reuse (or hands it straight to the next waiter).
The FIFO handoff is validated, so a resource that goes bad while leased is
never handed to the next lessee, and a parked `acquire` given an `AbortSignal`
rejects and de-queues itself when the signal fires — no leaked waiter. The
pool is observable (a typed `emitter` surfaces `create` / `acquire` /
`release` / `destroy`) and deliberately de-bloated — no warm-floor, no
eviction timers. Environment-agnostic — no I/O, no browser or server
assumptions. Part of the `@orkestrel` line.

## Install

```sh
npm install @orkestrel/pool
```

## Requirements

- Node.js >= 24
- ESM-only (no CommonJS build)

## Usage

```ts
import { createPool } from '@orkestrel/pool'

const pool = createPool<Connection>({
	create: () => connect(),
	destroy: (connection) => connection.close(),
	validate: (connection) => connection.alive,
	max: 8,
})

const token = await pool.acquire()
try {
	await token.value.query('select 1')
} finally {
	token.release()
}
```

## Guide

For the full surface — the `Pool` engine, options, the observable `emitter`,
and usage patterns — see [`guides/src/pool.md`](guides/src/pool.md).

## Package

Published as a single typed entry point per the `exports` field in
`package.json`.

## License

MIT © [Orkestrel](https://github.com/orkestrel) — see [LICENSE](./LICENSE).
