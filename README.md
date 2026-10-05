# @orkestrel/pool

> A typed resource pool with optional bounded capacity, a warm floor, bounded loss recovery,
> exclusive leases by default, FIFO settlement, validated idle reuse, caller-owned cancellation, and explicit cleanup.

Create a pool with the `createPool` function, hand it the hooks that make, check, and tear
down one resource, and `await pool.acquire()` wherever the work needs one. Release the token
in a `finally` block, and `await pool.destroy()` when the process is done with the pool.
Environment-agnostic — no I/O, no browser or server assumptions. Part of the `@orkestrel`
line.

For eager resources, set `min` and the required `restarts` bound on failed refills and losses
of idle records between strike resets, then call `start()`.
The `watch(value, signal)` hook reports loss; a holder can also call its token's `destroy()`.
The pool disposes lost records before replacing them and keeps a record whose cleanup fails
counted against `max`.

## Install

```sh
npm install @orkestrel/pool
```

## Requirements

- Node.js >= 22.12.0
- ESM and CommonJS builds

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
and usage patterns — see [`guides/pool.md`](guides/pool.md).

## Package

Published as a single typed entry point per the `exports` field in
`package.json`.

## License

MIT © [Orkestrel](https://github.com/orkestrel) — see [LICENSE](./LICENSE).
