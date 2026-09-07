# @orkestrel/pool

> A typed resource pool with optional bounded capacity, unique ownership, FIFO settlement,
> validated reuse, caller-owned cancellation, explicit cleanup failures, and a stable
> event-driven teardown barrier.

Create a pool with the `createPool` function, hand it the hooks that make, check, and tear
down one resource, and `await pool.acquire()` wherever the work needs one. Release the token
in a `finally` block, and `await pool.destroy()` when the process is done with the pool.
Environment-agnostic — no I/O, no browser or server assumptions. Part of the `@orkestrel`
line.

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
