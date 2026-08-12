# Guides

A dual-axis index into this repository's guides — by concept, and by directory (AGENTS §22).

## By concept

| Concept | Spec                         | Source                    | Tests                                 |
| ------- | ---------------------------- | ------------------------- | ------------------------------------- |
| Pool    | [`pool.md`](pool.md) | [`src/core`](../src/core) | [`tests/src/core`](../tests/src/core) |

## By directory

| Directory  | Guide                        |
| ---------- | ---------------------------- |
| `src/core` | [`pool.md`](pool.md) |

## Dependency reference

[`emitter.md`](emitter.md) is a byte-identical mirror of the guide for
`@orkestrel/emitter` — a runtime dependency, and the observable primitive `Pool`
composes as its `emitter`. It documents **that package's** surface (the typed
push-observation `Emitter`), not anything sourced in this repo; it is kept here
so a reader of this package can see the primitive it is built from without
leaving this guide set.

[`guide.md`](guide.md) is a byte-identical mirror of the guide for
`@orkestrel/guide` — the devDependency powering this repo's guides-parity test
suite (`tests/guides.test.ts`). It documents **that package's**
surface (`Guide` / `Source`, the manifest and comparison helpers), not anything
sourced in this repo; it is kept here so a reader of the parity suite can see
the primitives it is built from without leaving this guide set.

## See also

- [`AGENTS.md`](../AGENTS.md) — the rules; §22 documentation-as-contracts.
