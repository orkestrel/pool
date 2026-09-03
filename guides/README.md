# Guides

A dual-axis index into this repository's guides — by concept, and by directory.

## By concept

| Concept | Spec                 | Source                    | Tests                                 |
| ------- | -------------------- | ------------------------- | ------------------------------------- |
| Pool    | [`pool.md`](pool.md) | [`src/core`](../src/core) | [`tests/src/core`](../tests/src/core) |

## By directory

| Directory  | Guide                |
| ---------- | -------------------- |
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

[`probe.md`](probe.md) and [`test.md`](test.md) are byte-identical mirrors of the
guides for `@orkestrel/probe` and `@orkestrel/test` — the devDependencies behind
this repo's probe project and test helpers. They document **those packages'**
surfaces, not anything sourced in this repo; they are kept here for the same
reason.

## See also

- [`AGENTS.md`](../AGENTS.md) — the repository's coding and documentation rules.
