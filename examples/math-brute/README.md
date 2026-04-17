# math-brute

A future `wasm32-wasi` example job for the ants mesh. The intent is a CPU-bound
numeric workload — for instance, approximating π via Monte Carlo or sieving
primes within a given range — that can be split into many independent chunks
and distributed across worker nodes.

## Status

Stub only. The actual crate (`Cargo.toml`, `src/lib.rs`) is added alongside
**Milestone 2** of [PROJECT.md](../../PROJECT.md), once the worker runtime can
load `.wasm` payloads.

## Planned shape

- A small Rust crate compiled with `--target wasm32-wasi`.
- Input: a serialized range / seed payload.
- Output: a serialized partial result the orchestrator aggregates across peers.
