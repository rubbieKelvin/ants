# ants

A WASM-based distributed mesh scheduler in Rust. ants ships pre-compiled
WebAssembly modules plus data payloads across a peer-to-peer network of
heterogenous devices (laptops, desktops, Raspberry Pis) and executes them in
parallel, MapReduce-style. Any node can submit a job; that node becomes the
orchestrator for the job's lifetime, managing the task queue, tracking worker
heartbeats, and recovering from failures. Idle workers steal pending work from
busy peers to keep total job time down.

## Status

Early scaffold. Core types and transports are stubs; milestones drive the
actual implementation. See [PROJECT.md](PROJECT.md) for the full specification
and the milestone-by-milestone execution plan.

## Workspace layout

```text
ants/
├── Cargo.toml                  # workspace definition
├── crates/
│   ├── ants-core/              # shared domain types
│   ├── ants-network/           # libp2p discovery + messaging
│   ├── ants-worker/            # wasmtime sandbox runtime
│   ├── ants-observability/     # tracing + ratatui dashboard
│   ├── ants-orchestrator/      # task queue, work-stealing, recovery
│   └── ants-cli/               # `ants` binary (entrypoint)
└── examples/
    ├── math-brute/             # WASM job: Pi / primes
    └── raytracer/              # WASM job: tiled renderer
```

## Build and run

Requires a recent stable Rust toolchain (edition 2024; Rust 1.85+).

```bash
cargo build --workspace
cargo run -p ants-cli --bin ants
```

The CLI currently prints a version banner and links against `ants-core` to
prove the workspace wires up; subcommands land with the milestones.

## Technology

- **Language:** Rust
- **Networking:** `libp2p` (mDNS, Gossipsub, Request/Response)
- **Execution:** `wasmtime`
- **Async:** `tokio`
- **Serialization:** `serde` + `bincode`
- **Observability:** `tracing` + `ratatui`

## Documentation

- [PROJECT.md](PROJECT.md) — full spec, requirements, and milestones.
- [AGENTS.md](AGENTS.md) — conventions for contributors and AI agents working
  in this repository.

## Licence

Licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) only (SPDX `AGPL-3.0-only`). See [LICENCE.md](LICENCE.md) for the full text.
