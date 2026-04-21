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
actual implementation.

## Build and run

```bash
cargo build --workspace
cargo run -p ants-cli --bin ants
```

The CLI currently prints a version banner and links against `ants-core` to
prove the workspace wires up; subcommands land with the milestones.

## stack

- **Language:** Rust
- **Networking:** `libp2p` (mDNS, Gossipsub, Request/Response)
- **Execution:** `wasmtime`
- **Async:** `tokio`
- **Serialization:** `serde` + `bincode`
- **Observability:** `tracing` + `ratatui`

## Licence

Licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) only (SPDX `AGPL-3.0-only`).
