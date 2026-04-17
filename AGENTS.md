# AGENTS.md

Conventions for contributors and AI agents working inside the ants workspace.
Read this file before editing; it is the quickest way to stay aligned with the
project's intent.

## Orientation

- [README.md](README.md) — user-facing overview and build instructions.
- [PROJECT.md](PROJECT.md) — full specification and the authoritative
  milestone plan. Treat the milestones as the ordered contract for what lands
  when.

## Workspace map

| Crate | Responsibility |
|-------|----------------|
| [`crates/ants-core`](crates/ants-core) | Shared domain types (`Job`, `Task`, `Heartbeat`, `NodeId`, …). No I/O, no async runtime. |
| [`crates/ants-network`](crates/ants-network) | `libp2p` transport, mDNS discovery, wire messages. |
| [`crates/ants-worker`](crates/ants-worker) | `wasmtime` sandbox and task execution. |
| [`crates/ants-orchestrator`](crates/ants-orchestrator) | Task queue, scheduling, work-stealing, fault recovery. |
| [`crates/ants-observability`](crates/ants-observability) | `tracing` subscribers, `ratatui` dashboard state. |
| [`crates/ants-cli`](crates/ants-cli) | User-facing `ants` binary that wires everything together. |
| [`examples/`](examples) | `wasm32-wasi` sample jobs. |

The dependency direction is one-way: `ants-cli` depends on the higher-level
crates, which depend on `ants-core`. `ants-core` must never depend on the
others.

## Working rules

1. **Follow the milestones.** Land features in the order described in
   `PROJECT.md`. Do not introduce Milestone 4 concerns (heartbeats,
   work-stealing) inside a Milestone 2 change.
2. **Do not add dependencies without a milestone reason.** Heavy crates such
   as `libp2p`, `wasmtime`, `tokio`, `bincode`, and `ratatui` are added only
   when the milestone that needs them is being implemented. Pin via the
   `[workspace.dependencies]` table so every crate uses the same version.
3. **Scope discipline.** Touch only the crates required by the task. No
   drive-by refactors, no reformatting of unrelated files.
4. **Match the surrounding style.** Read a neighboring file before writing a
   new one. Reuse existing abstractions instead of reinventing them.
5. **Comments earn their keep.** Document non-obvious intent, invariants, or
   trade-offs. Do not narrate code the reader can see.

## Concurrency and I/O

- All network, disk, and long-running work is async on `tokio` once
  introduced. `ants-core` stays sync and dependency-light.
- Never block the runtime from an async context; use `tokio::task::spawn_blocking`
  for CPU-bound or synchronous C FFI calls (`wasmtime` engine work lives
  behind this boundary).

## Serialization boundary

- Types that cross the network derive `serde::Serialize` and
  `serde::Deserialize` and are encoded with `bincode` at the transport edge.
- Keep serialized types in `ants-core` so both the orchestrator and workers
  agree on the schema.

## Observability

- Use `tracing` spans and events for anything worth seeing in the dashboard;
  do not fall back to `println!` / `eprintln!` outside the CLI entrypoint.
- The TUI consumes structured events; prefer stable field names over free-form
  messages.

## Verification checklist

Before finishing a change, run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo check --workspace` is the minimum bar for scaffolding-only commits.

## Out of scope for the current bootstrap

The workspace currently holds stubs only. `libp2p`, `wasmtime`, `ratatui`,
real protocols, the TUI, and example WASM builds all arrive with their
respective milestones — not before.
