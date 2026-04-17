# Project: ants

**A WASM-based distributed mesh scheduler in Rust.**

## 1. Project description

ants is a decentralized, MapReduce-style task orchestrator designed for
heterogenous edge devices (laptops, desktops, Raspberry Pis). Instead of
running monolithic binaries, ants distributes pre-compiled WebAssembly (WASM)
modules and distinct data payloads across a peer-to-peer mesh network.

The node that submits the job dynamically becomes the **orchestrator**,
managing a central task queue, tracking worker-node telemetry, and handling
fault tolerance. If a worker node goes offline unexpectedly, the orchestrator
instantly reassigns its work. To maximize efficiency, idle nodes employ a
**work-stealing algorithm** to pull pending tasks from overburdened peers.

## 2. Core requirements

### Functional

- **WASM execution.** Worker nodes must securely sandbox and execute arbitrary
  `.wasm` binaries.
- **P2P networking.** Nodes must automatically discover each other on a local
  network and establish secure communication channels.
- **Dynamic orchestration.** Any node can submit a job and become the
  orchestrator for that specific workload.
- **Fault tolerance.** The orchestrator must track node heartbeats. Missed
  heartbeats trigger task reassignment.
- **Work-stealing.** Idle nodes must be able to query the network for
  unassigned or backlogged tasks to minimize total job execution time.
- **Observability.** The orchestrator must provide a real-time visualization
  of mesh state, task completion, and node health.

### Non-functional

- **Cross-platform.** Must run seamlessly on x86_64 (desktops) and ARM64
  (Raspberry Pi) without cross-compilation of the target tasks.
- **Concurrency.** Must safely handle thousands of async network events and
  local execution threads without blocking.

## 3. Technology stack

- **Language:** Rust.
- **Networking:** `libp2p` (mDNS for local discovery; Gossipsub and
  Request/Response for messaging).
- **Execution runtime:** `wasmtime` (Bytecode Alliance).
- **Concurrency:** `tokio` (async runtime).
- **Serialization:** `bincode` + `serde` for fast, binary data transfer.
- **Observability:** `tracing` for async-aware logging and `ratatui` for the
  terminal dashboard.

## 4. Project structure

The codebase is a Rust Cargo workspace with one crate per responsibility.

```text
ants/
├── Cargo.toml                  # workspace definition
├── crates/
│   ├── ants-core/              # shared structs: Job, Task, Heartbeat, NodeId
│   ├── ants-network/           # libp2p setup, discovery, msg serialization
│   ├── ants-worker/            # wasmtime runtime, task execution logic
│   ├── ants-observability/     # tracing subscribers, dashboard state
│   ├── ants-orchestrator/      # task queue, work-stealing, fault recovery
│   └── ants-cli/               # `ants` binary: user-facing CLI entrypoint
└── examples/
    ├── math-brute/             # WASM job: calculate π or primes
    └── raytracer/              # WASM job: render a small 3D image
```

## 5. Execution plan and milestones

The system is built in vertical slices so every stage produces a working,
testable deliverable.

### Milestone 1 — The mesh foundation (networking)

Goal: get heterogenous devices talking to each other.

- Initialize the Rust workspace.
- Implement `libp2p` mDNS so nodes automatically discover each other on the
  local network.
- Implement a basic Request/Response protocol.
- **Deliverable:** starting the app on a laptop and a Raspberry Pi causes them
  to log "Discovered Node X" and successfully exchange ping/pong messages.

### Milestone 2 — The sandbox (local WASM)

Goal: execute a WASM payload safely.

- Write a simple Rust function (for instance, multiply two numbers or check a
  prime) and compile it to `wasm32-wasi`.
- Integrate `wasmtime` into `ants-worker`.
- Load a `.wasm` file from disk, pass it input arguments, and extract the
  return value.
- **Deliverable:** a CLI command `ants run local test.wasm` that executes the
  task locally.

### Milestone 3 — The hive (basic distribution)

Goal: distribute work over the network.

- Define the `bincode`-serialized `JobBroadcast` and `TaskPayload` messages.
- Implement the orchestrator logic that splits a dummy job into ten payloads.
- Send the WASM file and payloads to available nodes over `libp2p`.
- Workers execute the tasks and return `TaskResult` messages.
- **Deliverable:** the orchestrator distributes a math job across three
  devices and aggregates the final answer.

### Milestone 4 — Resilience and work-stealing

Goal: handle chaos and optimize speed.

- Broadcast a `Heartbeat` every two seconds.
- Orchestrator monitors timestamps; if a node's heartbeat is older than five
  seconds, mark it as dead and push its active tasks back to the queue.
- Implement work-stealing: when a worker's local queue is empty, it sends a
  `NeedWork` broadcast and the orchestrator reassigns pending chunks.
- **Deliverable:** pulling the power cord on a Raspberry Pi mid-job does not
  stop the remaining laptops from finishing the whole job cleanly.

### Milestone 5 — The control tower (observability)

Goal: see the matrix.

- Integrate `tracing` throughout the workspace.
- Build a `ratatui` dashboard in the `ants` binary.
- Visual panes for active nodes and their latency, global job progress, and
  an event log (for example, "Node A stole 5 tasks from Node C").
- **Deliverable:** a real-time terminal UI tracking the distributed mesh.
