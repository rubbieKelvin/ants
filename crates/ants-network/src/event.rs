//! Structured events emitted by the node event loop.
//!
//! Consumers (CLI logger, GPUI app, tests) subscribe via
//! [`crate::node::run_node_with_events`].

use ants_core::job::{JobId, JobStatus, TaskId};
use libp2p::{Multiaddr, PeerId};

/// Every significant thing that happens inside the node swarm.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// Swarm started listening on a multiaddress.
    Listening { address: Multiaddr },
    /// mDNS discovered a new peer on the LAN.
    PeerDiscovered { peer_id: PeerId, address: Multiaddr },
    /// An mDNS peer entry expired.
    PeerExpired { peer_id: PeerId, address: Multiaddr },
    /// A libp2p connection was fully established.
    ConnectionEstablished { peer_id: PeerId },
    /// A libp2p connection was closed.
    ConnectionClosed {
        peer_id: PeerId,
        reason: Option<String>,
    },
    /// An outbound ping round-trip completed.
    PingResult { peer_id: PeerId, rtt_ms: u64 },
    /// Heartbeat liveness signal received from a peer.
    HeartbeatReceived {
        peer_id: PeerId,
        queue_depth: u32,
        active_tasks: u32,
    },
    /// A worker failed to send heartbeats within the timeout window and its
    /// tasks have been reclaimed.
    WorkerTimedOut {
        peer_id: PeerId,
        reclaimed_tasks: usize,
    },
    /// A new job was submitted to the orchestrator.
    JobSubmitted { job_id: JobId },
    /// A job's status changed.
    JobStatusChanged { job_id: JobId, status: JobStatus },
    /// A task's result was recorded.
    TaskResultReceived { task_id: TaskId, exit_code: i32 },
}
