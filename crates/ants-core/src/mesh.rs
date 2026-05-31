//! Wire-level message types exchanged by nodes in the ants mesh.
use serde::{Deserialize, Serialize};

/// libp2p `StreamProtocol` name for the ping/pong request-response endpoint.
///
/// The `/ants/<feature>/<semver>` shape leaves room to version individual
/// behaviours independently as the protocol grows.
pub const PING_PROTOCOL: &str = "/ants/ping/1.0.0";

/// libp2p `StreamProtocol` name for the heartbeat endpoint.
pub const HEARTBEAT_PROTOCOL: &str = "/ants/heartbeat/1.0.0";

/// Sent by a node to probe a peer's liveness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingRequest {
    /// Random identifier echoed in the response so the sender can correlate
    /// replies to specific outbound pings.
    pub nonce: u64,
    /// Unix timestamp (milliseconds) captured on the sender just before the
    /// request is dispatched.
    pub sent_unix_ms: u64,
}

/// Reply to a [`PingRequest`]; `nonce` is copied verbatim from the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PingResponse {
    pub nonce: u64,
    /// Unix timestamp (milliseconds) captured on the responder as it sends
    /// the reply. Primarily useful for coarse clock-skew / RTT diagnostics.
    pub echoed_unix_ms: u64,
}

// ── Heartbeat ─────────────────────────────────────────────────────────────────

/// Periodic liveness signal sent by every connected peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    /// Random nonce for deduplication / RTT correlation.
    pub nonce: u64,
    /// Unix timestamp (milliseconds) captured by the sender.
    pub sent_unix_ms: u64,
    /// Number of tasks waiting in the sender's queue.
    pub queue_depth: u32,
    /// Number of tasks currently being executed.
    pub active_tasks: u32,
}

/// Acknowledgement sent in reply to a [`HeartbeatRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Copied from the request so the sender can correlate.
    pub nonce: u64,
    /// Unix timestamp (milliseconds) captured by the responder.
    pub echoed_unix_ms: u64,
}
