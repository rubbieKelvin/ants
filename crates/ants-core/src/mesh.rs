//! Wire-level message types exchanged by nodes in the ants mesh.
use serde::{Deserialize, Serialize};

/// libp2p `StreamProtocol` name for the ping/pong request-response endpoint.
///
/// The `/ants/<feature>/<semver>` shape leaves room to version individual
/// behaviours independently as the protocol grows.
pub const PING_PROTOCOL: &str = "/ants/ping/1.0.0";

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
