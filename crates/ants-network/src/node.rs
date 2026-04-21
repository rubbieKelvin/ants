//! Public entry point for running an ants node.

use std::str::FromStr;

use libp2p::{Multiaddr, SwarmBuilder, noise, tcp, yamux};
use thiserror::Error;

use crate::behaviour::AntsBehaviour;
use crate::event_loop;

/// User-facing configuration for [`run_node`]. Extended cautiously; defaults
/// stay sane so the CLI can call `run_node(NodeConfig::default())` and get
/// LAN-ready behaviour.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Addresses the swarm should listen on. An empty vector falls back to
    /// [`NodeConfig::default_listen_addrs`].
    pub listen_on: Vec<Multiaddr>,
}

impl NodeConfig {
    /// Default listen set: one IPv4 TCP socket on an OS-assigned port.
    pub fn default_listen_addrs() -> Vec<Multiaddr> {
        vec![Multiaddr::from_str("/ip4/0.0.0.0/tcp/0").expect("valid multiaddr literal")]
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            listen_on: Self::default_listen_addrs(),
        }
    }
}

/// Errors surfaced from [`run_node`].
#[derive(Debug, Error)]
pub enum NodeError {
    #[error("failed to configure transport: {0}")]
    Transport(String),

    #[error("failed to build network behaviour: {0}")]
    Behaviour(String),

    #[error("failed to start listening on {addr}: {source}")]
    Listen {
        addr: Multiaddr,
        #[source]
        source: libp2p::TransportError<std::io::Error>,
    },

    #[error("I/O error while running node: {0}")]
    Io(#[from] std::io::Error),
}

/// Start a long-running ants node. Blocks until an unrecoverable error or
/// the user presses Ctrl-C.
pub async fn run_node(cfg: NodeConfig) -> Result<(), NodeError> {
    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| NodeError::Transport(e.to_string()))?
        .with_behaviour(AntsBehaviour::new)
        .map_err(|e| NodeError::Behaviour(e.to_string()))?
        .build();

    let local_peer_id = *swarm.local_peer_id();
    tracing::info!(peer_id = %local_peer_id, "local peer id");

    let listen_addrs = if cfg.listen_on.is_empty() {
        NodeConfig::default_listen_addrs()
    } else {
        cfg.listen_on
    };

    for addr in listen_addrs {
        swarm
            .listen_on(addr.clone())
            .map_err(|source| NodeError::Listen { addr, source })?;
    }

    event_loop::drive(swarm).await
}
