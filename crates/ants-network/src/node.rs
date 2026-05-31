//! Public entry point for running an ants node.

use std::str::FromStr;
use std::sync::Arc;

use ants_orchestrator::Orchestrator;
use ants_worker::WasmEngine;
use libp2p::{Multiaddr, SwarmBuilder, noise, tcp, yamux};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

use crate::behaviour::AntsBehaviour;
use crate::event::NodeEvent;
use crate::event_loop::{self, SharedState};

/// User-facing configuration for [`run_node`].
#[derive(Clone)]
pub struct NodeConfig {
    /// Addresses the swarm should listen on.
    pub listen_on: Vec<Multiaddr>,
    /// An optional orchestrator to manage jobs on this node.
    pub orchestrator: Option<Arc<Mutex<Orchestrator>>>,
    /// An optional worker engine to execute tasks on this node.
    pub worker: Option<Arc<WasmEngine>>,
}

impl NodeConfig {
    pub fn default_listen_addrs() -> Vec<Multiaddr> {
        return vec![Multiaddr::from_str("/ip4/0.0.0.0/tcp/0").expect("valid multiaddr literal")];
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        return Self {
            listen_on: Self::default_listen_addrs(),
            orchestrator: None,
            worker: None,
        };
    }
}

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

/// Start a long-running ants node with an event sink.
///
/// The caller receives typed [`NodeEvent`] values over the returned
/// unbounded sender.  For headless operation (e.g. the CLI) use
/// [`run_node`] instead, which logs events to `tracing`.
pub async fn run_node_with_events(
    cfg: NodeConfig,
    event_tx: mpsc::UnboundedSender<NodeEvent>,
) -> Result<(), NodeError> {
    let (swarm, orchestrator, worker) = build_node(cfg).await?;
    let shared = SharedState::new_with_events(orchestrator, worker, event_tx);
    return event_loop::drive(swarm, shared).await;
}

/// Start a long-running ants node.  Events are logged via `tracing`.
/// Blocks until Ctrl-C or fatal error.
pub async fn run_node(cfg: NodeConfig) -> Result<(), NodeError> {
    let (swarm, orchestrator, worker) = build_node(cfg).await?;
    let shared = SharedState::new(orchestrator, worker);
    return event_loop::drive(swarm, shared).await;
}

async fn build_node(
    cfg: NodeConfig,
) -> Result<
    (
        libp2p::Swarm<AntsBehaviour>,
        Arc<Mutex<Orchestrator>>,
        Arc<WasmEngine>,
    ),
    NodeError,
> {
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

    let orchestrator = cfg
        .orchestrator
        .unwrap_or_else(|| Arc::new(Mutex::new(Orchestrator::new())));
    let worker = cfg.worker.unwrap_or_else(|| {
        Arc::new(
            WasmEngine::new(ants_worker::SandboxConfig::default())
                .expect("default wasm engine construction"),
        )
    });

    return Ok((swarm, orchestrator, worker));
}
