//! Composed `NetworkBehaviour` for an ants node.
//!
//! MS1: mDNS for LAN peer discovery + cbor-encoded ping request/response.
//! MS2: adds a job protocol (`/ants/job/1.0.0`) extending request/response
//! with task assignment, result submission, job creation, and status queries.

use ants_core::job::JOB_PROTOCOL;
use ants_core::mesh::{PING_PROTOCOL, PingRequest, PingResponse};
use libp2p::{
    StreamProtocol, mdns,
    request_response::{Config, ProtocolSupport, cbor},
    swarm::NetworkBehaviour,
};

use crate::protocol::{TaskRequest, TaskResponse};

/// Top-level behaviour combining mDNS discovery, request/response ping,
/// and the job protocol.
#[derive(NetworkBehaviour)]
pub struct AntsBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub ping: cbor::Behaviour<PingRequest, PingResponse>,
    pub job: cbor::Behaviour<TaskRequest, TaskResponse>,
}

impl AntsBehaviour {
    /// Construct the composed behaviour from a libp2p identity keypair.
    pub(crate) fn new(
        keypair: &libp2p::identity::Keypair,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mdns_config = mdns::Config::default();
        let mdns = mdns::tokio::Behaviour::new(mdns_config, keypair.public().to_peer_id())?;

        let ping = cbor::Behaviour::<PingRequest, PingResponse>::new(
            [(StreamProtocol::new(PING_PROTOCOL), ProtocolSupport::Full)],
            Config::default(),
        );

        let job = cbor::Behaviour::<TaskRequest, TaskResponse>::new(
            [(StreamProtocol::new(JOB_PROTOCOL), ProtocolSupport::Full)],
            Config::default(),
        );

        return Ok(Self { mdns, ping, job });
    }
}
