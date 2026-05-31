//! Composed `NetworkBehaviour` for an ants node.
//!
//! MS1: mDNS + ping.
//! MS2: + job protocol.
//! MS4: + heartbeat protocol.

use ants_core::job::JOB_PROTOCOL;
use ants_core::mesh::{
    HEARTBEAT_PROTOCOL, HeartbeatRequest, HeartbeatResponse, PING_PROTOCOL, PingRequest,
    PingResponse,
};
use libp2p::{
    StreamProtocol, mdns,
    request_response::{Config, ProtocolSupport, cbor},
    swarm::NetworkBehaviour,
};

use crate::protocol::{TaskRequest, TaskResponse};

/// Top-level behaviour combining mDNS discovery, ping, job protocol, and
/// heartbeat liveness.
#[derive(NetworkBehaviour)]
pub struct AntsBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub ping: cbor::Behaviour<PingRequest, PingResponse>,
    pub job: cbor::Behaviour<TaskRequest, TaskResponse>,
    pub heartbeat: cbor::Behaviour<HeartbeatRequest, HeartbeatResponse>,
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

        let heartbeat = cbor::Behaviour::<HeartbeatRequest, HeartbeatResponse>::new(
            [(
                StreamProtocol::new(HEARTBEAT_PROTOCOL),
                ProtocolSupport::Full,
            )],
            Config::default(),
        );

        return Ok(Self {
            mdns,
            ping,
            job,
            heartbeat,
        });
    }
}
