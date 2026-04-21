//! Composed `NetworkBehaviour` for an ants node.
//!
//! For Milestone 1 the behaviour is deliberately narrow: mDNS for LAN peer
//! discovery plus a cbor-encoded request/response endpoint that carries
//! `PingRequest` / `PingResponse`. The cbor codec is used because it is the
//! zero-boilerplate path in libp2p 0.56. `PROJECT.md` mentions `bincode` as
//! the long-term wire format for real job/task payloads; that swap happens
//! in Milestone 3 when those types are introduced.

use ants_core::mesh::{PING_PROTOCOL, PingRequest, PingResponse};
use libp2p::{
    StreamProtocol, mdns,
    request_response::{self, ProtocolSupport},
    swarm::NetworkBehaviour,
};

/// Top-level behaviour combining mDNS discovery with request/response ping.
#[derive(NetworkBehaviour)]
pub struct AntsBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub ping: request_response::cbor::Behaviour<PingRequest, PingResponse>,
}

impl AntsBehaviour {
    /// Construct the composed behaviour from a libp2p identity keypair.
    pub(crate) fn new(
        keypair: &libp2p::identity::Keypair,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mdns =
            mdns::tokio::Behaviour::new(mdns::Config::default(), keypair.public().to_peer_id())?;

        let ping = request_response::cbor::Behaviour::<PingRequest, PingResponse>::new(
            [(StreamProtocol::new(PING_PROTOCOL), ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        Ok(Self { mdns, ping })
    }
}
