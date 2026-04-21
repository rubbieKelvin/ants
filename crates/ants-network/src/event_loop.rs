//! Swarm event loop for Milestone 1.
//!
//! Flow:
//!
//! 1. mDNS discovers a peer and reports its dialable address.
//! 2. We teach the swarm the address, then actively dial the peer.
//! 3. Once the connection is established, we send a [`PingRequest`].
//! 4. The remote side replies with a [`PingResponse`]; we log the RTT.
//!
//! Ctrl-C exits the loop cleanly.

use std::collections::{HashMap, HashSet};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ants_core::mesh::{PingRequest, PingResponse};
use futures::StreamExt;
use libp2p::{
    PeerId, Swarm,
    request_response::{self, Message, OutboundRequestId},
    swarm::SwarmEvent,
};

use crate::behaviour::{AntsBehaviour, AntsBehaviourEvent};
use crate::node::NodeError;

/// Per-node mutable state for the event loop. Bundled together so the match
/// arms below stay terse.
#[derive(Default)]
struct NodeState {
    /// Peers for which we have already fired a ping (and don't want to spam
    /// again when mDNS re-announces them).
    pinged: HashSet<PeerId>,
    /// Peers discovered via mDNS that we owe a ping once their connection
    /// becomes established.
    pending_ping: HashSet<PeerId>,
    /// Outstanding outbound pings keyed by `OutboundRequestId`, used to
    /// compute RTT when the response comes back.
    outbound: HashMap<OutboundRequestId, Instant>,
}

pub(crate) async fn drive(mut swarm: Swarm<AntsBehaviour>) -> Result<(), NodeError> {
    let mut state = NodeState::default();

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received ctrl-c, shutting down");
                return Ok(());
            }
            event = swarm.select_next_some() => {
                handle_event(&mut swarm, event, &mut state);
            }
        }
    }
}

fn handle_event(
    swarm: &mut Swarm<AntsBehaviour>,
    event: SwarmEvent<AntsBehaviourEvent>,
    state: &mut NodeState,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            tracing::info!(%address, "listening");
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            tracing::debug!(peer = %peer_id, "connection established");
            if state.pending_ping.remove(&peer_id) && state.pinged.insert(peer_id) {
                send_ping(swarm, peer_id, &mut state.outbound);
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            tracing::debug!(peer = %peer_id, ?cause, "connection closed");
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            tracing::warn!(peer = ?peer_id, %error, "outgoing connection failed");
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Mdns(event)) => {
            handle_mdns(swarm, event, state);
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Ping(event)) => {
            handle_ping(swarm, event, &mut state.outbound);
        }
        _ => {}
    }
}

fn handle_mdns(
    swarm: &mut Swarm<AntsBehaviour>,
    event: libp2p::mdns::Event,
    state: &mut NodeState,
) {
    use libp2p::mdns::Event;

    match event {
        Event::Discovered(peers) => {
            let local = *swarm.local_peer_id();
            for (peer_id, addr) in peers {
                tracing::info!(peer = %peer_id, %addr, "discovered peer");

                // Teach the swarm how to reach this peer so request-response
                // (and any future behaviour) can find an address in the
                // book when it needs to dial.
                swarm.add_peer_address(peer_id, addr.clone());

                if state.pinged.contains(&peer_id) {
                    continue;
                }

                // Both sides of an mDNS discovery see each other at roughly
                // the same instant. If both actively dial, noise handshakes
                // race and one side fails. We break the tie with the peer
                // id: only the lexicographically smaller peer dials; the
                // other side will answer on the inbound connection.
                if local < peer_id {
                    state.pending_ping.insert(peer_id);
                    if let Err(err) = swarm.dial(addr) {
                        tracing::warn!(peer = %peer_id, %err, "failed to dial discovered peer");
                        state.pending_ping.remove(&peer_id);
                    }
                } else {
                    tracing::debug!(peer = %peer_id, "yielding dial to remote peer");
                }
            }
        }
        Event::Expired(peers) => {
            for (peer_id, addr) in peers {
                tracing::info!(peer = %peer_id, %addr, "peer expired");
            }
        }
    }
}

fn handle_ping(
    swarm: &mut Swarm<AntsBehaviour>,
    event: request_response::Event<PingRequest, PingResponse>,
    outbound: &mut HashMap<OutboundRequestId, Instant>,
) {
    match event {
        request_response::Event::Message { peer, message, .. } => match message {
            Message::Request {
                request, channel, ..
            } => {
                tracing::info!(peer = %peer, nonce = request.nonce, "ping received");
                let response = PingResponse {
                    nonce: request.nonce,
                    echoed_unix_ms: now_unix_ms(),
                };
                if swarm
                    .behaviour_mut()
                    .ping
                    .send_response(channel, response)
                    .is_err()
                {
                    tracing::warn!(peer = %peer, "failed to send pong; receiver dropped");
                }
            }
            Message::Response {
                response,
                request_id,
            } => {
                let rtt_ms = outbound
                    .remove(&request_id)
                    .map(|sent| sent.elapsed().as_millis());
                match rtt_ms {
                    Some(rtt) => tracing::info!(
                        peer = %peer,
                        nonce = response.nonce,
                        rtt_ms = rtt as u64,
                        "pong received",
                    ),
                    None => tracing::info!(
                        peer = %peer,
                        nonce = response.nonce,
                        "pong received (no tracked send time)",
                    ),
                }
            }
        },
        request_response::Event::OutboundFailure {
            peer,
            request_id,
            error,
            ..
        } => {
            outbound.remove(&request_id);
            tracing::warn!(peer = %peer, %error, "outbound ping failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            tracing::warn!(peer = %peer, %error, "inbound ping failed");
        }
        request_response::Event::ResponseSent { peer, .. } => {
            tracing::debug!(peer = %peer, "pong delivered");
        }
    }
}

fn send_ping(
    swarm: &mut Swarm<AntsBehaviour>,
    peer: PeerId,
    outbound: &mut HashMap<OutboundRequestId, Instant>,
) {
    let request = PingRequest {
        nonce: rand::random(),
        sent_unix_ms: now_unix_ms(),
    };
    let request_id = swarm.behaviour_mut().ping.send_request(&peer, request);
    outbound.insert(request_id, Instant::now());
    tracing::debug!(peer = %peer, ?request_id, "ping dispatched");
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
