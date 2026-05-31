//! Swarm event loop for ants.
//!
//! Handles mDNS discovery, ping/pong, and the job wire protocol.
//! The orchestrator and worker engine are injected via shared state.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use ants_core::mesh::{PingRequest, PingResponse};
use ants_orchestrator::Orchestrator;
use ants_worker::WasmEngine;
use futures::StreamExt;
use libp2p::{
    PeerId, Swarm,
    request_response::{self, Message, OutboundRequestId},
    swarm::SwarmEvent,
};
use tokio::sync::{Mutex, mpsc};

use crate::behaviour::{AntsBehaviour, AntsBehaviourEvent};
use crate::node::NodeError;
use crate::protocol::{TaskRequest, TaskResponse};

/// Shared state for the event loop.
pub(crate) struct SharedState {
    pub orchestrator: Arc<Mutex<Orchestrator>>,
    pub worker: Arc<WasmEngine>,
    /// Queue of outbound job-protocol messages: `(peer, request)`.
    pub outbound_tx: mpsc::UnboundedSender<(PeerId, TaskRequest)>,
    pub outbound_rx: mpsc::UnboundedReceiver<(PeerId, TaskRequest)>,
}

impl SharedState {
    pub fn new(orch: Arc<Mutex<Orchestrator>>, worker: Arc<WasmEngine>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        return Self {
            orchestrator: orch,
            worker,
            outbound_tx: tx,
            outbound_rx: rx,
        };
    }
}

/// Per-node p2p state.
#[derive(Default)]
struct NodeState {
    pinged: HashSet<PeerId>,
    pending_ping: HashSet<PeerId>,
    outbound: HashMap<OutboundRequestId, Instant>,
}

pub(crate) async fn drive(
    mut swarm: Swarm<AntsBehaviour>,
    mut shared: SharedState,
) -> Result<(), NodeError> {
    let mut state = NodeState::default();

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received ctrl-c, shutting down");
                return Ok(());
            }
            event = swarm.select_next_some() => {
                handle_event(&mut swarm, event, &mut state, &shared);
            }
            maybe_out = shared.outbound_rx.recv() => {
                if let Some((peer, request)) = maybe_out {
                    let req_id = swarm.behaviour_mut().job.send_request(&peer, request);
                    tracing::debug!(peer = %peer, ?req_id, "outbound job request dispatched");
                }
            }
        }
    }
}

fn handle_event(
    swarm: &mut Swarm<AntsBehaviour>,
    event: SwarmEvent<AntsBehaviourEvent>,
    state: &mut NodeState,
    shared: &SharedState,
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
            let worker_id = peer_id.to_bytes();
            let orch = shared.orchestrator.clone();
            tokio::spawn(async move {
                let mut orch = orch.lock().await;
                orch.recover_tasks(&worker_id);
            });
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
        SwarmEvent::Behaviour(AntsBehaviourEvent::Job(event)) => {
            handle_job(swarm, event, shared);
        }
        _ => {}
    }
}

// ── mDNS ─────────────────────────────────────────────────────────────────────

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
                swarm.add_peer_address(peer_id, addr.clone());
                if state.pinged.contains(&peer_id) {
                    continue;
                }
                if local < peer_id {
                    state.pending_ping.insert(peer_id);
                    if let Err(err) = swarm.dial(addr) {
                        tracing::warn!(peer = %peer_id, %err, "failed to dial");
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

// ── Ping ─────────────────────────────────────────────────────────────────────

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
                let response = PingResponse {
                    nonce: request.nonce,
                    echoed_unix_ms: now_unix_ms(),
                };
                let _ = swarm.behaviour_mut().ping.send_response(channel, response);
            }
            Message::Response {
                response,
                request_id,
            } => {
                let rtt = outbound
                    .remove(&request_id)
                    .map(|sent| sent.elapsed().as_millis());
                tracing::info!(peer = %peer, nonce = response.nonce, rtt_ms = rtt.unwrap_or(0) as u64, "pong");
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
        request_response::Event::ResponseSent { .. } => {}
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
}

// ── Job protocol ─────────────────────────────────────────────────────────────

fn handle_job(
    swarm: &mut Swarm<AntsBehaviour>,
    event: request_response::Event<TaskRequest, TaskResponse>,
    shared: &SharedState,
) {
    match event {
        request_response::Event::Message { peer, message, .. } => match message {
            Message::Request {
                request, channel, ..
            } => {
                handle_job_request(swarm, peer, request, channel, shared);
            }
            Message::Response { response, .. } => {
                handle_job_response(shared, peer, response);
            }
        },
        request_response::Event::OutboundFailure { peer, error, .. } => {
            tracing::warn!(peer = %peer, %error, "outbound job request failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            tracing::warn!(peer = %peer, %error, "inbound job request failed");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

fn handle_job_request(
    swarm: &mut Swarm<AntsBehaviour>,
    peer: PeerId,
    request: TaskRequest,
    channel: request_response::ResponseChannel<TaskResponse>,
    shared: &SharedState,
) {
    let peer_bytes = peer.to_bytes();
    let orch = shared.orchestrator.clone();

    match request {
        TaskRequest::AssignTasks { count } => {
            let mut orch = orch.blocking_lock();
            let tasks = orch.assign_tasks(&peer_bytes, count as usize);
            let _ = swarm
                .behaviour_mut()
                .job
                .send_response(channel, TaskResponse::Tasks(tasks));
        }
        TaskRequest::SubmitTaskResult { task_id, result } => {
            let mut orch = orch.blocking_lock();
            match orch.record_result(&task_id, result) {
                Ok(()) => {
                    let _ = swarm
                        .behaviour_mut()
                        .job
                        .send_response(channel, TaskResponse::Accepted);
                }
                Err(e) => {
                    let _ = swarm
                        .behaviour_mut()
                        .job
                        .send_response(channel, TaskResponse::Error(e.to_string()));
                }
            }
        }
        TaskRequest::SubmitJob { spec } => {
            let mut orch = orch.blocking_lock();
            match orch.submit_job(spec) {
                Ok(job_id) => {
                    let _ = swarm
                        .behaviour_mut()
                        .job
                        .send_response(channel, TaskResponse::JobCreated(job_id));
                }
                Err(e) => {
                    let _ = swarm
                        .behaviour_mut()
                        .job
                        .send_response(channel, TaskResponse::Error(e.to_string()));
                }
            }
        }
        TaskRequest::QueryJobStatus { job_id } => {
            let orch = orch.blocking_lock();
            let status = orch.get_job_status(&job_id).cloned();
            let _ = swarm
                .behaviour_mut()
                .job
                .send_response(channel, TaskResponse::JobStatus(status));
        }
    }
}

fn handle_job_response(shared: &SharedState, peer: PeerId, response: TaskResponse) {
    match response {
        TaskResponse::Tasks(tasks) => {
            tracing::info!(peer = %peer, count = tasks.len(), "received task assignments");
            let worker = shared.worker.clone();
            let tx = shared.outbound_tx.clone();

            for task in tasks {
                let worker = worker.clone();
                let tx = tx.clone();

                tokio::spawn(async move {
                    let task_id = task.task_id;
                    let result = worker
                        .execute_task(task_id, &task.wasm_bytes, &task.input_slice)
                        .await;
                    tracing::info!(%task_id, exit_code = result.exit_code, "task completed");

                    let req = TaskRequest::SubmitTaskResult { task_id, result };
                    let _ = tx.send((peer, req));
                });
            }
        }
        TaskResponse::Accepted => {
            tracing::debug!(peer = %peer, "result accepted");
        }
        TaskResponse::JobCreated(job_id) => {
            tracing::info!(peer = %peer, %job_id, "job created");
        }
        TaskResponse::JobStatus(status) => {
            tracing::info!(peer = %peer, ?status, "job status");
        }
        TaskResponse::Error(msg) => {
            tracing::warn!(peer = %peer, %msg, "job protocol error");
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
