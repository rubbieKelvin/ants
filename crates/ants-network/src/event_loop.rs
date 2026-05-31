//! Swarm event loop for ants.
//!
//! MS1: mDNS + ping.
//! MS2: + job protocol, orchestrator/worker.
//! MS4: + heartbeat liveness, event channel, timeout detection, work-stealing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ants_core::mesh::{HeartbeatRequest, HeartbeatResponse, PingRequest, PingResponse};
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
use crate::event::NodeEvent;
use crate::node::NodeError;
use crate::protocol::{TaskRequest, TaskResponse};

// ── Constants ─────────────────────────────────────────────────────────────────

/// How often each connected peer is sent a heartbeat request (PROJECT.md: 2s).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// How long without a heartbeat before a worker is considered dead (PROJECT.md: 5s).
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// Interval for checking heartbeat timeouts.
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(5);

// ── Shared state ──────────────────────────────────────────────────────────────

/// State shared between the event loop and external consumers.
pub(crate) struct SharedState {
    pub orchestrator: Arc<Mutex<Orchestrator>>,
    pub worker: Arc<WasmEngine>,
    pub outbound_tx: mpsc::UnboundedSender<(PeerId, TaskRequest)>,
    pub outbound_rx: mpsc::UnboundedReceiver<(PeerId, TaskRequest)>,
    /// Sink for typed node events (can be `None` for headless operation).
    pub event_tx: Option<mpsc::UnboundedSender<NodeEvent>>,
}

impl SharedState {
    pub fn new(orch: Arc<Mutex<Orchestrator>>, worker: Arc<WasmEngine>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        return Self {
            orchestrator: orch,
            worker,
            outbound_tx: tx,
            outbound_rx: rx,
            event_tx: None,
        };
    }

    pub fn new_with_events(
        orch: Arc<Mutex<Orchestrator>>,
        worker: Arc<WasmEngine>,
        event_tx: mpsc::UnboundedSender<NodeEvent>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        return Self {
            orchestrator: orch,
            worker,
            outbound_tx: tx,
            outbound_rx: rx,
            event_tx: Some(event_tx),
        };
    }

    fn emit(&self, event: NodeEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }
}

// ── Per-node p2p state ───────────────────────────────────────────────────────

#[derive(Default)]
struct NodeState {
    pinged: HashSet<PeerId>,
    pending_ping: HashSet<PeerId>,
    outbound: HashMap<OutboundRequestId, Instant>,
    /// Tracks the set of peers we should send heartbeats to.
    connected_peers: HashSet<PeerId>,
    /// Tracks sent heartbeats for RTT calculation.
    heartbeat_outbound: HashMap<OutboundRequestId, Instant>,
}

// ── Drive ─────────────────────────────────────────────────────────────────────

pub(crate) async fn drive(
    mut swarm: Swarm<AntsBehaviour>,
    mut shared: SharedState,
) -> Result<(), NodeError> {
    let mut state = NodeState::default();

    let mut heartbeat_tick = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut timeout_tick = tokio::time::interval(TIMEOUT_CHECK_INTERVAL);

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
            _ = heartbeat_tick.tick() => {
                send_heartbeats(&mut swarm, &mut state);
            }
            _ = timeout_tick.tick() => {
                check_timeouts(&mut state, &shared).await;
            }
        }
    }
}

// ── Event dispatch ────────────────────────────────────────────────────────────

fn handle_event(
    swarm: &mut Swarm<AntsBehaviour>,
    event: SwarmEvent<AntsBehaviourEvent>,
    state: &mut NodeState,
    shared: &SharedState,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            tracing::info!(%address, "listening");
            shared.emit(NodeEvent::Listening { address });
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            tracing::debug!(peer = %peer_id, "connection established");
            state.connected_peers.insert(peer_id);
            shared.emit(NodeEvent::ConnectionEstablished { peer_id });
            if state.pending_ping.remove(&peer_id) && state.pinged.insert(peer_id) {
                send_ping(swarm, peer_id, &mut state.outbound);
            }
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            tracing::debug!(peer = %peer_id, ?cause, "connection closed");
            state.connected_peers.remove(&peer_id);
            let reason = cause.map(|c| c.to_string());
            shared.emit(NodeEvent::ConnectionClosed {
                peer_id,
                reason: reason.clone(),
            });
            let worker_id = peer_id.to_bytes();
            let orch = shared.orchestrator.clone();
            let etx = shared.event_tx.clone();
            tokio::spawn(async move {
                let mut orch = orch.lock().await;
                let recovered = orch.recover_tasks(&worker_id);
                if let Some(tx) = etx {
                    let _ = tx.send(NodeEvent::WorkerTimedOut {
                        peer_id,
                        reclaimed_tasks: recovered.len(),
                    });
                }
            });
        }
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            tracing::warn!(peer = ?peer_id, %error, "outgoing connection failed");
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Mdns(event)) => {
            handle_mdns(swarm, event, state, shared);
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Ping(event)) => {
            handle_ping(swarm, event, &mut state.outbound);
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Job(event)) => {
            handle_job(swarm, event, shared);
        }
        SwarmEvent::Behaviour(AntsBehaviourEvent::Heartbeat(event)) => {
            handle_heartbeat(swarm, event, state, shared);
        }
        _ => {}
    }
}

// ── mDNS ─────────────────────────────────────────────────────────────────────

fn handle_mdns(
    swarm: &mut Swarm<AntsBehaviour>,
    event: libp2p::mdns::Event,
    state: &mut NodeState,
    shared: &SharedState,
) {
    use libp2p::mdns::Event;

    match event {
        Event::Discovered(peers) => {
            let local = *swarm.local_peer_id();
            for (peer_id, addr) in peers {
                tracing::info!(peer = %peer_id, %addr, "discovered peer");
                shared.emit(NodeEvent::PeerDiscovered {
                    peer_id,
                    address: addr.clone(),
                });
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
                shared.emit(NodeEvent::PeerExpired {
                    peer_id,
                    address: addr,
                });
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

// ── Heartbeat ─────────────────────────────────────────────────────────────────

/// Send a heartbeat to every connected peer.
fn send_heartbeats(swarm: &mut Swarm<AntsBehaviour>, state: &mut NodeState) {
    let peers: Vec<PeerId> = state.connected_peers.iter().copied().collect();
    if peers.is_empty() {
        return;
    }
    for peer in &peers {
        let request = HeartbeatRequest {
            nonce: rand::random(),
            sent_unix_ms: now_unix_ms(),
            queue_depth: 0,  // TODO: expose from orchestrator
            active_tasks: 0, // TODO: expose from orchestrator
        };
        let req_id = swarm.behaviour_mut().heartbeat.send_request(peer, request);
        state.heartbeat_outbound.insert(req_id, Instant::now());
    }
    if !peers.is_empty() {
        tracing::trace!(count = peers.len(), "heartbeats sent");
    }
}

fn handle_heartbeat(
    swarm: &mut Swarm<AntsBehaviour>,
    event: request_response::Event<HeartbeatRequest, HeartbeatResponse>,
    state: &mut NodeState,
    shared: &SharedState,
) {
    match event {
        request_response::Event::Message { peer, message, .. } => match message {
            Message::Request {
                request, channel, ..
            } => {
                // Record heartbeat in orchestrator.
                let orch = shared.orchestrator.clone();
                let peer_bytes = peer.to_bytes();
                let queue_depth = request.queue_depth;
                let active_tasks = request.active_tasks;
                tokio::spawn(async move {
                    orch.lock()
                        .await
                        .record_heartbeat(&peer_bytes, queue_depth, active_tasks);
                });

                shared.emit(NodeEvent::HeartbeatReceived {
                    peer_id: peer,
                    queue_depth,
                    active_tasks,
                });

                let response = HeartbeatResponse {
                    nonce: request.nonce,
                    echoed_unix_ms: now_unix_ms(),
                };
                let _ = swarm
                    .behaviour_mut()
                    .heartbeat
                    .send_response(channel, response);
            }
            Message::Response {
                response,
                request_id,
            } => {
                let rtt = state
                    .heartbeat_outbound
                    .remove(&request_id)
                    .map(|sent| sent.elapsed().as_millis());
                tracing::trace!(
                    peer = %peer,
                    nonce = response.nonce,
                    echo_delta = response.echoed_unix_ms.saturating_sub(now_unix_ms()),
                    rtt_ms = rtt.unwrap_or(0),
                    "heartbeat ack",
                );
            }
        },
        request_response::Event::OutboundFailure {
            peer,
            request_id,
            error,
            ..
        } => {
            state.heartbeat_outbound.remove(&request_id);
            tracing::debug!(peer = %peer, %error, "outbound heartbeat failed");
        }
        request_response::Event::InboundFailure { peer, error, .. } => {
            tracing::debug!(peer = %peer, %error, "inbound heartbeat failed");
        }
        request_response::Event::ResponseSent { .. } => {}
    }
}

/// Check for workers that have exceeded the heartbeat timeout.
async fn check_timeouts(state: &mut NodeState, shared: &SharedState) {
    let timed_out = {
        let mut orch = shared.orchestrator.lock().await;
        orch.check_timeouts(HEARTBEAT_TIMEOUT)
    };

    for (worker_id, reclaimed) in &timed_out {
        // Convert worker bytes back to PeerId for the event.
        if let Ok(peer_id) = PeerId::from_bytes(worker_id) {
            state.connected_peers.remove(&peer_id);
            shared.emit(NodeEvent::WorkerTimedOut {
                peer_id,
                reclaimed_tasks: reclaimed.len(),
            });
            tracing::warn!(peer = %peer_id, count = reclaimed.len(), "worker timed out, tasks reclaimed");
        }
    }
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
    let etx = shared.event_tx.clone();

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
            let exit_code = result.exit_code;
            let mut orch = orch.blocking_lock();
            match orch.record_result(&task_id, result) {
                Ok(()) => {
                    if let Some(tx) = etx {
                        let _ = tx.send(NodeEvent::TaskResultReceived { task_id, exit_code });
                    }
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
                    if let Some(tx) = etx {
                        let _ = tx.send(NodeEvent::JobSubmitted { job_id });
                    }
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
            if let Some(ref status) = status
                && let Some(tx) = etx
            {
                let _ = tx.send(NodeEvent::JobStatusChanged {
                    job_id,
                    status: status.clone(),
                });
            }
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
