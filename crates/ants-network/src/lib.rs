//! libp2p transport, mDNS discovery, and wire-level messaging for ants.
//!
//! Milestone 1 surface area:
//!
//! - [`NodeConfig`] describes how the node should listen.
//! - [`run_node`] boots a libp2p [`Swarm`][libp2p::Swarm], discovers LAN
//!   peers via mDNS, and exchanges `PingRequest` / `PingResponse` messages
//!   on the `/ants/ping/1.0.0` request-response protocol.
//!
//! Later milestones will extend the behaviour set (gossipsub, task
//! distribution, heartbeats, work-stealing). This crate intentionally keeps
//! its public API small so those additions can land incrementally.

mod behaviour;
mod event_loop;
mod node;

pub use behaviour::{AntsBehaviour, AntsBehaviourEvent};
pub use node::{NodeConfig, NodeError, run_node};
