//! libp2p transport, mDNS discovery, and wire-level messaging for ants.
#![allow(clippy::needless_return)]
//!
//! MS1: ping/pong + mDNS.
//! MS2: job/task wire protocol + orchestrator/worker integration.

mod behaviour;
mod event_loop;
mod node;
mod protocol;

pub use behaviour::{AntsBehaviour, AntsBehaviourEvent};
pub use node::{NodeConfig, NodeError, run_node};
pub use protocol::{TaskRequest, TaskResponse};
