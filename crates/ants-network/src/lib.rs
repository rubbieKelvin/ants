//! libp2p transport, mDNS discovery, and wire-level messaging for ants.
#![allow(clippy::needless_return)]
//!
//! MS1: ping/pong + mDNS.
//! MS2: job/task wire protocol + orchestrator/worker integration.
//! MS4: heartbeat liveness, event channel, work-stealing.

mod behaviour;
mod event;
mod event_loop;
mod node;
mod protocol;

pub use behaviour::{AntsBehaviour, AntsBehaviourEvent};
pub use event::NodeEvent;
pub use node::{NodeConfig, NodeError, run_node, run_node_with_events};
pub use protocol::{TaskRequest, TaskResponse};
