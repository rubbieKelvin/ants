//! Shared domain types for the ants mesh scheduler.
//!
//! Concrete types (`Job`, `Task`, `Heartbeat`, `NodeId`, …) are introduced
//! alongside the milestones that first require them. See `PROJECT.md`.

pub mod mesh;

/// Crate name constant kept for smoke tests and banner output.
pub const CRATE_NAME: &str = "ants-core";
