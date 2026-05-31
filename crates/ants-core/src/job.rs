//! Job domain types for the ants mesh scheduler.
//!
//! Defines the shape of a distributed computation: a [`JobSpec`] describes the
//! WASM module and input data; the orchestrator splits it into [`Task`] units,
//! workers return [`TaskResult`], and the orchestrator tracks progress via
//! [`JobState`] and [`JobStatus`].

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// libp2p `StreamProtocol` name for the job request-response endpoint.
pub const JOB_PROTOCOL: &str = "/ants/job/1.0.0";

// ── IDs ─────────────────────────────────────────────────────────────────────

/// Globally unique identifier for a submitted job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(Uuid);

/// Globally unique identifier for one unit of work inside a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(Uuid);

impl JobId {
    /// Generate a fresh [`JobId`] using UUID v7 (time-ordered, unique).
    pub fn new() -> Self {
        return Self(Uuid::now_v7());
    }
}

impl TaskId {
    /// Generate a fresh [`TaskId`] using UUID v7.
    pub fn new() -> Self {
        return Self(Uuid::now_v7());
    }
}

impl Default for JobId {
    fn default() -> Self {
        return Self::new();
    }
}

impl Default for TaskId {
    fn default() -> Self {
        return Self::new();
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        return write!(f, "{}", self.0);
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        return write!(f, "{}", self.0);
    }
}

impl FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        return Uuid::from_str(s).map(JobId);
    }
}

impl FromStr for TaskId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        return Uuid::from_str(s).map(TaskId);
    }
}

// ── Job spec ─────────────────────────────────────────────────────────────────

/// Describes a computation that a user wants to distribute.
///
/// The orchestrator splits [`Self::input_data`] into `num_tasks` chunks and
/// hands each chunk (plus the WASM binary) to a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobSpec {
    /// The raw WASM module bytes (compiled from C/Rust/AssemblyScript, …).
    pub wasm_bytes: Vec<u8>,
    /// Input data that will be partitioned across tasks.
    pub input_data: Vec<u8>,
    /// How many tasks (shards) to create from this job.
    pub num_tasks: u32,
    /// Arbitrary key-value metadata (job name, owner, priority hint, …).
    pub metadata: HashMap<String, String>,
    /// SHA-256 digest of [`Self::wasm_bytes`] computed at construction time.
    /// Workers can verify integrity before execution.
    pub wasm_hash: [u8; 32],
}

impl JobSpec {
    /// Build a [`JobSpec`], computing the [`Self::wasm_hash`] from the
    /// supplied WASM bytes.  Returns `None` if `num_tasks` is zero or if
    /// `input_data` is empty.
    pub fn new(
        wasm_bytes: Vec<u8>,
        input_data: Vec<u8>,
        num_tasks: u32,
        metadata: HashMap<String, String>,
    ) -> Option<Self> {
        if num_tasks == 0 || input_data.is_empty() {
            return None;
        }

        let mut hasher = Sha256::new();
        hasher.update(&wasm_bytes);
        let wasm_hash: [u8; 32] = hasher.finalize().into();

        return Some(Self {
            wasm_bytes,
            input_data,
            num_tasks,
            metadata,
            wasm_hash,
        });
    }
}

// ── Task ─────────────────────────────────────────────────────────────────────

/// One unit of work: a slice of the parent job's input data plus the shared
/// WASM module.  A worker receives a [`Task`], executes the WASM, and returns a
/// [`TaskResult`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    /// Unique identifier for this task.
    pub task_id: TaskId,
    /// The job this task belongs to.
    pub job_id: JobId,
    /// The full WASM binary (shared across all tasks of a job).
    pub wasm_bytes: Vec<u8>,
    /// The input slice assigned to this task.
    pub input_slice: Vec<u8>,
    /// Position of this task within the job (0, 1, …, num_tasks-1).
    pub seq: u32,
}

// ── Task result ──────────────────────────────────────────────────────────────

/// The output of executing a single [`Task`] in the WASM sandbox.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResult {
    /// Which task this result belongs to.
    pub task_id: TaskId,
    /// Captured stdout bytes from the WASM instance.
    pub stdout: Vec<u8>,
    /// Captured stderr bytes from the WASM instance.
    pub stderr: Vec<u8>,
    /// Exit code returned by the WASM module (0 = success).
    pub exit_code: i32,
    /// Wall-clock duration of the execution, in milliseconds.
    pub wall_time_ms: u64,
}

// ── Job status ───────────────────────────────────────────────────────────────

/// Tracks the high-level lifecycle phase of a submitted job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStatus {
    /// Not yet dispatched to any worker.
    Pending,
    /// Some tasks are in-flight or completed.
    Running {
        /// Number of tasks that have finished (success or non-zero exit).
        done: u32,
        /// Total number of tasks in this job.
        total: u32,
    },
    /// All tasks returned [`TaskResult`] with exit code 0.
    Completed,
    /// At least one task returned a non-zero exit code or the job was
    /// explicitly cancelled. The `String` carries a human-readable reason.
    Failed(String),
}

// ── Job state ────────────────────────────────────────────────────────────────

/// Full mutable state held by the orchestrator for one job.
#[derive(Debug, Clone)]
pub struct JobState {
    /// The original job specification.
    pub spec: JobSpec,
    /// Current lifecycle status.
    pub status: JobStatus,
    /// Per-task results.  A key that maps to `None` means the task has not
    /// been assigned / completed yet.  `Some(TaskResult)` means the task is
    /// done.
    pub tasks: HashMap<TaskId, Option<TaskResult>>,
    /// Task IDs in insertion order (position = sequence number).
    pub task_order: Vec<TaskId>,
    /// Unix timestamp (milliseconds) when the job was first submitted.
    pub created_at_ms: u64,
    /// Which peer is currently assigned to each in-flight task.  Used for
    /// recovery when a worker disappears.
    pub assignments: HashMap<TaskId, Vec<u8>>,
}

impl JobState {
    /// Construct a new [`JobState`] in `Pending` status with all tasks
    /// recorded as unresolved (`None` results).
    pub fn new(spec: JobSpec, task_ids: &[TaskId], created_at_ms: u64) -> Self {
        let mut tasks = HashMap::new();
        for task_id in task_ids {
            tasks.insert(*task_id, None);
        }

        return Self {
            spec,
            status: JobStatus::Pending,
            tasks,
            task_order: task_ids.to_vec(),
            created_at_ms,
            assignments: HashMap::new(),
        };
    }
}
