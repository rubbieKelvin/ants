//! Job wire protocol types carried on the `/ants/job/1.0.0` request-response
//! endpoint.
//!
//! [`TaskRequest`] and [`TaskResponse`] are cbor-serialised by libp2p's
//! request/response behaviour.

use ants_core::job::{JobId, JobSpec, JobStatus, Task, TaskId, TaskResult};
use serde::{Deserialize, Serialize};

/// An outbound or inbound job-protocol message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskRequest {
    /// Worker is requesting up to `count` pending tasks from the orchestrator.
    AssignTasks { count: u32 },
    /// Worker submits the result of a completed task.
    SubmitTaskResult { task_id: TaskId, result: TaskResult },
    /// User submits a new job for the orchestrator to manage.
    SubmitJob { spec: JobSpec },
    /// Queries the status of a previously submitted job.
    QueryJobStatus { job_id: JobId },
}

/// Reply sent in response to a [`TaskRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskResponse {
    /// Tasks assigned to the requesting worker.
    Tasks(Vec<Task>),
    /// Acknowledgement that a result was stored.
    Accepted,
    /// A new job was created with this ID.
    JobCreated(JobId),
    /// The requested job's current status (or `None` if not found).
    JobStatus(Option<JobStatus>),
    /// An error message for the requestor.
    Error(String),
}
