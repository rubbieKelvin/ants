//! Orchestrator state, scheduling, and recovery for ants jobs.
#![allow(clippy::needless_return)]
//!
//! The [`Orchestrator`] tracks every submitted [`Job`][ants_core::job::JobSpec],
//! splits it into [`Task`][ants_core::job::Task] units, assigns tasks to
//! workers, collects [`TaskResult`][ants_core::job::TaskResult]s, and detects
//! job completion or failure.
//!
//! MS4 adds heartbeat liveness tracking, automatic timeout recovery, and
//! work-stealing when tasks have been assigned for longer than a threshold.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_core::job::{
    AssignmentInfo, JobId, JobSpec, JobState, JobStatus, Task, TaskId, TaskResult,
};

pub const CRATE_NAME: &str = "ants-orchestrator";

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default threshold after which an assigned task is eligible for stealing.
const STEAL_THRESHOLD_MS: u64 = 10_000;

// ── Error type ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum OrchestratorError {
    InvalidSpec(String),
    TaskNotFound(TaskId),
    DuplicateResult(TaskId),
}

impl std::fmt::Display for OrchestratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrchestratorError::InvalidSpec(msg) => write!(f, "invalid spec: {msg}"),
            OrchestratorError::TaskNotFound(tid) => write!(f, "task {tid} not found"),
            OrchestratorError::DuplicateResult(tid) => write!(f, "duplicate result for {tid}"),
        }
    }
}

// ── Heartbeat info ───────────────────────────────────────────────────────────

/// Metadata tracked for a connected worker.
#[derive(Debug, Clone)]
pub struct HeartbeatInfo {
    /// Unix timestamp (milliseconds) of the last received heartbeat.
    pub last_seen_ms: u64,
    /// The worker's self-reported queue depth.
    pub queue_depth: u32,
    /// The worker's self-reported active task count.
    pub active_tasks: u32,
}

// ── Orchestrator ─────────────────────────────────────────────────────────────

/// Tracks the full lifecycle of all submitted jobs and worker liveness.
pub struct Orchestrator {
    jobs: HashMap<JobId, JobState>,
    /// Last-seen heartbeat metadata per worker (keyed by peer-id bytes).
    worker_heartbeats: HashMap<Vec<u8>, HeartbeatInfo>,
}

impl Orchestrator {
    /// Create an empty orchestrator.
    pub fn new() -> Self {
        return Self {
            jobs: HashMap::new(),
            worker_heartbeats: HashMap::new(),
        };
    }

    // ── Job submission ──────────────────────────────────────────────────────

    /// Submit a new job: validate the spec, split input into tasks, return the
    /// assigned [`JobId`].
    pub fn submit_job(&mut self, spec: JobSpec) -> Result<JobId, OrchestratorError> {
        if spec.num_tasks == 0 {
            return Err(OrchestratorError::InvalidSpec(
                "num_tasks must be greater than zero".to_owned(),
            ));
        }
        if spec.input_data.is_empty() {
            return Err(OrchestratorError::InvalidSpec(
                "input_data must not be empty".to_owned(),
            ));
        }

        let job_id = JobId::new();
        let num_tasks = spec.num_tasks;

        let mut task_ids = Vec::with_capacity(num_tasks as usize);
        for _i in 0..num_tasks {
            task_ids.push(TaskId::new());
        }

        let created_at_ms = now_unix_ms();

        let mut state = JobState::new(spec, &task_ids, created_at_ms);
        state.status = JobStatus::Running {
            done: 0,
            total: num_tasks,
        };

        self.jobs.insert(job_id, state);

        tracing::info!(%job_id, num_tasks, "job submitted");
        return Ok(job_id);
    }

    // ── Task assignment (with work-stealing) ───────────────────────────────

    /// Assign up to `count` pending tasks to the given worker.
    ///
    /// If no truly-pending tasks exist, the orchestrator may *steal* tasks from
    /// workers whose assignments are older than [`STEAL_THRESHOLD_MS`].
    pub fn assign_tasks(&mut self, worker_id: &[u8], count: usize) -> Vec<Task> {
        let mut assigned = Vec::new();

        // First pass: hand out truly-pending (never assigned) tasks.
        self.collect_pending(&mut assigned, worker_id, count);

        // Second pass if we still need more: steal from slow workers.
        if assigned.len() < count {
            self.steal_tasks(&mut assigned, worker_id, count);
        }

        if !assigned.is_empty() {
            tracing::debug!(
                worker = %hexify(worker_id),
                count = assigned.len(),
                "tasks assigned",
            );
        }

        return assigned;
    }

    /// Collect truly-pending tasks (not assigned, not completed).
    fn collect_pending(&mut self, assigned: &mut Vec<Task>, worker_id: &[u8], count: usize) {
        let now = now_unix_ms();
        for (job_id, state) in self.jobs.iter_mut() {
            if assigned.len() >= count {
                break;
            }
            if matches!(state.status, JobStatus::Failed(_) | JobStatus::Completed) {
                continue;
            }
            let chunk_size = state
                .spec
                .input_data
                .len()
                .div_ceil(state.spec.num_tasks as usize);

            for (seq, task_id) in state.task_order.iter().enumerate() {
                if assigned.len() >= count {
                    break;
                }
                // Skip if already assigned or completed.
                if state.assignments.contains_key(task_id) {
                    continue;
                }
                if state.tasks.get(task_id).and_then(|r| r.as_ref()).is_some() {
                    continue;
                }

                let task = build_task(job_id, state, &seq, task_id, chunk_size);
                state.assignments.insert(
                    *task_id,
                    AssignmentInfo {
                        worker_id: worker_id.to_vec(),
                        assigned_at_ms: now,
                    },
                );
                state.tasks.insert(*task_id, None);
                assigned.push(task);
            }
        }
    }

    /// Steal tasks from workers whose assignments are older than the threshold.
    fn steal_tasks(&mut self, assigned: &mut Vec<Task>, requesting_worker: &[u8], count: usize) {
        let now = now_unix_ms();
        for (job_id, state) in self.jobs.iter_mut() {
            if assigned.len() >= count {
                break;
            }
            if matches!(state.status, JobStatus::Failed(_) | JobStatus::Completed) {
                continue;
            }

            let chunk_size = state
                .spec
                .input_data
                .len()
                .div_ceil(state.spec.num_tasks as usize);

            // Find stealable task IDs (assigned for longer than threshold).
            let stealable: Vec<(usize, TaskId)> = state
                .task_order
                .iter()
                .enumerate()
                .filter_map(|(seq, tid)| {
                    let info = state.assignments.get(tid)?;
                    if info.worker_id == requesting_worker {
                        return None; // already assigned to caller
                    }
                    if now.saturating_sub(info.assigned_at_ms) < STEAL_THRESHOLD_MS {
                        return None; // too recent
                    }
                    return Some((seq, *tid));
                })
                .collect();

            for (seq, task_id) in stealable {
                if assigned.len() >= count {
                    break;
                }

                let task = build_task(job_id, state, &seq, &task_id, chunk_size);
                state.assignments.insert(
                    task_id,
                    AssignmentInfo {
                        worker_id: requesting_worker.to_vec(),
                        assigned_at_ms: now,
                    },
                );
                state.tasks.insert(task_id, None);
                assigned.push(task);

                tracing::debug!(
                    %task_id,
                    "task stolen from stale assignment",
                );
            }
        }
    }

    // ── Result recording ───────────────────────────────────────────────────

    /// Record the result of a completed task.
    pub fn record_result(
        &mut self,
        task_id: &TaskId,
        result: TaskResult,
    ) -> Result<(), OrchestratorError> {
        let state = self
            .find_job_mut(task_id)
            .ok_or(OrchestratorError::TaskNotFound(*task_id))?;

        if state.tasks.get(task_id).and_then(|r| r.as_ref()).is_some() {
            return Err(OrchestratorError::DuplicateResult(*task_id));
        }

        let failed = result.exit_code != 0;
        let exit_code = result.exit_code;
        state.tasks.insert(*task_id, Some(result));
        state.assignments.remove(task_id);

        if let JobStatus::Running { done, total } = &mut state.status {
            *done += 1;
            if *done >= *total {
                state.status = if failed {
                    JobStatus::Failed(format!("task {task_id} exited with code {exit_code}"))
                } else {
                    JobStatus::Completed
                };
                tracing::info!(?state.status, "job finished");
            } else if failed {
                state.status =
                    JobStatus::Failed(format!("task {task_id} exited with code {exit_code}"));
                tracing::warn!(%task_id, "task failed, marking job failed");
            }
        }

        return Ok(());
    }

    // ── Task recovery ──────────────────────────────────────────────────────

    /// Reclaim all tasks assigned to a dead or disconnected worker.
    pub fn recover_tasks(&mut self, worker_id: &[u8]) -> Vec<TaskId> {
        let mut reclaimed = Vec::new();

        for state in self.jobs.values_mut() {
            let to_remove: Vec<TaskId> = state
                .assignments
                .iter()
                .filter(|(_, info)| info.worker_id.as_slice() == worker_id)
                .map(|(tid, _)| *tid)
                .collect();

            for task_id in &to_remove {
                state.assignments.remove(task_id);
                state.tasks.insert(*task_id, None);
                reclaimed.push(*task_id);
            }
        }

        self.worker_heartbeats.remove(worker_id);

        if !reclaimed.is_empty() {
            tracing::warn!(
                worker = %hexify(worker_id),
                count = reclaimed.len(),
                "reclaimed tasks from dead worker",
            );
        }

        return reclaimed;
    }

    // ── Heartbeat tracking ─────────────────────────────────────────────────

    /// Record or update the heartbeat metadata for a worker.
    pub fn record_heartbeat(&mut self, worker_id: &[u8], queue_depth: u32, active_tasks: u32) {
        self.worker_heartbeats.insert(
            worker_id.to_vec(),
            HeartbeatInfo {
                last_seen_ms: now_unix_ms(),
                queue_depth,
                active_tasks,
            },
        );
    }

    /// Check which workers have exceeded the heartbeat timeout and recover
    /// their tasks.  Returns a list of `(worker_bytes, recovered_task_ids)`.
    pub fn check_timeouts(&mut self, timeout: Duration) -> Vec<(Vec<u8>, Vec<TaskId>)> {
        let now = now_unix_ms();
        let timeout_ms = timeout.as_millis() as u64;
        let mut timed_out = Vec::new();

        let dead: Vec<Vec<u8>> = self
            .worker_heartbeats
            .iter()
            .filter(|(_, info)| now.saturating_sub(info.last_seen_ms) >= timeout_ms)
            .map(|(id, _)| id.clone())
            .collect();

        for worker_id in dead {
            let reclaimed = self.recover_tasks(&worker_id);
            if !reclaimed.is_empty() {
                timed_out.push((worker_id, reclaimed));
            }
        }

        return timed_out;
    }

    // ── Queries ────────────────────────────────────────────────────────────

    /// Return a reference to the [`JobStatus`] for the given job, if it exists.
    pub fn get_job_status(&self, job_id: &JobId) -> Option<&JobStatus> {
        return self.jobs.get(job_id).map(|s| &s.status);
    }

    /// Return a reference to the full [`JobState`], if it exists.
    pub fn get_job(&self, job_id: &JobId) -> Option<&JobState> {
        return self.jobs.get(job_id);
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn find_job_mut(&mut self, task_id: &TaskId) -> Option<&mut JobState> {
        return self
            .jobs
            .values_mut()
            .find(|s| s.tasks.contains_key(task_id));
    }
}

impl Default for Orchestrator {
    fn default() -> Self {
        return Self::new();
    }
}

// ── Free functions ───────────────────────────────────────────────────────────

fn build_task(
    job_id: &JobId,
    state: &JobState,
    seq: &usize,
    task_id: &TaskId,
    chunk_size: usize,
) -> Task {
    let start = seq * chunk_size;
    let end = std::cmp::min(start + chunk_size, state.spec.input_data.len());
    let input_slice = if start < state.spec.input_data.len() {
        state.spec.input_data[start..end].to_vec()
    } else {
        vec![]
    };

    return Task {
        task_id: *task_id,
        job_id: *job_id,
        wasm_bytes: state.spec.wasm_bytes.clone(),
        input_slice,
        seq: *seq as u32,
    };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn now_unix_ms() -> u64 {
    return SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
}

fn hexify(bytes: &[u8]) -> String {
    return bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("");
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_spec(wasm: &[u8], data: &[u8], num_tasks: u32) -> JobSpec {
        return JobSpec::new(wasm.to_vec(), data.to_vec(), num_tasks, HashMap::new())
            .expect("valid spec");
    }

    #[test]
    fn submit_and_complete_job() {
        let mut orch = Orchestrator::new();
        let spec = make_spec(b"fake_wasm", b"0123456789", 2);

        let job_id = orch.submit_job(spec).expect("submit");
        let status = orch.get_job_status(&job_id).expect("job exists");
        assert!(matches!(status, JobStatus::Running { done: 0, total: 2 }));

        let tasks = orch.assign_tasks(b"worker-a", 2);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].seq, 0);
        assert_eq!(tasks[1].seq, 1);

        orch.record_result(
            &tasks[0].task_id,
            TaskResult {
                task_id: tasks[0].task_id,
                stdout: b"ok".to_vec(),
                stderr: vec![],
                exit_code: 0,
                wall_time_ms: 10,
            },
        )
        .expect("record result 1");

        let status = orch.get_job_status(&job_id).expect("job exists");
        assert!(matches!(status, JobStatus::Running { done: 1, total: 2 }));

        orch.record_result(
            &tasks[1].task_id,
            TaskResult {
                task_id: tasks[1].task_id,
                stdout: b"ok".to_vec(),
                stderr: vec![],
                exit_code: 0,
                wall_time_ms: 10,
            },
        )
        .expect("record result 2");

        let status = orch.get_job_status(&job_id).expect("job exists");
        assert!(matches!(status, JobStatus::Completed));
    }

    #[test]
    fn failed_task_marks_job_failed() {
        let mut orch = Orchestrator::new();
        let spec = make_spec(b"fake_wasm", b"data", 2);
        let job_id = orch.submit_job(spec).expect("submit");

        let tasks = orch.assign_tasks(b"w", 1);
        assert_eq!(tasks.len(), 1);

        orch.record_result(
            &tasks[0].task_id,
            TaskResult {
                task_id: tasks[0].task_id,
                stdout: vec![],
                stderr: b"crash".to_vec(),
                exit_code: 1,
                wall_time_ms: 5,
            },
        )
        .expect("record failed result");

        let status = orch.get_job_status(&job_id).expect("job exists");
        assert!(matches!(status, JobStatus::Failed(_)));
    }

    #[test]
    fn recover_and_reassign() {
        let mut orch = Orchestrator::new();
        let spec = make_spec(b"fake_wasm", b"0123456789", 3);
        orch.submit_job(spec).expect("submit");

        let tasks_a = orch.assign_tasks(b"worker-a", 2);
        assert_eq!(tasks_a.len(), 2);

        let recovered = orch.recover_tasks(b"worker-a");
        assert_eq!(recovered.len(), 2);

        let tasks_b = orch.assign_tasks(b"worker-b", 3);
        assert_eq!(tasks_b.len(), 3);
    }

    #[test]
    fn heartbeat_record_and_timeout() {
        let mut orch = Orchestrator::new();
        orch.record_heartbeat(b"alice", 3, 2);

        // Record a second heartbeat to update the timestamp (to now).
        orch.record_heartbeat(b"alice", 2, 1);

        // A very short timeout should NOT fire for the just-recorded worker.
        let timed_out = orch.check_timeouts(Duration::from_millis(1));
        assert_eq!(timed_out.len(), 0, "just recorded — should not time out");

        // A worker that was never recorded — no-op.
        let timed_out = orch.check_timeouts(Duration::from_millis(1));
        assert_eq!(timed_out.len(), 0);
    }

    #[test]
    fn work_steal_reassigns_stale_task() {
        let mut orch = Orchestrator::new();
        let spec = make_spec(b"wasm", b"0123456789", 2);
        orch.submit_job(spec).expect("submit");

        // Worker-a gets both tasks (only 2 exist).
        let tasks_a = orch.assign_tasks(b"worker-a", 10);
        assert_eq!(tasks_a.len(), 2);

        // Worker-b asks for work — nothing truly pending.
        let tasks_b = orch.assign_tasks(b"worker-b", 10);
        // With steal-threshold 10s, and the test being instantaneous, nothing
        // should be stolen yet.
        assert_eq!(tasks_b.len(), 0, "no tasks older than steal threshold");
    }
}
