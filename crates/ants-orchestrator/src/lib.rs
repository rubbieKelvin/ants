//! Orchestrator state, scheduling, and recovery for ants jobs.
#![allow(clippy::needless_return)]
//!
//! The [`Orchestrator`] tracks every submitted [`Job`][ants_core::job::JobSpec],
//! splits it into [`Task`][ants_core::job::Task] units, assigns tasks to
//! workers, collects [`TaskResult`][ants_core::job::TaskResult]s, and detects
//! job completion or failure.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ants_core::job::{JobId, JobSpec, JobState, JobStatus, Task, TaskId, TaskResult};

pub const CRATE_NAME: &str = "ants-orchestrator";

/// Orchestrator error type.
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

// ── Orchestrator ─────────────────────────────────────────────────────────────

/// Tracks the full lifecycle of all submitted jobs.
pub struct Orchestrator {
    jobs: HashMap<JobId, JobState>,
}

impl Orchestrator {
    /// Create an empty orchestrator.
    pub fn new() -> Self {
        return Self {
            jobs: HashMap::new(),
        };
    }

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

    /// Assign up to `count` pending tasks to the given worker.
    ///
    /// The returned [`Task`]s are removed from the pending pool and recorded
    /// as assigned to `worker_id`.
    pub fn assign_tasks(&mut self, worker_id: &[u8], count: usize) -> Vec<Task> {
        let mut assigned = Vec::new();

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
                if state.assignments.contains_key(task_id) {
                    continue;
                }
                if state.tasks.get(task_id).and_then(|r| r.as_ref()).is_some() {
                    continue;
                }

                let start = seq * chunk_size;
                let end = std::cmp::min(start + chunk_size, state.spec.input_data.len());
                let input_slice = if start < state.spec.input_data.len() {
                    state.spec.input_data[start..end].to_vec()
                } else {
                    vec![]
                };

                let task = Task {
                    task_id: *task_id,
                    job_id: *job_id,
                    wasm_bytes: state.spec.wasm_bytes.clone(),
                    input_slice,
                    seq: seq as u32,
                };

                state.assignments.insert(*task_id, worker_id.to_vec());
                state.tasks.insert(*task_id, None);
                assigned.push(task);
            }
        }

        if !assigned.is_empty() {
            tracing::debug!(
                worker = %hexify(worker_id),
                count = assigned.len(),
                "tasks assigned"
            );
        }

        return assigned;
    }

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
        state.assignments.remove(task_id);
        state.tasks.insert(*task_id, Some(result));

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

    /// Reclaim all tasks assigned to a dead or disconnected worker.
    ///
    /// Clears the assignment records so the tasks become eligible for
    /// reassignment via [`Self::assign_tasks`].
    pub fn recover_tasks(&mut self, worker_id: &[u8]) -> Vec<TaskId> {
        let mut reclaimed = Vec::new();

        for state in self.jobs.values_mut() {
            let to_remove: Vec<TaskId> = state
                .assignments
                .iter()
                .filter(|(_, w)| w.as_slice() == worker_id)
                .map(|(tid, _)| *tid)
                .collect();

            for task_id in &to_remove {
                state.assignments.remove(task_id);
                state.tasks.insert(*task_id, None);
                reclaimed.push(*task_id);
            }
        }

        if !reclaimed.is_empty() {
            tracing::warn!(
                worker = %hexify(worker_id),
                count = reclaimed.len(),
                "reclaimed tasks from dead worker"
            );
        }

        return reclaimed;
    }

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
    fn reject_empty_input() {
        let result = JobSpec::new(b"wasm".to_vec(), vec![], 1, HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn reject_zero_tasks() {
        let result = JobSpec::new(b"wasm".to_vec(), b"data".to_vec(), 0, HashMap::new());
        assert!(result.is_none());
    }
}
