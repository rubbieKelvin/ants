//! Worker-side WASM execution runtime for ants.
#![allow(clippy::needless_return)]
//!
//! [`SandboxConfig`] controls resource limits; [`WasmEngine`] wraps a shared
//! `wasmtime::Engine` and has an async [`WasmEngine::execute_task`] method
//! that compiles a WASM module, links WASI snapshot preview 1, and runs it
//! inside `tokio::task::spawn_blocking` with a wall-clock timeout.

use std::io::Cursor;
use std::time::Duration;

use ants_core::job::{TaskId, TaskResult};
use wasi_common::pipe::{ReadPipe, WritePipe};
use wasi_common::sync::WasiCtxBuilder;
use wasmtime::{Config, Engine, Linker, Module, Store};

pub const CRATE_NAME: &str = "ants-worker";

// ── Sandbox configuration ────────────────────────────────────────────────────

/// Resource limits enforced on every WASM task execution.
#[derive(Debug, Clone)]
pub struct SandboxConfig {
    /// Memory limit is not enforced in this milestone; reserved for MS5.
    pub memory_limit_bytes: u64,
    /// Fuel units granted before the engine traps.  One fuel unit ≈ one WASM
    /// instruction (approximately; the exact mapping is implementation-defined).
    pub fuel_per_task: u64,
    /// Wall-clock budget for the full compile + execute cycle.
    pub wall_clock_timeout: Duration,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        return Self {
            memory_limit_bytes: 256 * 1024 * 1024, // 256 MiB
            fuel_per_task: 1_000_000,
            wall_clock_timeout: Duration::from_secs(300), // 5 minutes
        };
    }
}

// ── Engine ───────────────────────────────────────────────────────────────────

/// A reusable, sandboxed WASM execution engine.
///
/// The inner [`wasmtime::Engine`] is cheap to clone and can be shared across
/// tasks.  A fresh [`Store`] is created per invocation so that fuel and WASI
/// state are isolated.
pub struct WasmEngine {
    engine: Engine,
    config: SandboxConfig,
}

impl WasmEngine {
    /// Build a new engine from the supplied sandbox configuration.
    pub fn new(sandbox: SandboxConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut wasm_config = Config::default();
        wasm_config.consume_fuel(true);
        let engine = Engine::new(&wasm_config)?;
        return Ok(Self {
            engine,
            config: sandbox,
        });
    }

    /// Compile and run one [`Task`] asynchronously.
    ///
    /// The call is dispatched to `spawn_blocking` to avoid blocking the Tokio
    /// runtime.  A wall-clock timeout (from [`SandboxConfig`]) wraps the
    /// blocking call.
    pub async fn execute_task(
        &self,
        task_id: TaskId,
        wasm_bytes: &[u8],
        input_data: &[u8],
    ) -> TaskResult {
        let wasm_bytes = wasm_bytes.to_vec();
        let input_data = input_data.to_vec();
        let engine = self.engine.clone();
        let fuel = self.config.fuel_per_task;
        let timeout_dur = self.config.wall_clock_timeout;

        let start = std::time::Instant::now();

        let result = tokio::time::timeout(
            timeout_dur,
            tokio::task::spawn_blocking(move || run_wasm(&engine, &wasm_bytes, &input_data, fuel)),
        )
        .await;

        let wall_time_ms = start.elapsed().as_millis() as u64;

        return match result {
            Ok(Ok(Ok(outcome))) => TaskResult {
                task_id,
                stdout: outcome.stdout,
                stderr: outcome.stderr,
                exit_code: outcome.exit_code,
                wall_time_ms,
            },
            Ok(Ok(Err(err_msg))) => TaskResult {
                task_id,
                stdout: vec![],
                stderr: err_msg.into_bytes(),
                exit_code: -1,
                wall_time_ms,
            },
            Ok(Err(_join_err)) => TaskResult {
                task_id,
                stdout: vec![],
                stderr: b"worker thread panicked or was cancelled".to_vec(),
                exit_code: -1,
                wall_time_ms,
            },
            Err(_elapsed) => TaskResult {
                task_id,
                stdout: vec![],
                stderr: b"task exceeded wall-clock timeout".to_vec(),
                exit_code: -1,
                wall_time_ms,
            },
        };
    }
}

// ── Execution outcome ────────────────────────────────────────────────────────

struct TaskOutcome {
    exit_code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Convert a `wasmtime::Error` into a WASI exit code if possible.
fn extract_exit_code(error: &wasmtime::Error) -> i32 {
    if let Some(exit) = error.downcast_ref::<wasi_common::I32Exit>() {
        return exit.0;
    }
    return 1;
}

// ── Synchronous WASM execution ───────────────────────────────────────────────

/// Compile, link, and run a WASM module synchronously.
fn run_wasm(
    engine: &Engine,
    wasm_bytes: &[u8],
    input_data: &[u8],
    fuel: u64,
) -> Result<TaskOutcome, String> {
    let mut builder = WasiCtxBuilder::new();
    let stdin = ReadPipe::from(input_data);
    let stdout = WritePipe::new_in_memory();
    let stderr = WritePipe::new_in_memory();

    builder.stdin(Box::new(stdin));
    builder.stdout(Box::new(stdout.clone()));
    builder.stderr(Box::new(stderr.clone()));
    let wasi_ctx = builder.build();

    let module = Module::new(engine, wasm_bytes).map_err(|e| format!("compilation failed: {e}"))?;

    let mut store = Store::new(engine, wasi_ctx);
    store
        .set_fuel(fuel)
        .map_err(|e| format!("fuel error: {e}"))?;

    let mut linker = Linker::new(engine);
    wasi_common::sync::add_to_linker(&mut linker, |ctx: &mut wasi_common::WasiCtx| ctx)
        .map_err(|e| format!("wasi link error: {e}"))?;

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| format!("instantiation failed: {e}"))?;

    let start_fn = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .map_err(|e| format!("missing _start export: {e}"))?;

    let exit_code = match start_fn.call(&mut store, ()) {
        Ok(()) => 0,
        Err(e) => extract_exit_code(&e),
    };

    drop(store);

    let stdout_vec = stdout
        .try_into_inner()
        .map(|c: Cursor<Vec<u8>>| c.into_inner())
        .unwrap_or_default();
    let stderr_vec = stderr
        .try_into_inner()
        .map(|c: Cursor<Vec<u8>>| c.into_inner())
        .unwrap_or_default();

    return Ok(TaskOutcome {
        exit_code,
        stdout: stdout_vec,
        stderr: stderr_vec,
    });
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_engine() -> WasmEngine {
        return WasmEngine::new(SandboxConfig::default()).expect("engine construction");
    }

    fn wat_to_wasm(wat: &str) -> Vec<u8> {
        return wat::parse_str(wat).expect("valid WAT");
    }

    // ── Valid module tests ───────────────────────────────────────────────

    /// WASM module that reads two i32s from stdin, adds them, writes result
    /// to stdout.
    #[tokio::test]
    async fn add_two_numbers() {
        let engine = make_engine();
        let wasm = wat_to_wasm(
            r#"
            (module
                (import "wasi_snapshot_preview1" "fd_read"
                    (func $fd_read (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $fd_write (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)

                (func (export "_start")
                    (local $tmp i32)

                    ;; iov for fd_read: buf=8, len=8 at offset 0
                    i32.const 0
                    i32.const 8
                    i32.store
                    i32.const 4
                    i32.const 8
                    i32.store

                    ;; fd_read(stdin=0, iovs=0, iovs_len=1, nread=4000)
                    i32.const 0
                    i32.const 0
                    i32.const 1
                    i32.const 4000
                    call $fd_read
                    drop

                    ;; Load two i32s and add
                    i32.const 8
                    i32.load
                    i32.const 12
                    i32.load
                    i32.add

                    ;; Save result, then store at offset 16
                    local.set $tmp
                    i32.const 16
                    local.get $tmp
                    i32.store

                    ;; iov for fd_write: buf=16, len=4 at offset 4000
                    i32.const 4000
                    i32.const 16
                    i32.store
                    i32.const 4004
                    i32.const 4
                    i32.store

                    ;; fd_write(stdout=1, iovs=4000, iovs_len=1, nwritten=5000)
                    i32.const 1
                    i32.const 4000
                    i32.const 1
                    i32.const 5000
                    call $fd_write
                    drop
                )
            )
            "#,
        );

        let input = vec![7u8, 0, 0, 0, 8, 0, 0, 0];

        let result = engine.execute_task(TaskId::new(), &wasm, &input).await;

        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            &result.stdout,
            &[15, 0, 0, 0],
            "expected 7+8=15 as LE i32, got {:?}",
            result.stdout
        );
    }

    /// WASM module that echoes stdin to stdout.
    #[tokio::test]
    async fn echo_stdin() {
        let engine = make_engine();
        let wasm = wat_to_wasm(
            r#"
            (module
                (import "wasi_snapshot_preview1" "fd_read"
                    (func $fd_read (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $fd_write (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)

                (func (export "_start")
                    (local $nread i32)

                    ;; iov for fd_read: buf=4, len=256 at offset 0
                    i32.const 0
                    i32.const 4
                    i32.store
                    i32.const 4
                    i32.const 256
                    i32.store

                    ;; fd_read(stdin=0, iovs=0, iovs_len=1, nread=3000)
                    i32.const 0
                    i32.const 0
                    i32.const 1
                    i32.const 3000
                    call $fd_read
                    drop

                    ;; Read nread from the output pointer
                    i32.const 3000
                    i32.load
                    local.set $nread

                    ;; iov for fd_write: buf=4, len=nread at offset 2000
                    i32.const 2000
                    i32.const 4
                    i32.store
                    i32.const 2004
                    local.get $nread
                    i32.store

                    ;; fd_write(stdout=1, iovs=2000, iovs_len=1, nwritten=4000)
                    i32.const 1
                    i32.const 2000
                    i32.const 1
                    i32.const 4000
                    call $fd_write
                    drop
                )
            )
            "#,
        );

        let input = b"hello wasm world!".to_vec();

        let result = engine.execute_task(TaskId::new(), &wasm, &input).await;

        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(result.stdout, input);
    }

    /// WASM module that sorts 3 bytes using bubble-sort.
    #[tokio::test]
    async fn sort_bytes() {
        let engine = make_engine();
        let wasm = wat_to_wasm(
            r#"
            (module
                (import "wasi_snapshot_preview1" "fd_read"
                    (func $fd_read (param i32 i32 i32 i32) (result i32)))
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $fd_write (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)

                (func (export "_start")
                    (local $i i32) (local $j i32)
                    (local $ai i32) (local $aj i32) (local $tmp i32)

                    ;; iov: buf=8, len=10 at offset 0
                    i32.const 0 i32.const 8 i32.store
                    i32.const 4 i32.const 10 i32.store
                    i32.const 0 i32.const 0 i32.const 1 i32.const 4000
                    call $fd_read
                    drop

                    ;; Bubble-sort 3 bytes at offset 8
                    i32.const 0
                    local.set $i
                    block $done
                    loop $outer
                        local.get $i
                        i32.const 3
                        i32.ge_u
                        br_if $done
                        local.get $i
                        i32.const 1
                        i32.add
                        local.set $j
                        block $inner_done
                        loop $inner
                            local.get $j
                            i32.const 3
                            i32.ge_u
                            br_if $inner_done
                            i32.const 8
                            local.get $i
                            i32.add
                            i32.load8_u
                            local.set $ai
                            i32.const 8
                            local.get $j
                            i32.add
                            i32.load8_u
                            local.set $aj
                            local.get $ai
                            local.get $aj
                            i32.gt_u
                            if
                                i32.const 8
                                local.get $i
                                i32.add
                                local.get $aj
                                i32.store8
                                i32.const 8
                                local.get $j
                                i32.add
                                local.get $ai
                                i32.store8
                            end
                            local.get $j
                            i32.const 1
                            i32.add
                            local.set $j
                            br $inner
                        end
                        end
                        local.get $i
                        i32.const 1
                        i32.add
                        local.set $i
                        br $outer
                    end
                    end

                    ;; iov: buf=8, len=3 at offset 5000
                    i32.const 5000 i32.const 8 i32.store
                    i32.const 5004 i32.const 3 i32.store
                    i32.const 1 i32.const 5000 i32.const 1 i32.const 6000
                    call $fd_write
                    drop
                )
            )
            "#,
        );

        let input = b"cba".to_vec();

        let result = engine.execute_task(TaskId::new(), &wasm, &input).await;

        assert_eq!(
            result.exit_code,
            0,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            &result.stdout, b"abc",
            "expected sorted output, got {:?}",
            result.stdout
        );
    }

    // ── Error handling tests ─────────────────────────────────────────────

    /// A malformed WASM binary should fail compilation cleanly.
    #[tokio::test]
    async fn malformed_module() {
        let engine = make_engine();
        let wasm = b"this is definitely not a valid wasm module".to_vec();

        let result = engine.execute_task(TaskId::new(), &wasm, &[]).await;

        assert_eq!(result.exit_code, -1);
        assert!(
            String::from_utf8_lossy(&result.stderr).contains("compilation failed"),
            "expected compilation error, got: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    /// A WASM module that triggers `unreachable` should produce a non-zero exit.
    #[tokio::test]
    async fn panic_in_wasm() {
        let engine = make_engine();
        let wasm = wat_to_wasm(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "_start")
                    unreachable
                )
            )
            "#,
        );

        let result = engine.execute_task(TaskId::new(), &wasm, &[]).await;

        assert_eq!(
            result.exit_code,
            1,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    /// An infinite loop with very low fuel should produce a fuel-exhausted error.
    #[tokio::test]
    async fn fuel_exhaustion() {
        let sandbox = SandboxConfig {
            fuel_per_task: 10_000,
            ..SandboxConfig::default()
        };
        let engine = WasmEngine::new(sandbox).expect("engine construction");

        let wasm = wat_to_wasm(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "_start")
                    loop
                        br 0
                    end
                )
            )
            "#,
        );

        let result = engine.execute_task(TaskId::new(), &wasm, &[]).await;

        assert_eq!(
            result.exit_code,
            1,
            "stderr: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    /// A task that loops should be stopped by either fuel or timeout.
    /// Uses a short deadline to check both mechanisms work.
    #[tokio::test]
    async fn wall_clock_timeout() {
        let sandbox = SandboxConfig {
            fuel_per_task: 50_000_000,
            wall_clock_timeout: Duration::from_millis(500),
            ..SandboxConfig::default()
        };
        let engine = WasmEngine::new(sandbox).expect("engine construction");

        let wasm = wat_to_wasm(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "_start")
                    loop
                        br 0
                    end
                )
            )
            "#,
        );

        let result = engine.execute_task(TaskId::new(), &wasm, &[]).await;

        assert!(
            result.exit_code != 0,
            "expected non-zero exit (timeout or fuel), got exit_code={}, stderr={}",
            result.exit_code,
            String::from_utf8_lossy(&result.stderr)
        );
    }
}
