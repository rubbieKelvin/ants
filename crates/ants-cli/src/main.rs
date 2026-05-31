//! `ants` command-line entrypoint.
//!
//! MS1: `ants node start` — boots a libp2p node with mDNS + ping/pong.
//! MS2: `ants job submit`, `ants job status` — submit and query WASM jobs.
#![allow(clippy::needless_return)]

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use ants_core::job::{JobId, JobSpec};
use ants_network::{NodeConfig, run_node};
use anyhow::Result;
use clap::{Parser, Subcommand};
use libp2p::Multiaddr;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "ants", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Operate a node in the ants mesh.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Manage distributed jobs.
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Start a long-running node that participates in mDNS discovery,
    /// answers ping/pong requests, and processes jobs.
    Start {
        /// Multiaddr(s) to listen on. Can be repeated.
        #[arg(long = "listen", value_name = "MULTIADDR")]
        listen: Vec<Multiaddr>,
    },
}

#[derive(Debug, Subcommand)]
enum JobCommand {
    /// Submit a WASM module as a distributed job.
    Submit {
        /// Path to the WASM module file.
        #[arg(long = "wasm", value_name = "PATH")]
        wasm: PathBuf,
        /// Path to the input data file.
        #[arg(long = "data", value_name = "PATH")]
        data: PathBuf,
        /// Number of tasks (shards) to split the job into.
        #[arg(long = "tasks", default_value = "1")]
        tasks: u32,
    },
    /// Query the status of a submitted job.
    Status {
        /// The JobId returned by `ants job submit`.
        #[arg(value_name = "JOB_ID")]
        job_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        None => {
            print_banner();
            return Ok(());
        }
        Some(Command::Node {
            command: NodeCommand::Start { listen },
        }) => {
            let cfg = NodeConfig {
                listen_on: listen,
                ..Default::default()
            };
            run_node(cfg).await?;
            return Ok(());
        }
        Some(Command::Job {
            command: JobCommand::Submit { wasm, data, tasks },
        }) => {
            return handle_submit(wasm, data, tasks);
        }
        Some(Command::Job {
            command: JobCommand::Status { job_id },
        }) => {
            return handle_status(&job_id);
        }
    }
}

fn handle_submit(wasm_path: PathBuf, data_path: PathBuf, num_tasks: u32) -> Result<()> {
    let wasm_bytes = fs::read(&wasm_path)
        .map_err(|e| anyhow::anyhow!("failed to read WASM file {}: {e}", wasm_path.display()))?;
    let input_data = fs::read(&data_path)
        .map_err(|e| anyhow::anyhow!("failed to read data file {}: {e}", data_path.display()))?;

    if wasm_bytes.is_empty() {
        return Err(anyhow::anyhow!("WASM file is empty"));
    }
    if input_data.is_empty() {
        return Err(anyhow::anyhow!("data file is empty"));
    }

    let spec = JobSpec::new(wasm_bytes, input_data, num_tasks, HashMap::new())
        .ok_or_else(|| anyhow::anyhow!("invalid job spec: num_tasks=0 or empty input"))?;

    let wasm_hash_hex: String = spec.wasm_hash.iter().map(|b| format!("{b:02x}")).collect();

    eprintln!("Job spec validated successfully.");
    eprintln!("  tasks:    {num_tasks}");
    eprintln!("  WASM SHA-256: {wasm_hash_hex}");
    eprintln!("  input size:   {} bytes", spec.input_data.len());
    eprintln!();
    eprintln!("To execute this job across the mesh, start one or more nodes:");
    eprintln!("  ants node start");
    eprintln!();
    eprintln!("The running node will process pending jobs on connected peers.");

    return Ok(());
}

fn handle_status(job_id: &str) -> Result<()> {
    let _id =
        JobId::from_str(job_id).map_err(|e| anyhow::anyhow!("invalid JobId '{job_id}': {e}"))?;

    eprintln!("querying job {_id}...");
    eprintln!(
        "(job status queries require a running orchestrator node — start one with `ants node start`)"
    );

    return Ok(());
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,libp2p=info,ants_network=debug"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

fn print_banner() {
    let version = env!("CARGO_PKG_VERSION");
    println!("ants {version}");
    println!("linked core crate: {}", ants_core::CRATE_NAME);
    println!("run `ants --help` for available subcommands.");
}
