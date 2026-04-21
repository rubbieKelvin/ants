//! `ants` command-line entrypoint.
//!
//! Milestone 1 exposes a single subcommand, `ants node start`, which boots a
//! long-running libp2p node that announces itself via mDNS and exchanges
//! ping/pong messages with any peer it discovers on the local network.
//! Additional subcommands land with their respective milestones.

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
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Start a long-running node that participates in mDNS discovery and
    /// answers ping/pong requests.
    Start {
        /// Multiaddr(s) to listen on. Can be repeated. Defaults to one
        /// IPv4 TCP socket on an OS-assigned port.
        #[arg(long = "listen", value_name = "MULTIADDR")]
        listen: Vec<Multiaddr>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        None => {
            print_banner();
            Ok(())
        }
        Some(Command::Node {
            command: NodeCommand::Start { listen },
        }) => {
            let cfg = NodeConfig { listen_on: listen };
            run_node(cfg).await?;
            Ok(())
        }
    }
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
