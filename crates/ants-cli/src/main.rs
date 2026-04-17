//! `ants` command-line entrypoint.
//!
//! Subcommands (`run local`, `join`, `submit`, …) will be wired up alongside
//! the milestones described in `PROJECT.md`.

fn main() {
    let version = env!("CARGO_PKG_VERSION");
    println!("ants {version} — workspace bootstrap (no subcommands yet)");
    println!("linked core crate: {}", ants_core::CRATE_NAME);
}
