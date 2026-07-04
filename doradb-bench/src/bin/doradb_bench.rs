use clap::Parser;
use doradb_bench::cli::{Cli, Command};
use doradb_bench::error::Result;
use doradb_bench::runner::{cleanup, prepare, run_load};
use std::env::args_os;
use std::process::exit;

fn main() {
    if let Err(err) = execute(Cli::parse()) {
        eprintln!("error: {err}");
        exit(1);
    }
}

fn execute(cli: Cli) -> Result<()> {
    let command_context = args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let storage_root = cli.resolve_root_from_env()?;
    smol::block_on(async {
        match cli.command {
            Command::Prepare(args) => prepare(storage_root, args).await,
            Command::Run(args) => run_load(storage_root, args, &command_context).await,
            Command::Cleanup => cleanup(storage_root).await,
        }
    })
}
