use clap::Parser;
use doradb_bench::cli::{Cli, Command};
use doradb_bench::error::{BenchError, Result};
use doradb_bench::plan_executor::execute_plan;
use doradb_bench::runner::{cleanup, prepare, run_workload};
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
    let Cli {
        root: storage_root,
        plan,
        command,
    } = cli;
    smol::block_on(async {
        match (plan, command) {
            (Some(plan), None) => execute_plan(storage_root, plan).await,
            (None, Some(Command::Prepare(args))) => prepare(storage_root, args).await,
            (None, Some(Command::Run { workload })) => {
                run_workload(storage_root, workload, &command_context).await
            }
            (None, Some(Command::Cleanup)) => cleanup(storage_root).await,
            _ => Err(BenchError::Message(
                "exactly one of --plan or a lifecycle command is required".to_owned(),
            )),
        }
    })
}
