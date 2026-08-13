use clap::Parser;
use doradb_bench::cli::Cli;
use doradb_bench::error::Result;
use doradb_bench::plan_executor::execute_plan;
use std::process::exit;

fn main() {
    if let Err(err) = execute(Cli::parse()) {
        eprintln!("error: {err}");
        exit(1);
    }
}

fn execute(cli: Cli) -> Result<()> {
    smol::block_on(execute_plan(cli.root, cli.plan))
}
