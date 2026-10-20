//! The command line: pack, unpack, verify and stats, over many shards on a pool of workers.

use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use serde_json::Value;

use pairpack::shard;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(version, about = "Store a string column of a parquet shard as edits of another string column.")]
struct Cli {
    /// The number of worker threads. The shards run in parallel, and the rows of each row group too.
    #[arg(long, default_value_t = 8, global = true)]
    workers: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Write each shard to the output directory with the derived column stored as edits, and verify every row.
    Pack {
        /// The column that stays in full.
        #[arg(long)]
        keep: String,
        /// The column that the edits rebuild from the kept column.
        #[arg(long)]
        derive: String,
        /// The name of the new edits column.
        #[arg(long, default_value = "edits")]
        edits: String,
        /// The directory for the packed shards. It must not hold a shard of the same name.
        #[arg(long)]
        out: PathBuf,
        shards: Vec<PathBuf>,
    },
    /// Write each packed shard to the output directory with the derived column in full, and verify every row.
    Unpack {
        #[arg(long)]
        out: PathBuf,
        shards: Vec<PathBuf>,
    },
    /// Check each packed shard against the shard of the same name in the original directory, row by row.
    Verify {
        #[arg(long)]
        original: PathBuf,
        shards: Vec<PathBuf>,
    },
    /// Give the rows, the bytes and the bytes of each column of each shard.
    Stats { shards: Vec<PathBuf> },
}

/// Run `job` over every shard on the pool, print each report as one JSON line as it finishes, and fail if one shard
/// failed.
fn run<F>(shards: &[PathBuf], job: F) -> Result<()>
where
    F: Fn(&PathBuf) -> Result<Value> + Sync,
{
    if shards.is_empty() {
        bail!("no shards given");
    }
    let out = Mutex::new(());
    let failed: Vec<String> = shards
        .par_iter()
        .with_max_len(1)
        .filter_map(|path| {
            let line = match job(path) {
                Ok(report) => report.to_string(),
                Err(e) => {
                    serde_json::json!({"shard": path.display().to_string(), "error": format!("{e:#}")}).to_string()
                }
            };
            let _guard = out.lock().expect("the lock is free");
            println!("{line}");
            line.contains("\"error\"").then(|| path.display().to_string())
        })
        .collect();
    if !failed.is_empty() {
        bail!("{} of {} shards failed: {}", failed.len(), shards.len(), failed.join(", "));
    }
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    rayon::ThreadPoolBuilder::new().num_threads(cli.workers.max(1)).build_global()?;
    match cli.command {
        Command::Pack { keep, derive, edits, out, shards } => {
            std::fs::create_dir_all(&out)?;
            run(&shards, |p| shard::pack(p, &out, &keep, &derive, &edits))
        }
        Command::Unpack { out, shards } => {
            std::fs::create_dir_all(&out)?;
            run(&shards, |p| shard::unpack(p, &out))
        }
        Command::Verify { original, shards } => run(&shards, |p| {
            let name = p.file_name().ok_or_else(|| anyhow::anyhow!("{} has no file name", p.display()))?;
            shard::verify(p, &original.join(name))
        }),
        Command::Stats { shards } => run(&shards, |p| shard::stats(p)),
    }
}
