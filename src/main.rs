//! densa studio: open Parquet and CSV files and explore them at once, in grids that fill as
//! the files load, with SQL over all of them and a chart of the result, in a workspace whose
//! arrangement is kept between runs.
//!
//! `densa-studio [FILE...]`, `--sample` to open a million sample web requests, `--query SQL`
//! to run once the files have loaded, `--light` for the light theme, `--fresh` to ignore the
//! saved arrangement, `--seconds N` to close after N seconds.

mod args;
mod chart;
mod config;
mod query;
mod sample;
mod source;
mod studio;

use std::error::Error;
use std::time::{Duration, Instant};

use densa_window::WindowOptions;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args = args::parse(&args).map_err(|e| format!("{e}\n{}", args::USAGE))?;
    let mut files = args.files;
    if args.sample {
        files.push(sample::path(|| {
            eprintln!("Writing the sample requests (once)...")
        })?);
    }
    let studio = studio::Studio::new(studio::Start {
        files,
        query: args.query,
        light: args.light,
        fresh: args.fresh,
        close_at: args
            .seconds
            .map(|s| Instant::now() + Duration::from_secs(s)),
    })?;
    densa_window::run(
        WindowOptions::new("densa studio").with_size(1280.0, 800.0),
        studio,
    )?;
    Ok(())
}
