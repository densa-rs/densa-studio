//! densa studio: open Parquet and CSV files and explore them at once, in grids that fill as
//! the files load, with SQL over all of them and a chart of the result, in a workspace whose
//! arrangement is kept between runs.
//!
//! `densa-studio [FILE or ws://URL...]`, `--sample` to open a million sample web requests and
//! a local stream of sensor readings, `--query SQL` to run once the files have loaded, `--live`
//! to run it again each second while sources change, `--light` for the light theme, `--fresh`
//! to ignore the saved arrangement, `--seconds N` to close after N seconds.

mod args;
mod chart;
mod config;
mod flight;
mod listing;
mod query;
mod sample;
mod source;
mod stream;
mod studio;
mod time;

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
    let mut sources = args.sources;
    if args.sample {
        let requests = sample::path(|| eprintln!("Writing the sample requests (once)..."))?;
        sources.push(requests.display().to_string());
        let flight = samples::flight_server()?;
        sources.push(format!("{flight}#tables"));
        let readings = samples::readings_server()?;
        // Every reading of the last minute, then each sensor's latest (in front, as its
        // values change in place).
        sources.push(format!("{readings}#time=time&window=1m&name=recent"));
        sources.push(format!("{readings}#key=sensor&time=time"));
    }
    let studio = studio::Studio::new(studio::Start {
        sources,
        query: args.query,
        live: args.live,
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
