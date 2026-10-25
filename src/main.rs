//! densa studio: open Parquet and CSV files, WebSocket streams and Arrow Flight services and
//! explore them at once, in grids that keep up as rows come, with SQL over all of them and a
//! chart of the result, in a workspace whose arrangement is kept between runs.
//!
//! `densa-studio [FILE, ws:// or grpc:// URL...]`, `--sample` to open sample web requests,
//! sites, sensor streams (one joined to the sites) and a Flight service's tables, `--query SQL`
//! to run once the files have loaded, `--live` to run it again each second while sources
//! change, `--light` for the light theme, `--fresh` to ignore the saved arrangement,
//! `--seconds N` to close after N seconds. How to use it: <https://densa.dev/studio/>.

mod args;
mod chart;
mod config;
mod counts;
mod flight;
mod join;
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
        sources.push(sample::sites()?.display().to_string());
        let readings = samples::readings_server()?;
        // Every reading of the last minute, counted per second, then each sensor's latest
        // with its site's columns (in front, as its values change in place).
        sources.push(format!("{readings}#time=time&window=1m&per=1s&name=recent"));
        sources.push(format!("{readings}#key=sensor&time=time&join=site:sites"));
    }
    let studio = studio::Studio::new(studio::Start {
        sources,
        query: args.query,
        live: args.live,
        light: args.light,
        fresh: args.fresh,
        paged: args.paged,
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
