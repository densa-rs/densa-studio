//! Sample data to explore without data of one's own: a service's web requests.

use std::error::Error;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use parquet::arrow::ArrowWriter;

use crate::config;

/// The million sample requests as a Parquet file in the user's cache folder: written the first
/// time (calling `writing` first, as it takes a moment), reused after.
pub(crate) fn path(writing: impl FnOnce()) -> Result<PathBuf, Box<dyn Error>> {
    let folder = config::cache_folder().ok_or("no cache folder to write the sample in")?;
    let path = folder.join("requests.parquet");
    if path.exists() {
        return Ok(path);
    }
    writing();
    std::fs::create_dir_all(&folder)?;
    // A file of this process's own, so two Studios writing at once do not mix their rows;
    // the first to finish publishes it.
    let partial = config::partial(&path);
    let written = write(&partial);
    let published = written.and_then(|()| Ok(std::fs::rename(&partial, &path)?));
    let _ = std::fs::remove_file(&partial);
    match published {
        Ok(()) => Ok(path),
        // Another Studio got there first.
        Err(_) if path.exists() => Ok(path),
        Err(error) => Err(error),
    }
}

fn write(file: &Path) -> Result<(), Box<dyn Error>> {
    const BATCH: i64 = 65_536;
    let out = OpenOptions::new().write(true).create_new(true).open(file)?;
    let first = samples::requests(0, BATCH)?;
    let mut writer = ArrowWriter::try_new(out, first.schema(), None)?;
    writer.write(&first)?;
    for start in (BATCH..samples::REQUESTS).step_by(65_536) {
        let end = (start + BATCH).min(samples::REQUESTS);
        writer.write(&samples::requests(start, end)?)?;
    }
    writer.close()?;
    Ok(())
}
