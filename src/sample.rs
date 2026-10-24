//! Sample data to explore without data of one's own: a service's web requests, and the
//! sites of the sample sensors.

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

/// Where the sample sensors are: each site's region, height and the temperature it alarms
/// at, to join their readings to.
const SITES: &str = "site,region,elevation_m,alarm_celsius
north,upland,420,30
south,coast,12,34
east,coast,30,33
west,upland,380,31
harbor,coast,2,35
airfield,plain,95,32
";

/// The sample sensors' sites as a CSV file in the user's cache folder, written when it is
/// not there as it should be.
pub(crate) fn sites() -> Result<PathBuf, Box<dyn Error>> {
    let folder = config::cache_folder().ok_or("no cache folder to write the sample in")?;
    let path = folder.join("sites.csv");
    if std::fs::read_to_string(&path).is_ok_and(|text| text == SITES) {
        return Ok(path);
    }
    std::fs::create_dir_all(&folder)?;
    let partial = config::partial(&path);
    let written = std::fs::write(&partial, SITES).and_then(|()| std::fs::rename(&partial, &path));
    let _ = std::fs::remove_file(&partial);
    match written {
        Ok(()) => Ok(path),
        // Another Studio got there first.
        Err(_) if path.exists() => Ok(path),
        Err(error) => Err(error.into()),
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
