//! Where Studio keeps what it remembers between runs (the window's arrangement) and what it
//! can make again (the sample data).

use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// An absolute path from the environment variable `name`, if it is set to one (relative
/// ones are ignored, as the XDG base directory rules ask).
fn var(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
}

/// The file the arrangement is saved in, in the user's configuration folder; `None` if the
/// system names none.
pub(crate) fn layout_file() -> Option<PathBuf> {
    let base = if cfg!(windows) {
        var("APPDATA")
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| home.join("Library").join("Application Support"))
    } else {
        var("XDG_CONFIG_HOME").or_else(|| var("HOME").map(|home| home.join(".config")))
    };
    Some(base?.join("densa-studio").join("layout.txt"))
}

/// The user's own folder for files Studio can make again; `None` if the system names none.
pub(crate) fn cache_folder() -> Option<PathBuf> {
    let base = if cfg!(windows) {
        var("LOCALAPPDATA")
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| home.join("Library").join("Caches"))
    } else {
        var("XDG_CACHE_HOME").or_else(|| var("HOME").map(|home| home.join(".cache")))
    };
    Some(base?.join("densa-studio"))
}

/// A name beside `file` for this process to write to before moving the result into place.
pub(crate) fn partial(file: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let id = std::process::id();
    file.with_extension(format!("{id}-{nanos}.partial"))
}

/// Writes `text` to `file` (and its folder) through a file beside it, so a crash midway
/// leaves the last one whole.
pub(crate) fn save(file: &Path, text: &str) -> io::Result<()> {
    if let Some(folder) = file.parent() {
        std::fs::create_dir_all(folder)?;
    }
    let partial = partial(file);
    let saved = std::fs::write(&partial, text).and_then(|()| std::fs::rename(&partial, file));
    if saved.is_err() {
        let _ = std::fs::remove_file(&partial);
    }
    saved
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn studio_has_folders_of_its_own() {
        // Every system CI runs on names a configuration and a cache folder.
        let file = layout_file().unwrap();
        assert!(file.ends_with(Path::new("densa-studio").join("layout.txt")));
        assert!(cache_folder().unwrap().ends_with("densa-studio"));
    }

    #[test]
    fn saving_makes_the_folder_and_replaces_the_file() {
        let folder = std::env::temp_dir().join(format!("densa-studio-test-{}", std::process::id()));
        let file = folder.join("nested").join("layout.txt");
        save(&file, "first").unwrap();
        save(&file, "second").unwrap();
        assert_eq!(std::fs::read_to_string(&file).unwrap(), "second");
        let left: Vec<_> = std::fs::read_dir(file.parent().unwrap()).unwrap().collect();
        assert_eq!(left.len(), 1, "no partial file left behind");
        std::fs::remove_dir_all(folder).unwrap();
    }

    #[test]
    fn partial_files_are_this_process_own() {
        let partial = partial(Path::new("/x/layout.txt"));
        let name = partial.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with(&format!("layout.{}-", std::process::id())),
            "{name}"
        );
        assert!(name.ends_with(".partial"));
    }
}
