//! A Parquet file too large to load, read a page at a time: its grid's model, a thread that
//! looks at the file for changes (off the window's thread, which a slow disk or share would
//! hold up), and rows that could not be read asked for again.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use densa_arrow::{FetchError, PagedModel};
use densa_files::{FileError, FilePages, OpenOptions};
use densa_ui::{Ui, WidgetId};
use densa_widgets::DataGrid;

use super::{grouped, row_key};

/// The grid of a file read a page at a time.
pub(crate) type PagedGrid = DataGrid<PagedModel>;

/// A Parquet file whose rows take more than this many bytes uncompressed (its footer says) is
/// read a page at a time instead of loaded: loaded, they would take at least as much memory.
pub(crate) const PAGED_BYTES: u64 = 1 << 30;

/// How often the file is looked at for changes.
const LOOK_EVERY: Duration = Duration::from_secs(1);

/// How long rows that could not be read wait before they are asked for again, the first time
/// and at most (the wait doubles while they still cannot be read).
const RETRY_FIRST: Duration = Duration::from_secs(2);
const RETRY_MOST: Duration = Duration::from_secs(60);

/// A wake for the window, for any thread.
pub(crate) type Wake = Arc<dyn Fn() + Send + Sync>;

/// What the thread looking at the file saw.
enum Seen {
    /// It was written to, its columns the same: count its rows and read them again.
    Changed,
    /// It was written to with other columns: show it anew, its rows numbered in `key`.
    Replaced(FilePages, String),
    /// It could not be looked at (being written, gone, on a share out of reach): why.
    Trouble(String),
    /// It can be looked at again.
    Fine,
}

/// What a step found: nothing, a file to read again (SQL too), or a file to show anew.
pub(crate) enum Step {
    Nothing,
    Changed,
    Replaced(Box<PagedModel>),
}

/// A file read a page at a time, and the thread looking at it.
pub(crate) struct Paged {
    pub(crate) path: PathBuf,
    /// The column numbering its rows.
    pub(crate) key: String,
    wake: Wake,
    seen: Receiver<Seen>,
    /// Dropped with the source: the thread looking at the file then ends.
    _stop: Sender<()>,
    /// Asks the thread to wake the window each time it looks, while rows wait to be asked for
    /// again.
    ticking: Arc<AtomicBool>,
    /// Why the file could not be looked at, last time.
    trouble: Option<String>,
    /// When rows that could not be read are asked for again, the wait after that, and when
    /// they last could not be.
    retry_at: Option<Instant>,
    retry_wait: Duration,
    failed_at: Option<Instant>,
}

impl Paged {
    /// The file at `path` (canonical), whose `columns` are read (without row numbers): the
    /// model of its pages, and the source watching it, waking the window with `wake`.
    pub(crate) fn open(
        path: PathBuf,
        columns: FilePages,
        wake: Wake,
    ) -> Result<(Self, PagedModel), Box<dyn Error>> {
        let key = row_key(&columns.schema());
        let pages = OpenOptions::new().row_numbers(&key).paged(&path)?;
        let model = PagedModel::new(pages.schema(), pages, waking(&wake))?;
        let (sender, seen) = mpsc::channel();
        let (stop, stopped) = mpsc::channel();
        let ticking = Arc::new(AtomicBool::new(false));
        let watcher = Watcher {
            file: columns,
            path: path.clone(),
            sender,
            stopped,
            ticking: Arc::clone(&ticking),
            wake: Arc::clone(&wake),
            troubled: false,
        };
        thread::Builder::new()
            .name("studio file watcher".to_owned())
            .spawn(move || watcher.watch())?;
        let paged = Self {
            path,
            key,
            wake,
            seen,
            _stop: stop,
            ticking,
            trouble: None,
            retry_at: None,
            retry_wait: RETRY_FIRST,
            failed_at: None,
        };
        Ok((paged, model))
    }

    /// Takes in what the thread looking at the file saw, and asks again for rows that could not
    /// be read, once their wait is over. `grid` is the file's grid.
    pub(crate) fn step(&mut self, ui: &mut Ui, grid: WidgetId) -> Result<Step, Box<dyn Error>> {
        let mut step = Step::Nothing;
        while let Ok(seen) = self.seen.try_recv() {
            match seen {
                // A file read anew can be looked at again.
                Seen::Changed => {
                    self.trouble = None;
                    ui.update::<PagedGrid, _>(grid, |grid, cx| grid.model_mut(cx).refresh());
                    if matches!(step, Step::Nothing) {
                        step = Step::Changed;
                    }
                }
                Seen::Replaced(pages, key) => {
                    self.trouble = None;
                    self.key = key;
                    let model = PagedModel::new(pages.schema(), pages, waking(&self.wake))?;
                    step = Step::Replaced(Box::new(model));
                }
                Seen::Trouble(why) => self.trouble = Some(why),
                Seen::Fine => self.trouble = None,
            }
        }
        if matches!(step, Step::Nothing) {
            self.retry(ui, grid);
        }
        Ok(step)
    }

    /// Asks again for rows `grid`'s model could not read (or counts them again, if fewer came
    /// than counted), once their wait is over; the thread looking at the file wakes the window
    /// meanwhile. The wait doubles while they still cannot be read, and starts over once they
    /// have been for as long as the longest wait.
    fn retry(&mut self, ui: &mut Ui, grid: WidgetId) {
        let now = Instant::now();
        let error = ui
            .widget::<PagedGrid>(grid)
            .and_then(|grid| grid.model().error().cloned());
        let error = match error {
            // Its thread is gone: nothing asks again.
            None | Some(FetchError::Stopped { .. }) => {
                if self.failed_at.is_some_and(|at| now - at >= RETRY_MOST) {
                    self.retry_wait = RETRY_FIRST;
                    self.failed_at = None;
                }
                self.retry_at = None;
                // Relaxed: a hint for the thread looking at the file, read on its next look.
                self.ticking.store(false, Ordering::Relaxed);
                return;
            }
            Some(error) => error,
        };
        self.failed_at = Some(now);
        // Relaxed: as above.
        self.ticking.store(true, Ordering::Relaxed);
        let at = *self.retry_at.get_or_insert(now + self.retry_wait);
        if now < at {
            return;
        }
        ui.update::<PagedGrid, _>(grid, |grid, cx| {
            let model = grid.model_mut(cx);
            match error {
                FetchError::Short { .. } => model.refresh(),
                _ => model.retry(),
            }
        });
        self.retry_wait = (self.retry_wait * 2).min(RETRY_MOST);
        self.retry_at = None;
    }

    /// Whether how it stands is news for the status bar: its rows being counted, rows that
    /// could not be read, or a file that cannot be looked at.
    pub(crate) fn tells(&self, ui: &Ui, grid: WidgetId) -> bool {
        let model = ui.widget::<PagedGrid>(grid).map(DataGrid::model);
        self.trouble.is_some()
            || model.is_some_and(|model| model.row_count().is_none() || model.error().is_some())
    }

    /// How it stands, for the status bar, its table named `name`.
    pub(crate) fn status(&self, ui: &Ui, grid: WidgetId, name: &str) -> String {
        let Some(grid) = ui.widget::<PagedGrid>(grid) else {
            return format!("{name}: its tab is gone");
        };
        let model = grid.model();
        let rows = match model.row_count() {
            Some(rows) => format!("{} rows", grouped(rows)),
            None => "counting its rows".to_owned(),
        };
        let mut text = format!(
            "{name}: {rows}, read a page at a time (too large to load: sort and filter it with \
             SQL)"
        );
        if let Some(error) = model.error() {
            text.push_str(&format!("; {}", missing(error)));
        }
        if let Some(why) = &self.trouble {
            text.push_str(&format!("; the file cannot be looked at: {why}"));
        }
        text
    }
}

/// Why rows are missing, in the words of an app that asks again by itself.
fn missing(error: &FetchError) -> String {
    match error {
        FetchError::Stopped { .. } => format!("{error}: open the file again"),
        // Its rows are counted again once the file is seen to have changed.
        _ if error.to_string().contains("was written to") => {
            "the file was written to: reading it again".to_owned()
        }
        _ => format!("{error} (asked for again by itself)"),
    }
}

/// A wake for the model's thread.
fn waking(wake: &Wake) -> impl Fn() + Send + 'static {
    let wake = Arc::clone(wake);
    move || wake()
}

/// The thread looking at the file.
struct Watcher {
    /// The file, read without row numbers: to see when it changes, and whether its columns did.
    file: FilePages,
    path: PathBuf,
    sender: Sender<Seen>,
    stopped: Receiver<()>,
    ticking: Arc<AtomicBool>,
    wake: Wake,
    /// The file could not be looked at last time.
    troubled: bool,
}

impl Watcher {
    /// Looks at the file each second until the source is gone, saying what changed and waking
    /// the window for it (and each time, while rows wait to be asked for again).
    fn watch(mut self) {
        loop {
            match self.stopped.recv_timeout(LOOK_EVERY) {
                Err(RecvTimeoutError::Timeout) => {}
                // The source is gone (nothing is ever sent).
                _ => return,
            }
            let seen = self.look();
            let news = seen.is_some();
            if let Some(seen) = seen {
                if self.sender.send(seen).is_err() {
                    return;
                }
            }
            // Relaxed: a hint from the window's thread; a look late by a second is harmless.
            if news || self.ticking.load(Ordering::Relaxed) {
                (self.wake)();
            }
        }
    }

    /// What changed since the last look, if anything.
    fn look(&mut self) -> Option<Seen> {
        let seen = match self.file.reload() {
            Ok(false) => None,
            Ok(true) => Some(Seen::Changed),
            Err(FileError::ColumnsChanged(_)) => Some(match anew(&self.path) {
                Ok((file, pages, key)) => {
                    self.file = file;
                    Seen::Replaced(pages, key)
                }
                Err(error) => Seen::Trouble(error.to_string()),
            }),
            Err(error) => Some(Seen::Trouble(error.to_string())),
        };
        let troubled = matches!(seen, Some(Seen::Trouble(_)));
        let seen = match (self.troubled, troubled, seen) {
            // Fine again: said once.
            (true, false, None) => Some(Seen::Fine),
            (_, _, seen) => seen,
        };
        self.troubled = troubled;
        seen
    }
}

/// The file at `path` opened anew: read without row numbers, and with them in a column it does
/// not have (named the third).
fn anew(path: &Path) -> Result<(FilePages, FilePages, String), FileError> {
    let file = OpenOptions::new().paged(path)?;
    let key = row_key(&file.schema());
    let pages = OpenOptions::new().row_numbers(&key).paged(path)?;
    Ok((file, pages, key))
}
