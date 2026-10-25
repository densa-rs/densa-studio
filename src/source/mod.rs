//! A data source: a file read in the background, a live stream, or an Arrow Flight service's
//! data, into a live table shown in a grid of its own and queryable with SQL by its table
//! name.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_files::{Loader, OpenOptions};
use densa_ui::{Ui, WidgetId};
use densa_widgets::{DataGrid, DockArea, DockAt, DockPanel, Panel};

use crate::counts::Counted;
use crate::flight::{Fetch, FlightSpec};
use crate::join::{Joined, Offer, Stepped};
use crate::listing;
use crate::stream::{Arrived, Live, StreamSpec};
use crate::time::TimeSpec;

/// The key column every file gets, numbering its rows as they are read (`row_`, `row__`
/// and so on when the file has a column of that name).
const ROW: &str = "row";

/// Words SQL reserves: a table named after one is given a `_` after it, so queries can name
/// it without quotes.
const RESERVED: &[&str] = &[
    "all",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "cast",
    "check",
    "column",
    "create",
    "cross",
    "current",
    "default",
    "delete",
    "desc",
    "distinct",
    "drop",
    "else",
    "end",
    "except",
    "exists",
    "false",
    "fetch",
    "filter",
    "for",
    "foreign",
    "from",
    "full",
    "group",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "interval",
    "into",
    "is",
    "join",
    "key",
    "lateral",
    "left",
    "like",
    "limit",
    "natural",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "over",
    "partition",
    "primary",
    "range",
    "references",
    "right",
    "row",
    "rows",
    "select",
    "set",
    "some",
    "table",
    "then",
    "to",
    "true",
    "union",
    "unique",
    "update",
    "user",
    "using",
    "values",
    "when",
    "where",
    "window",
    "with",
];

pub(crate) type Grid = DataGrid<LiveModel>;

/// Where a source's rows come from.
enum Feed {
    /// A file read once, to its end.
    File {
        loader: Loader,
        /// The column numbering its rows.
        key: String,
        /// The rows the file says it has, if it says.
        total: Option<u64>,
        done: bool,
        /// Why reading stopped early.
        failed: Option<String>,
    },
    /// A stream that goes on; its table is made when its first rows show its columns.
    Stream { live: Live, started: bool },
    /// A Flight service's data, read to its end; its table is made (anew) when its columns
    /// come.
    Flight { fetch: Fetch, started: bool },
}

/// A file loading (or loaded), or a stream, in a live table.
pub(crate) struct Source {
    /// The name SQL knows it by.
    pub(crate) name: String,
    /// What it was opened from, as given to find it open again: the file as the system
    /// names it, or the stream's URL.
    pub(crate) origin: String,
    /// Its tab in the dock.
    pub(crate) panel: WidgetId,
    pub(crate) grid: WidgetId,
    feed: Feed,
    /// The column of the rows' times, made timestamps as rows arrive, and the window kept
    /// over it: for a remote source that names one.
    time: Option<TimeSpec>,
    /// What went wrong with the last batch's times, if anything: the rows come all the same.
    time_off: Option<String>,
    /// Why the window could not keep the table, if it could not.
    window_off: Option<String>,
    /// The rows counted per bucket of the time column, in a tab of their own: for a remote
    /// source that says `per=`.
    pub(crate) counts: Option<Counted>,
    /// For a list of a Flight SQL service's tables: what was asked, to ask for a table.
    listing: Option<FlightSpec>,
    /// Its join to another open table: for a remote source that says `join=`.
    pub(crate) joined: Option<Joined>,
    /// Studio numbers its rows (it has no key of its own).
    numbered: bool,
    /// A source is joined to this one: the rows its table takes are kept for it.
    pub(crate) watched: bool,
    /// The rows its table took since the sources joined to it last took them.
    arrived: Vec<RecordBatch>,
    /// How many times rows left its table, and were replaced in it: a source joined to it
    /// then looks for rows that left.
    deletions: u64,
    updates: u64,
    /// Rows in batches the table refused, and why the last one was.
    skipped: u64,
    refused: Option<String>,
    /// Rows came in since SQL last saw the table.
    pub(crate) changed: bool,
}

impl Source {
    /// Opens the file at `path` in a new tab of `dock` placed `at`, named `name` for SQL,
    /// and starts reading it, calling `wake` as rows arrive. Its tab is keyed by
    /// its path, so a saved arrangement finds it whatever order sources are opened in.
    pub(crate) fn open_file(
        ui: &mut Ui,
        dock: WidgetId,
        path: &Path,
        name: String,
        at: DockAt,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, Box<dyn Error>> {
        let path = std::fs::canonicalize(path)?;
        // The file's own columns first, to number rows in a column it does not have.
        let columns = OpenOptions::new().open(&path)?.schema();
        let mut key = ROW.to_owned();
        while columns.column_with_name(&key).is_some() {
            key.push('_');
        }
        let reader = OpenOptions::new().row_numbers(&key).open(&path)?;
        let total = reader.total_rows();
        let table = LiveTable::new(reader.schema(), &key)?;
        let title = path
            .file_name()
            .map_or_else(|| name.clone(), |f| f.to_string_lossy().into_owned());
        let origin = path.display().to_string();
        let (panel, grid) = add_tab(ui, dock, table, (&name, &title), &origin, at)?;
        let loader = Loader::spawn(reader, wake)?;
        let feed = Feed::File {
            loader,
            key,
            total,
            done: false,
            failed: None,
        };
        Ok(Self::new(name, origin, panel, grid, feed))
    }

    /// Opens the stream `spec` in a new tab of `dock` placed `at`, named `name` for SQL, and
    /// starts reading it, calling `wake` as rows arrive. Its tab is keyed by its URL.
    pub(crate) fn open_stream(
        ui: &mut Ui,
        dock: WidgetId,
        spec: &StreamSpec,
        name: String,
        at: DockAt,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, Box<dyn Error>> {
        let live = Live::connect(spec, wake)?;
        let table = LiveTable::new(waiting(live.key()), live.key())?;
        let origin = spec.origin();
        let (panel, grid) = add_tab(ui, dock, table, (&name, &spec.title()), &origin, at)?;
        let feed = Feed::Stream {
            live,
            started: false,
        };
        let mut source = Self::new(name, origin, panel, grid, feed);
        source.time = spec.time.clone();
        source.counts = counted(ui, dock, &source)?;
        source.joined = spec.join.clone().map(Joined::new);
        source.numbered = spec.key.is_none();
        // Nothing for SQL until its rows show their columns.
        source.changed = false;
        Ok(source)
    }

    /// Opens the Flight source `spec` in a new tab of `dock` placed `at`, named `name` for
    /// SQL, and starts reading it, calling `wake` as rows arrive. Its tab is keyed by what
    /// it asks for.
    pub(crate) fn open_flight(
        ui: &mut Ui,
        dock: WidgetId,
        spec: &FlightSpec,
        name: String,
        at: DockAt,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, Box<dyn Error>> {
        let fetch = Fetch::connect(spec, wake)?;
        let table = LiveTable::new(waiting(fetch.key()), fetch.key())?;
        let origin = spec.origin();
        let (panel, grid) = add_tab(ui, dock, table, (&name, &spec.title()), &origin, at)?;
        let feed = Feed::Flight {
            fetch,
            started: false,
        };
        let mut source = Self::new(name, origin, panel, grid, feed);
        source.time = spec.time.clone();
        source.counts = counted(ui, dock, &source)?;
        source.listing = spec.lists_tables().then(|| spec.clone());
        source.joined = spec.join.clone().map(Joined::new);
        source.numbered = spec.key.is_none();
        // Nothing for SQL until its columns come.
        source.changed = false;
        Ok(source)
    }

    fn new(name: String, origin: String, panel: WidgetId, grid: WidgetId, feed: Feed) -> Self {
        Self {
            name,
            origin,
            panel,
            grid,
            feed,
            time: None,
            time_off: None,
            window_off: None,
            counts: None,
            listing: None,
            joined: None,
            numbered: true,
            watched: false,
            arrived: Vec::new(),
            deletions: 0,
            updates: 0,
            skipped: 0,
            refused: None,
            changed: true,
        }
    }

    /// Whether the source has all its rows: a file or a Flight result read to its end (or
    /// as far as it could be). A stream never is.
    pub(crate) fn is_done(&self) -> bool {
        match &self.feed {
            Feed::File { done, .. } => *done,
            Feed::Flight { fetch, .. } => fetch.is_done(),
            Feed::Stream { .. } => false,
        }
    }

    /// Whether rows keep coming: a stream.
    pub(crate) fn is_live(&self) -> bool {
        matches!(self.feed, Feed::Stream { .. })
    }

    /// Takes in the rows read so far, until `deadline`, then charts the rows counted per
    /// bucket. Whether some rows, or counts to chart, are left.
    pub(crate) fn take(&mut self, ui: &mut Ui, deadline: Instant) -> bool {
        let rows = self.take_rows(ui, deadline);
        let counts = self.refresh_counts(ui);
        rows || counts
    }

    /// Takes in the rows read so far, until `deadline`. Whether some are left.
    fn take_rows(&mut self, ui: &mut Ui, deadline: Instant) -> bool {
        while Instant::now() < deadline {
            let next = match &mut self.feed {
                Feed::File {
                    loader,
                    done,
                    failed,
                    ..
                } => match loader.try_next() {
                    None => {
                        *done = loader.is_finished();
                        return false;
                    }
                    // Nothing comes after an error.
                    Some(Err(error)) => {
                        *failed = Some(error.to_string());
                        continue;
                    }
                    Some(Ok(batch)) => Arrived::Rows {
                        batch,
                        deleted: None,
                    },
                },
                Feed::Stream { live, .. } => match live.next() {
                    None => return false,
                    Some(arrived) => arrived,
                },
                Feed::Flight { fetch, .. } => match fetch.next() {
                    None => return false,
                    Some(arrived) => arrived,
                },
            };
            let next = self.timed(self.readable(next));
            match next {
                Arrived::Columns(columns) => self.start_table(ui, &columns),
                Arrived::Rows { batch, deleted } => self.upsert(ui, batch, deleted),
                Arrived::Refused { rows, reason } => {
                    self.skipped += rows as u64;
                    self.refused = Some(reason);
                }
            }
        }
        true
    }

    /// `arrived` of a list of tables with the schemas sent with it written out (see
    /// [`listing::readable`]); other sources' as it is.
    fn readable(&self, arrived: Arrived) -> Arrived {
        if self.listing.is_none() {
            return arrived;
        }
        match arrived {
            Arrived::Columns(columns) => Arrived::Columns(listing::readable(columns)),
            Arrived::Rows { batch, deleted } => Arrived::Rows {
                batch: listing::readable(batch),
                deleted,
            },
            refused @ Arrived::Refused { .. } => refused,
        }
    }

    /// `arrived` with its time column made timestamps, if the source names one; what went
    /// wrong is kept for the status bar.
    fn timed(&mut self, arrived: Arrived) -> Arrived {
        let Some(time) = &self.time else {
            return arrived;
        };
        let mut convert = |batch| {
            let (batch, problem) = time.convert(batch);
            self.time_off = problem;
            batch
        };
        match arrived {
            Arrived::Columns(columns) => Arrived::Columns(convert(columns)),
            Arrived::Rows { batch, deleted } => Arrived::Rows {
                batch: convert(batch),
                deleted,
            },
            refused @ Arrived::Refused { .. } => refused,
        }
    }

    /// Puts `batch` in the table (making the table, for a stream's first rows), then deletes
    /// the rows keyed `deleted`.
    fn upsert(
        &mut self,
        ui: &mut Ui,
        batch: RecordBatch,
        deleted: Option<arrow_array::UInt64Array>,
    ) {
        let rows = batch.num_rows() as u64;
        if let Err(error) = self.ready_table(ui, &batch) {
            self.skipped += rows;
            self.refused = Some(error);
            return;
        }
        // Kept for the sources joined to this one, if it takes them.
        let copy = self.watched.then(|| batch.clone());
        let joined = &mut self.joined;
        let (mut taken, mut gone) = (None, 0);
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            let table = grid.model_mut(cx).table_mut();
            taken = Some(match joined.as_mut().and_then(|j| j.join_for(table)) {
                Some(join) => join.upsert_left(table, batch),
                None => table.upsert(batch),
            });
            // Numbered rows too old to keep go even when these rows were refused: their
            // numbers are never offered again.
            if let Some(deleted) = deleted {
                gone = table.delete(&deleted).map_or(0, |changes| changes.deleted);
            }
        });
        if gone > 0 {
            self.deletions += 1;
        }
        match taken {
            Some(Ok(changes)) => {
                self.deletions += u64::from(changes.deleted > 0);
                self.updates += u64::from(changes.updated > 0);
                self.arrived.extend(copy);
                self.changed = true;
            }
            Some(Err(error)) => {
                self.skipped += rows;
                self.refused = Some(error.to_string());
            }
            // The tab is gone.
            None => {}
        }
    }

    /// Makes a stream's table from its first rows' columns, kept to its window if it can be
    /// (again with later rows while the table cannot be made, the reason given each time).
    fn ready_table(&mut self, ui: &mut Ui, batch: &RecordBatch) -> Result<(), String> {
        let (key, started) = match &mut self.feed {
            Feed::Stream { live, started } => (live.key(), started),
            Feed::Flight { fetch, started } => (fetch.key(), started),
            Feed::File { .. } => return Ok(()),
        };
        if *started {
            return Ok(());
        }
        let table = LiveTable::new(batch.schema(), key).map_err(|e| e.to_string())?;
        *started = true;
        // A join made notices the table is not its own, and is made anew.
        self.install(ui, table);
        Ok(())
    }

    /// Shows `table` (a table of this source's made anew) in the grid, kept to the window
    /// and counted if the source says so.
    fn install(&mut self, ui: &mut Ui, mut table: LiveTable) {
        if let Some(time) = &self.time {
            // A window that cannot apply now never will (the columns are settled): the rows
            // are shown without it, and the status bar says why.
            self.window_off = time.apply(&mut table).err();
        }
        if let Some(counts) = &mut self.counts {
            counts.count(ui, &mut table);
        }
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            *grid.model_mut(cx).table_mut() = table
        });
    }

    /// Whether its table has its columns: a remote source's is made with its first rows.
    fn is_started(&self) -> bool {
        !matches!(
            self.feed,
            Feed::Stream { started: false, .. } | Feed::Flight { started: false, .. }
        )
    }

    /// The column that keys its rows.
    fn key(&self) -> &str {
        key_of(&self.feed)
    }

    /// What it offers the sources joined to it for this frame: the rows its table took
    /// since the last one, and how its table stands.
    pub(crate) fn offer(&mut self, ui: &Ui) -> Offer {
        let table = ui
            .widget::<Grid>(self.grid)
            .map(|grid| grid.model().table());
        let started = self.is_started();
        Offer {
            instance: table.filter(|_| started).map(LiveTable::instance),
            schema: table.map_or_else(|| Arc::new(Schema::empty()), |t| t.schema().clone()),
            arrived: std::mem::take(&mut self.arrived),
            deletions: self.deletions,
            updates: self.updates,
            key: self.key().to_owned(),
            numbered: self.numbered,
            joined: self.joined.is_some(),
        }
    }

    /// Takes in what the table it is joined to offers (`None`: no table of that name is
    /// open; `itself`: it names its own), and `all` that table's rows when the join asked
    /// for them; shows the joined rows once the join is made.
    pub(crate) fn link(
        &mut self,
        ui: &mut Ui,
        offer: Option<&Offer>,
        all: Option<Vec<RecordBatch>>,
        itself: bool,
    ) {
        if !self.is_started() {
            return;
        }
        let key = key_of(&self.feed);
        let Some(joined) = &mut self.joined else {
            return;
        };
        if itself {
            joined.refuse("a table cannot be joined to itself");
            return;
        }
        let Some(ready) = joined.ready(offer) else {
            return;
        };
        if joined.is_idle(ready.0, ready.1, all.is_some()) {
            return;
        }
        let mut stepped = Stepped::Nothing;
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            stepped = joined.step(grid.model_mut(cx).table_mut(), key, ready, all);
        });
        match stepped {
            Stepped::Nothing => {}
            Stepped::Changed => self.changed = true,
            Stepped::Replaced(table) => {
                self.install(ui, *table);
                self.changed = true;
            }
        }
    }

    /// A table of `columns`, made anew: a Flight result starts (over).
    fn start_table(&mut self, ui: &mut Ui, columns: &RecordBatch) {
        let key = match &mut self.feed {
            Feed::Flight { fetch, started } => {
                *started = false;
                fetch.key().to_owned()
            }
            _ => return,
        };
        // What was said of the last try is old news.
        self.skipped = 0;
        self.refused = None;
        self.window_off = None;
        if let Some(counts) = &mut self.counts {
            counts.wait(ui);
        }
        if let Err(error) = self.ready_table(ui, columns) {
            // The last try's rows go all the same: a table of the key alone, until columns
            // that can make one come.
            if let Ok(table) = LiveTable::new(waiting(&key), &key) {
                ui.update::<Grid, _>(self.grid, |grid, cx| {
                    *grid.model_mut(cx).table_mut() = table
                });
            }
            self.refused = Some(error);
        }
        self.changed = true;
    }

    /// Charts the rows counted per bucket, if they are and they changed. Whether changed
    /// counts are left to chart at a later frame.
    fn refresh_counts(&mut self, ui: &mut Ui) -> bool {
        let Some(counts) = &mut self.counts else {
            return false;
        };
        let Some(grid) = ui.widget::<Grid>(self.grid) else {
            return false;
        };
        let read = counts.read(grid.model().table());
        counts.show(ui, read)
    }

    /// Whether the first query should wait for this source: a file or a Flight result being
    /// read, unless a read failed and is being tried again (a service not there).
    pub(crate) fn blocks_query(&self) -> bool {
        match &self.feed {
            Feed::File { done, .. } => !done,
            Feed::Flight { fetch, .. } => !fetch.is_done() && !fetch.has_retried(),
            Feed::Stream { .. } => false,
        }
    }

    /// For a list of tables, the table in its selected row, to open: the same service and
    /// headers.
    pub(crate) fn selected_table(&self, ui: &Ui) -> Option<FlightSpec> {
        let listing = self.listing.as_ref()?;
        let grid = ui.widget::<Grid>(self.grid)?;
        let model = grid.model();
        let at = model.table().row_of(model.record(grid.selected()?)?)?;
        let (catalog, schema, table) = listed(&model.table().batches()[at.batch], at.row)?;
        Some(listing.table(catalog.as_deref(), schema.as_deref(), &table))
    }

    /// The rows taken in so far.
    pub(crate) fn rows(&self, ui: &Ui) -> usize {
        ui.widget::<Grid>(self.grid)
            .map_or(0, |grid| grid.model().table().len())
    }

    /// How the source stands, for the status bar.
    pub(crate) fn status(&self, ui: &Ui) -> String {
        let rows = grouped(self.rows(ui) as u64);
        let mut text = match &self.feed {
            Feed::File { done: true, .. } => format!("{}: {rows} rows", self.name),
            Feed::File {
                total: Some(total), ..
            } => format!("{}: reading, {rows} of {}", self.name, grouped(*total)),
            Feed::File { .. } => format!("{}: reading, {rows} rows", self.name),
            Feed::Stream { live, .. } => format!("{}: {}, {rows} rows", self.name, live.status()),
            Feed::Flight { fetch, .. } if self.listing.is_some() => {
                let mut text = format!(
                    "{}: {}, {rows} tables (Enter opens the selected one)",
                    self.name,
                    fetch.status()
                );
                if let Some(why) = fetch.without_columns() {
                    text.push_str(&format!("; their columns were refused: {why}"));
                }
                text
            }
            Feed::Flight { fetch, .. } => {
                format!("{}: {}, {rows} rows", self.name, fetch.status())
            }
        };
        let mut notes = Vec::new();
        if let Some(off) = &self.time_off {
            notes.push(format!("times: {off}"));
        }
        match (
            &self.window_off,
            self.time.as_ref().and_then(TimeSpec::describe),
        ) {
            (Some(off), _) => notes.push(format!("window off: {off}")),
            (None, Some(window)) => notes.push(window),
            (None, None) => {}
        }
        if let Some(off) = self.counts.as_ref().and_then(|c| c.off.as_ref()) {
            notes.push(format!("counts off: {off}"));
        }
        if let Some(joined) = &self.joined {
            notes.push(joined.describe());
        }
        if !notes.is_empty() {
            text.push_str(&format!(" ({})", notes.join("; ")));
        }
        if let Some(refused) = &self.refused {
            match self.skipped {
                0 => text.push_str(&format!(", refused: {refused}")),
                rows => text.push_str(&format!(", {} skipped ({refused})", grouped(rows))),
            }
        }
        if let Feed::File {
            failed: Some(failed),
            ..
        } = &self.feed
        {
            text.push_str(&format!(", reading stopped: {failed}"));
        }
        text
    }

    /// The live rows as they are now, for SQL.
    pub(crate) fn snapshot(&self, ui: &Ui) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
        if let Feed::Stream { started: false, .. } | Feed::Flight { started: false, .. } = self.feed
        {
            return Err("no rows yet".to_owned());
        }
        let grid = ui.widget::<Grid>(self.grid).ok_or("its tab is gone")?;
        let table = grid.model().table();
        let batches = table.snapshot().map_err(|e| e.to_string())?;
        Ok((table.schema().clone(), batches))
    }
}

/// The column that keys the rows of `feed`.
fn key_of(feed: &Feed) -> &str {
    match feed {
        Feed::File { key, .. } => key,
        Feed::Stream { live, .. } => live.key(),
        Feed::Flight { fetch, .. } => fetch.key(),
    }
}

/// The catalog, schema and name of the table in `row` of a list of tables (as Flight SQL
/// lists them); `None` without a name. Values of any type are read as text.
pub(crate) fn listed(
    batch: &RecordBatch,
    row: usize,
) -> Option<(Option<String>, Option<String>, String)> {
    let text = |name: &str| -> Option<String> {
        let column = batch.column_by_name(name)?.slice(row, 1);
        let column = arrow_cast::cast(&column, &DataType::Utf8).ok()?;
        let column = column.as_any().downcast_ref::<arrow_array::StringArray>()?;
        column.is_valid(0).then(|| column.value(0).to_owned())
    };
    let table = text("table_name")?;
    Some((text("catalog_name"), text("db_schema_name"), table))
}

/// A source's tab title: the name SQL knows its table by, then `title` (its file, or its
/// URL's host and what it asks for), so the name stays in view when tabs shrink from the
/// right; `title` alone when it starts with the name already (`trips.parquet`).
pub(crate) fn tab_title(name: &str, title: &str) -> String {
    let says_it = title
        .strip_prefix(name)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'));
    match says_it {
        true => title.to_owned(),
        false => format!("{name} · {title}"),
    }
}

/// Adds a tab showing `table` to `dock`, placed `at`, keyed by `origin`, titled by the
/// table's SQL name and `title`.
fn add_tab(
    ui: &mut Ui,
    dock: WidgetId,
    table: LiveTable,
    (name, title): (&str, &str),
    origin: &str,
    at: DockAt,
) -> Result<(WidgetId, WidgetId), Box<dyn Error>> {
    let model = LiveModel::new(table)?;
    let spec = DockPanel::new(tab_title(name, title)).with_key(format!("source:{origin}"));
    let panel = DockArea::add_panel(ui, dock, Panel::new(), spec, at)?;
    let grid = ui.add(
        panel,
        Grid::new(model).with_frozen_columns(1),
        Grid::style(),
    )?;
    Ok((panel, grid))
}

/// The tab counting `source`'s rows per bucket of its time column, below its own tab and
/// named after its table, if it says `per=`.
fn counted(
    ui: &mut Ui,
    dock: WidgetId,
    source: &Source,
) -> Result<Option<Counted>, Box<dyn Error>> {
    let Some(time) = &source.time else {
        return Ok(None);
    };
    let Some(every) = time.per else {
        return Ok(None);
    };
    let counted = Counted::open(
        ui,
        dock,
        source.panel,
        (&source.name, &source.origin),
        (&time.column, every, time.window),
    )?;
    Ok(Some(counted))
}

/// A stream's columns before its first rows: its key alone.
fn waiting(key: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(key, DataType::Utf8, true)]))
}

/// A SQL table name for `path`: its file name without the extension, lower case, with
/// anything but letters, digits and `_` made `_`, a `_` after a word SQL reserves, and a
/// number added if `taken` has it.
pub(crate) fn table_name(path: &Path, taken: &[String]) -> String {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    sql_name(&stem, taken)
}

/// A SQL table name from `text`: lower case, with anything but letters, digits and `_` made
/// `_`, a `_` after a word SQL reserves, and a number added if `taken` has it.
pub(crate) fn sql_name(text: &str, taken: &[String]) -> String {
    let mut base: String = text
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if base.is_empty() {
        base = "data".to_owned();
    } else if base.starts_with(|c: char| c.is_ascii_digit()) {
        base.insert_str(0, "t_");
    } else if RESERVED.contains(&base.as_str()) {
        base.push('_');
    }
    let mut name = base.clone();
    let mut n = 2;
    while taken.contains(&name) {
        name = format!("{base}_{n}");
        n += 1;
    }
    name
}

/// `n` with a comma between groups of three digits.
pub(crate) fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut text = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            text.push(',');
        }
        text.push(digit);
    }
    text
}

#[cfg(test)]
mod tests;
