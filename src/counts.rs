//! Rows counted per bucket of time: `per=1m` beside `time=COLUMN` counts a source's rows in
//! each minute of that column, charted as bars in a tab of its own below the source's. The
//! source's live table keeps the counts as rows come and go (with a window, old minutes leave
//! with their rows), so charting a million rows reads a row per minute, not every row.

use std::error::Error;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, TimeUnit};
use densa_chart::{
    paint_bars, points_within, y_extent, Axis, AxisKind, BarStyle, ChartModel, ChartPalette, Plot,
};
use densa_data::{Aggregate, GroupSpec, GroupsId, LiveTable};
use densa_draw::Canvas;
use densa_ui::{Style, Ui, WidgetId};
use densa_widgets::{Chart, DockArea, DockAt, DockPanel, DockSide, Label, Panel};

use crate::chart::numbers;
use crate::time::written;

/// The name of the counts' column.
const ROWS: &str = "rows";
/// Buckets a chart shows at first, for a source without a window (with one, it shows the
/// window's span).
const SHOWN: f64 = 60.0;
/// Up to this many buckets, the counts are charted again whenever they change; past it, at
/// most every [`READ_EVERY`], or [`SLOWER`] times as long as the last read took, as reading
/// them costs a row of each.
const READ_ALWAYS: usize = 4_096;
const READ_EVERY: Duration = Duration::from_millis(250);
const SLOWER: u32 = 4;

pub(crate) type CountChart = Chart<Counts>;

/// A source's rows counted per bucket of its time column, and the tab charting them.
pub(crate) struct Counted {
    /// The column of times and the buckets' length.
    column: String,
    every: Duration,
    /// What the chart shows when its rows can be counted, and what its caption says now.
    shows: String,
    said: String,
    caption: WidgetId,
    chart: WidgetId,
    /// The counts, in the source's table: none until the table is made, or if its time
    /// column cannot be counted.
    groups: Option<GroupsId>,
    /// Why the rows are not counted, if they are not.
    pub(crate) off: Option<String>,
    /// The revision of the counts last charted, when they were read, and how long to wait
    /// before reading many again.
    charted: Option<u64>,
    read_at: Option<Instant>,
    read_every: Duration,
}

/// Counts read for the chart.
pub(crate) struct Bars {
    /// Each bucket's start, in seconds since the epoch, and its rows.
    starts: Vec<f64>,
    rows: Vec<f64>,
    /// Rows without a time, in no bucket.
    untimed: u64,
}

/// What reading the counts found.
pub(crate) enum Read {
    /// Nothing new to chart.
    Same,
    /// New counts, charted at the next read: they were read a moment ago.
    Later,
    /// The counts to chart, or why they cannot be.
    New(Result<Bars, String>),
}

impl Counted {
    /// A tab titled after `name` below the source's `beside` in `dock`, keyed by the source's
    /// `origin`, to chart the rows per `every` of `column`; it shows the newest `window` of
    /// them, or [`SHOWN`] buckets without one. Nothing is counted until [`count`](Self::count)
    /// is given the source's table.
    pub(crate) fn open(
        ui: &mut Ui,
        dock: WidgetId,
        beside: WidgetId,
        (name, origin): (&str, &str),
        (column, every, window): (&str, Duration, Option<Duration>),
    ) -> Result<Self, Box<dyn Error>> {
        let per = written(every);
        let spec = DockPanel::new(format!("{name} per {per}")).with_key(format!("counts:{origin}"));
        let below = DockAt::Beside(beside, DockSide::Bottom);
        let panel = DockArea::add_panel(ui, dock, Panel::new(), spec, below)?;
        let body = Style::column().grow(1.0).padding(6.0).gap(6.0);
        let body = ui.add(panel, Panel::transparent(), body)?;
        let line = ui.add(body, Panel::transparent(), Style::row().shrink(0.0))?;
        let shows = match window {
            Some(window) => format!(
                "Rows per {per} of {column}, over the last {}",
                written(window)
            ),
            None => format!("Rows per {per} of {column}"),
        };
        let said = waiting(&shows);
        let caption = Label::new(said.clone()).wrapping().dim();
        let caption = ui.add(line, caption, Style::row().grow(1.0))?;
        let span = window.map_or(every.as_secs_f64() * SHOWN, |w| w.as_secs_f64());
        let chart = CountChart::new(Counts::new(every, &per), span);
        let chart = ui.add(body, chart, CountChart::style())?;
        Ok(Self {
            column: column.to_owned(),
            every,
            shows,
            said,
            caption,
            chart,
            groups: None,
            off: None,
            charted: None,
            read_at: None,
            read_every: READ_EVERY,
        })
    }

    /// Counts nothing until [`count`](Self::count) is given a table again: the source's
    /// table starts over.
    pub(crate) fn wait(&mut self, ui: &mut Ui) {
        self.restart(ui, Ok(None));
    }

    /// Counts the rows of `table`, a source's table made anew, from now on; or says why they
    /// cannot be.
    pub(crate) fn count(&mut self, ui: &mut Ui, table: &mut LiveTable) {
        let added = self
            .counts_of(table)
            .and_then(|spec| table.add_groups(spec).map_err(|e| e.to_string()));
        self.restart(ui, added.map(Some));
    }

    /// Charts no counts, then `groups` (none: waiting for a table); or says why there are
    /// none.
    fn restart(&mut self, ui: &mut Ui, groups: Result<Option<GroupsId>, String>) {
        self.charted = None;
        self.read_at = None;
        self.read_every = READ_EVERY;
        let every = self.every;
        ui.update::<CountChart, _>(self.chart, |chart, cx| {
            *chart.model_mut(cx) = Counts::new(every, &written(every));
        });
        let caption = match groups {
            Ok(groups) => {
                self.groups = groups;
                self.off = None;
                waiting(&self.shows)
            }
            Err(why) => {
                self.groups = None;
                let caption = format!("Not counted: {why}");
                self.off = Some(why);
                caption
            }
        };
        self.say(ui, caption);
    }

    /// Puts `caption` above the chart, if it says something new.
    fn say(&mut self, ui: &mut Ui, caption: String) {
        if caption != self.said {
            self.said.clone_from(&caption);
            ui.update::<Label, _>(self.caption, |label, cx| label.set_text(cx, caption));
        }
    }

    /// What counts the rows of `table`; or why its time column cannot be counted in buckets
    /// of `every`.
    fn counts_of(&self, table: &LiveTable) -> Result<GroupSpec, String> {
        let column = &self.column;
        let schema = table.schema();
        let field = schema
            .field_with_name(column)
            .map_err(|_| format!("the time column {column} is not in the rows"))?;
        let (unit, name) = match field.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => (1_000_000_000, "seconds"),
            DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => {
                (1_000_000, "milliseconds")
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => (1_000, "microseconds"),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => (1, "nanoseconds"),
            DataType::Date32 => (86_400_000_000_000, "days"),
            other => return Err(format!("counts need times; {column} holds {other}")),
        };
        if !self.every.as_nanos().is_multiple_of(unit) {
            let one = written(Duration::from_nanos(unit as u64));
            return Err(format!(
                "per={} is not a whole number of {name}, which {column} counts: write per={one} \
                 or a multiple of it",
                written(self.every)
            ));
        }
        // The counts' column is named apart from the times'.
        let rows = if column == ROWS { "rows_" } else { ROWS };
        Ok(GroupSpec::new()
            .group_by_time(column, self.every)
            .aggregate(Aggregate::count().named(rows)))
    }

    /// Charts what [`read`](Self::read) found. Whether changed counts are left to chart at a
    /// later frame.
    pub(crate) fn show(&mut self, ui: &mut Ui, read: Read) -> bool {
        match read {
            Read::Same => false,
            Read::Later => true,
            Read::New(Ok(bars)) => {
                let caption = match bars.untimed {
                    0 => self.shows.clone(),
                    1 => format!("{}; a row has no time and is left out", self.shows),
                    n => format!("{}; {n} rows have no time and are left out", self.shows),
                };
                self.off = None;
                self.say(ui, caption);
                ui.update::<CountChart, _>(self.chart, |chart, cx| {
                    chart.model_mut(cx).set(&bars.starts, bars.rows);
                });
                false
            }
            Read::New(Err(why)) => {
                let caption = format!("Not counted: {why}");
                self.off = Some(why);
                self.say(ui, caption);
                false
            }
        }
    }

    /// Reads the counts in the source's `table`, if they changed since they were charted.
    pub(crate) fn read(&mut self, table: &LiveTable) -> Read {
        let Some(groups) = self.groups.and_then(|id| table.groups(id)) else {
            return Read::Same;
        };
        let revision = groups.revision();
        if self.charted == Some(revision) {
            return Read::Same;
        }
        let recent = self
            .read_at
            .is_some_and(|at| at.elapsed() < self.read_every);
        if groups.len() > READ_ALWAYS && recent {
            return Read::Later;
        }
        let started = Instant::now();
        let counts = groups.to_batch().map_err(|e| e.to_string());
        let read = counts.and_then(|batch| bars(&batch));
        self.charted = Some(revision);
        self.read_at = Some(Instant::now());
        self.read_every = pause(started.elapsed());
        Read::New(read)
    }

    /// The chart, for tests.
    #[cfg(test)]
    pub(crate) fn chart(&self) -> WidgetId {
        self.chart
    }

    /// What the caption says, for tests.
    #[cfg(test)]
    pub(crate) fn said(&self) -> &str {
        &self.said
    }
}

/// How long to wait before reading many counts again, after a read that `took` this long:
/// a read that takes long waits longer for the next, so the frames between stay smooth.
fn pause(took: Duration) -> Duration {
    READ_EVERY.max(took * SLOWER)
}

/// What the chart says while it waits for rows to count.
fn waiting(shows: &str) -> String {
    format!("{shows}: waiting for the first rows")
}

/// The buckets in `counts` (the time column's buckets, then their counts), but for rows
/// without a time: they are counted apart.
fn bars(counts: &RecordBatch) -> Result<Bars, String> {
    let starts = numbers(counts.column(0).as_ref())?;
    let counted = counts
        .column(1)
        .as_primitive_opt::<UInt64Type>()
        .ok_or("the counts are not counts")?;
    let mut bars = Bars {
        starts: Vec::with_capacity(starts.len()),
        rows: Vec::with_capacity(starts.len()),
        untimed: 0,
    };
    for (start, &rows) in starts.into_iter().zip(counted.values()) {
        if start.is_finite() {
            bars.starts.push(start);
            // Lossless: a bucket has at most 2^32 rows.
            bars.rows.push(rows as f64);
        } else {
            bars.untimed += rows;
        }
    }
    Ok(bars)
}

/// Rows per bucket as a chart model: a bar for each bucket, across its whole length.
pub(crate) struct Counts {
    /// The buckets' length, in seconds.
    every: f64,
    /// Each bucket's middle, where its bar goes, and its rows.
    middles: Vec<f64>,
    rows: Vec<f64>,
    title: String,
}

impl Counts {
    /// No counts yet of buckets `every` long, written `per`.
    fn new(every: Duration, per: &str) -> Self {
        Self {
            every: every.as_secs_f64(),
            middles: Vec::new(),
            rows: Vec::new(),
            title: format!("rows per {per}"),
        }
    }

    /// Charts buckets starting at `starts` (in seconds, in order) with `rows` each.
    fn set(&mut self, starts: &[f64], rows: Vec<f64>) {
        let half = self.every / 2.0;
        self.middles = starts.iter().map(|start| start + half).collect();
        self.rows = rows;
    }

    /// The buckets' middles and rows, for tests.
    #[cfg(test)]
    pub(crate) fn bars(&self) -> (&[f64], &[f64]) {
        (&self.middles, &self.rows)
    }

    fn points(&self) -> (&[f64], &[f64]) {
        (&self.middles, &self.rows)
    }
}

impl ChartModel for Counts {
    fn x_kind(&self) -> AxisKind {
        AxisKind::Time
    }

    /// The first and last buckets' middles: the chart leaves half a bucket of room past
    /// each, so the bars show whole (edges here would show one bucket as two).
    fn x_extent(&self) -> Option<(f64, f64)> {
        Some((*self.middles.first()?, *self.middles.last()?))
    }

    fn x_step(&self) -> Option<f64> {
        Some(self.every)
    }

    fn y_axis(&self, _pane: usize, x: &Axis) -> Option<Axis> {
        let points = self.points();
        let (_, high) = y_extent(&points, points_within(&points, x))?;
        // Bars rise from zero, which shows; every bucket has a row, so the top is above it.
        Some(Axis::number(0.0, high).padded(0.08))
    }

    fn paint(&self, _pane: usize, plot: &Plot, canvas: &mut Canvas<'_>, palette: &ChartPalette) {
        let style = BarStyle::new();
        paint_bars(canvas, plot, &self.points(), self.every, style, |_| {
            palette.line
        });
    }

    fn pane_title(&self, _pane: usize) -> &str {
        &self.title
    }

    fn describe(&self) -> String {
        format!("Chart of the {}", self.title)
    }
}

#[cfg(test)]
mod tests;
