//! The SQL result as a chart: its first column across, and each number column after it in a
//! pane of its own, filling as the result arrives, in order of the first column whatever
//! order the rows come in. A first column of text (or true/false) makes a bar chart
//! instead: a bar for each row, in the result's order, named by that column.
//!
//! Each column is read into plain numbers once, as its rows arrive: a chart reads every point
//! in sight each frame, and slices of numbers are the cheapest thing to read them from.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_cast::cast;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use densa_chart::{
    paint_bars, paint_line, points_within, y_extent, Axis, AxisKind, BarStyle, ChartModel,
    ChartPalette, LineStyle, Plot, Points, Rect,
};
use densa_draw::Canvas;

/// Panes at most: number columns after these are left out.
pub(crate) const MAX_PANES: usize = 4;
/// While at most this many points are in sight, each is drawn as a dot too: a grouped
/// result reads better as dots on its line, and a single point has no line.
const DOTS_UP_TO: usize = 200;
/// The side of a dot, in pixels.
const DOT: f32 = 4.0;
/// How often rows that came out of order are sorted in while the result arrives (and once
/// it has all arrived).
const SORT_EVERY: Duration = Duration::from_millis(300);
/// What the chart says before a query has run.
const WAITING: &str = "Run a query to chart it: its first column goes across (a number, date \
                       or timestamp; text makes a bar for each row), and each number column \
                       after it gets a pane";

/// A query's result as a chart model.
pub(crate) struct Plotted {
    charted: Option<Charted>,
    /// What the chart shows, or why it shows nothing.
    caption: String,
    /// Where across the grid's selected row is.
    mark: Option<f64>,
}

/// A result that can be charted.
struct Charted {
    /// Where the key, `x` and each pane's column are in the result's columns.
    key: usize,
    x: usize,
    panes: Vec<(usize, String)>,
    x_name: String,
    kind: AxisKind,
    /// The rows charted, by `x`.
    shown: Rows,
    /// Rows that came out of order, until they are sorted in.
    waiting: Rows,
    sorted_at: Instant,
    /// Each pane's last `y` extent, for the `x` and rows it was found for: finding it reads
    /// every point in sight, and a chart is often repainted without moving (the pointer
    /// moving over it).
    extents: RefCell<Vec<Option<Extent>>>,
}

/// A pane's `y` extent while `x` shows, of the first `rows` rows.
#[derive(Clone, Copy)]
struct Extent {
    x: (f64, f64),
    rows: usize,
    found: Option<(f64, f64)>,
}

/// Rows as numbers: each row's key, `x`, and a value per pane; and for bars, each one's
/// name.
#[derive(Default)]
struct Rows {
    keys: Vec<u64>,
    xs: Vec<f64>,
    ys: Vec<Vec<f64>>,
    names: Vec<Box<str>>,
}

impl Plotted {
    /// A chart waiting for a query.
    pub(crate) fn waiting() -> Self {
        Self::not_charted(WAITING.to_owned())
    }

    /// A chart showing nothing, saying why.
    pub(crate) fn not_charted(caption: String) -> Self {
        Self {
            charted: None,
            caption,
            mark: None,
        }
    }

    /// A chart for a result of columns `schema`, keyed by its column `key` (unsigned
    /// integers, not charted); its rows come through [`add`](Self::add).
    pub(crate) fn new(schema: &SchemaRef, key: &str) -> Self {
        match Charted::new(schema, key) {
            Ok((charted, caption)) => Self {
                charted: Some(charted),
                caption,
                mark: None,
            },
            Err(caption) => Self::not_charted(caption),
        }
    }

    /// What the chart shows, or why it shows nothing.
    pub(crate) fn caption(&self) -> &str {
        &self.caption
    }

    /// Takes in rows of the result. A result that cannot be charted ignores them.
    pub(crate) fn add(&mut self, batch: &RecordBatch) -> Result<(), String> {
        match &mut self.charted {
            Some(charted) => charted.add(batch),
            None => Ok(()),
        }
    }

    /// The result has all arrived: rows out of order are sorted in now, and a result with
    /// nothing to show says so.
    pub(crate) fn finish(&mut self) {
        let Some(charted) = &mut self.charted else {
            return;
        };
        charted.sort_in(true);
        if charted.shown.xs.is_empty() {
            self.caption = format!("Not charted: no row has a value for {}", charted.x_name);
        }
    }

    /// Marks where across the row keyed `key` is, and returns it; clears the mark (and
    /// returns `None`) for no row, a row the chart does not have, or one with no `x`.
    pub(crate) fn mark_row(&mut self, key: Option<u64>) -> Option<f64> {
        self.mark = match (key, &mut self.charted) {
            (Some(key), Some(charted)) => {
                // The row may still be waiting to be sorted in.
                charted.sort_in(true);
                let shown = &charted.shown;
                let at = shown.keys.iter().position(|&k| k == key);
                at.map(|at| shown.xs[at])
            }
            _ => None,
        };
        self.mark
    }

    /// Where the mark is, if there is one.
    #[cfg(test)]
    pub(crate) fn mark(&self) -> Option<f64> {
        self.mark
    }

    /// The points of pane `pane`, by `x`.
    fn points(&self, pane: usize) -> Option<(&[f64], &[f64])> {
        let shown = &self.charted.as_ref()?.shown;
        Some((&shown.xs, shown.ys.get(pane)?))
    }
}

impl Charted {
    /// The chart of a result of columns `schema` keyed by `key`, and its caption; or why it
    /// cannot be charted.
    fn new(schema: &SchemaRef, key: &str) -> Result<(Self, String), String> {
        let fields = schema.fields();
        let key = fields
            .iter()
            .position(|field| field.name() == key)
            .ok_or("The result has no row numbers")?;
        let mut columns = (0..fields.len()).filter(|&i| i != key);
        let x = columns.next().ok_or("The result has no columns to chart")?;
        let x_name = fields[x].name().clone();
        let kind = axis_kind(fields[x].data_type())
            .or_else(|| is_text(fields[x].data_type()).then_some(AxisKind::Category))
            .ok_or_else(|| {
                format!(
                    "Not charted: the first column, {x_name}, is not a number, date, timestamp \
                     or text"
                )
            })?;
        let numeric: Vec<(usize, String)> = columns
            .filter(|&i| axis_kind(fields[i].data_type()) == Some(AxisKind::Number))
            .map(|i| (i, fields[i].name().clone()))
            .collect();
        if numeric.is_empty() {
            return Err(format!("Not charted: no number column after {x_name}"));
        }
        let caption = caption(&x_name, kind, &numeric);
        let panes: Vec<(usize, String)> = numeric.into_iter().take(MAX_PANES).collect();
        let empty = || Rows {
            ys: vec![Vec::new(); panes.len()],
            ..Rows::default()
        };
        let charted = Self {
            key,
            x,
            x_name,
            kind,
            shown: empty(),
            waiting: empty(),
            sorted_at: Instant::now(),
            extents: RefCell::new(vec![None; panes.len()]),
            panes,
        };
        Ok((charted, caption))
    }

    /// Takes in `batch`'s rows that have an `x`: straight into the chart if they come in
    /// order after it, else to be sorted in.
    fn add(&mut self, batch: &RecordBatch) -> Result<(), String> {
        let keys = batch.column(self.key);
        let keys = keys
            .as_primitive_opt::<UInt64Type>()
            .ok_or("the row numbers are not unsigned integers")?;
        let ys = self
            .panes
            .iter()
            .map(|&(column, _)| numbers(batch.column(column).as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if self.kind == AxisKind::Category {
            return self.add_bars(batch, keys, &ys);
        }
        let xs = numbers(batch.column(self.x).as_ref())?;
        let mut rows = Rows {
            ys: vec![Vec::new(); ys.len()],
            ..Rows::default()
        };
        for (row, &x) in xs.iter().enumerate() {
            // A row without a place across (null, NaN, infinite) is left out.
            if x.is_finite() && keys.is_valid(row) {
                rows.push(keys.value(row), x, ys.iter().map(|y| y[row]));
            }
        }
        let after = self
            .shown
            .xs
            .last()
            .is_none_or(|&last| rows.xs.first().is_none_or(|&first| last <= first));
        if self.waiting.xs.is_empty() && after && rows.xs.is_sorted() {
            self.shown.append(rows);
        } else {
            self.waiting.append(rows);
            self.sort_in(false);
        }
        Ok(())
    }

    /// Takes in `batch`'s rows as bars after the ones the chart has, each named by its `x`
    /// (`null` for none), whose values per pane are `ys`.
    fn add_bars(
        &mut self,
        batch: &RecordBatch,
        keys: &UInt64Array,
        ys: &[Vec<f64>],
    ) -> Result<(), String> {
        let options = FormatOptions::new().with_null("null");
        let names = ArrayFormatter::try_new(batch.column(self.x).as_ref(), &options)
            .map_err(|e| e.to_string())?;
        for row in (0..batch.num_rows()).filter(|&row| keys.is_valid(row)) {
            // Lossless: a place below 2^53.
            let place = self.shown.xs.len() as f64;
            self.shown.names.push(named(&names, row));
            self.shown
                .push(keys.value(row), place, ys.iter().map(|y| y[row]));
        }
        Ok(())
    }

    /// Sorts the rows waiting into the chart: now if `now`, else if it has been a while.
    fn sort_in(&mut self, now: bool) {
        if self.waiting.xs.is_empty() || !(now || self.sorted_at.elapsed() >= SORT_EVERY) {
            return;
        }
        let waiting = std::mem::take(&mut self.waiting);
        self.waiting.ys = vec![Vec::new(); self.panes.len()];
        self.shown.append(waiting);
        self.shown.sort();
        self.sorted_at = Instant::now();
    }
}

impl Rows {
    fn push(&mut self, key: u64, x: f64, ys: impl Iterator<Item = f64>) {
        self.keys.push(key);
        self.xs.push(x);
        for (column, y) in self.ys.iter_mut().zip(ys) {
            column.push(y);
        }
    }

    fn append(&mut self, mut other: Rows) {
        debug_assert!(other.names.is_empty(), "bars go straight in, never waiting");
        self.keys.append(&mut other.keys);
        self.xs.append(&mut other.xs);
        for (column, other) in self.ys.iter_mut().zip(&mut other.ys) {
            column.append(other);
        }
    }

    /// Puts the rows in order of `x`; rows of the same `x` keep their order.
    fn sort(&mut self) {
        debug_assert!(
            self.names.is_empty(),
            "bars come in order and are never sorted"
        );
        let mut order: Vec<usize> = (0..self.xs.len()).collect();
        order.sort_by(|&a, &b| self.xs[a].total_cmp(&self.xs[b]));
        let take = |values: &[f64]| order.iter().map(|&i| values[i]).collect::<Vec<_>>();
        self.keys = order.iter().map(|&i| self.keys[i]).collect();
        self.xs = take(&self.xs);
        self.ys = self.ys.iter().map(|y| take(y)).collect();
    }
}

/// What axis a column of `data_type` goes on: `None` if it is not charted.
fn axis_kind(data_type: &DataType) -> Option<AxisKind> {
    match data_type {
        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64 => Some(AxisKind::Time),
        data_type if data_type.is_numeric() => Some(AxisKind::Number),
        _ => None,
    }
}

/// Characters of a bar's name kept, at most: no more could show under it.
const NAME_CHARS: usize = 200;

/// The name of bar `row`, cut to [`NAME_CHARS`].
fn named(names: &ArrayFormatter<'_>, row: usize) -> Box<str> {
    let mut name = names.value(row).to_string();
    if let Some((cut, _)) = name.char_indices().nth(NAME_CHARS) {
        name.truncate(cut);
    }
    name.into_boxed_str()
}

/// Whether a column of `data_type` names bars: text, or true and false.
fn is_text(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Boolean => true,
        DataType::Dictionary(_, values) => is_text(values),
        _ => false,
    }
}

/// What a chart of `x` (across, of `kind`) and `panes` says above it.
fn caption(x: &str, kind: AxisKind, panes: &[(usize, String)]) -> String {
    let names: Vec<&str> = panes
        .iter()
        .take(MAX_PANES)
        .map(|(_, name)| name.as_str())
        .collect();
    let names = names.join(", ");
    let mut caption = match kind {
        AxisKind::Category => format!("Bars of {names}, a bar for each {x}"),
        _ => format!("{x} across; {names} below"),
    };
    if panes.len() > MAX_PANES {
        let count = panes.len();
        caption.push_str(&format!(
            " (the first {MAX_PANES} of {count} number columns)"
        ));
    }
    caption
}

/// The values of `array` as numbers (NaN for a null): numbers as they are, timestamps as
/// seconds since the epoch, dates as the seconds of their midnight.
pub(crate) fn numbers(array: &dyn Array) -> Result<Vec<f64>, String> {
    let counted = |per_second: f64, days: bool| -> Result<Vec<f64>, String> {
        let counts = cast(array, &DataType::Int64).map_err(|e| e.to_string())?;
        let counts = counts.as_primitive::<Int64Type>();
        Ok(counts
            .iter()
            .map(|count| {
                // Lossless enough: a chart draws to a pixel.
                count.map_or(f64::NAN, |count| match days {
                    true => count as f64 * 86_400.0,
                    false => count as f64 / per_second,
                })
            })
            .collect())
    };
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => counted(1.0, false),
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => counted(1e3, false),
        DataType::Timestamp(TimeUnit::Microsecond, _) => counted(1e6, false),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => counted(1e9, false),
        DataType::Date32 => counted(1.0, true),
        _ => {
            let values = cast(array, &DataType::Float64).map_err(|e| e.to_string())?;
            let values = values.as_primitive::<Float64Type>();
            Ok(values.iter().map(|v| v.unwrap_or(f64::NAN)).collect())
        }
    }
}

impl ChartModel for Plotted {
    fn x_kind(&self) -> AxisKind {
        self.charted.as_ref().map_or(AxisKind::Number, |c| c.kind)
    }

    fn x_extent(&self) -> Option<(f64, f64)> {
        let charted = self.charted.as_ref()?;
        let xs = &charted.shown.xs;
        let (first, last) = (*xs.first()?, *xs.last()?);
        // A bar takes the room up to halfway to its neighbours.
        match charted.kind {
            AxisKind::Category => Some((first - 0.5, last + 0.5)),
            _ => Some((first, last)),
        }
    }

    fn x_step(&self) -> Option<f64> {
        let bars = self.charted.as_ref()?.kind == AxisKind::Category;
        bars.then_some(1.0)
    }

    fn panes(&self) -> usize {
        self.charted.as_ref().map_or(1, |c| c.panes.len())
    }

    fn y_axis(&self, pane: usize, x: &Axis) -> Option<Axis> {
        let charted = self.charted.as_ref()?;
        let points = self.points(pane)?;
        let mut extents = charted.extents.borrow_mut();
        let last = extents.get_mut(pane)?;
        let (shown, rows) = ((x.start(), x.end()), points.len());
        let found = match *last {
            // Rows only arrive or move into place, and the count changes with either.
            Some(extent) if extent.x == shown && extent.rows == rows => extent.found,
            _ => {
                let found = y_extent(&points, points_within(&points, x));
                *last = Some(Extent {
                    x: shown,
                    rows,
                    found,
                });
                found
            }
        };
        let (low, high) = found?;
        // Bars rise from zero, which shows.
        let (low, high) = match charted.kind {
            AxisKind::Category => (low.min(0.0), high.max(0.0)),
            _ => (low, high),
        };
        Some(Axis::number(low, high).padded(0.08))
    }

    fn paint(&self, pane: usize, plot: &Plot, canvas: &mut Canvas<'_>, palette: &ChartPalette) {
        let area = plot.area();
        let (start, end) = (plot.x().start(), plot.x().end());
        let bars = self.x_kind() == AxisKind::Category;
        // A bar's place reaches half a place each side of it.
        let reach = if bars { 0.5 } else { 0.0 };
        let shown = |m: &f64| m + reach >= start && m - reach <= end;
        if let Some(mark) = self.mark.filter(shown) {
            let band = if bars {
                // Behind a bar: its whole place shows around and above it.
                let (left, right) = (plot.x_px(mark - 0.5), plot.x_px(mark + 0.5));
                Rect::from_edges(left.round(), area.y0, right.round(), area.y1)
            } else {
                let x = plot.x_px(mark).round();
                Rect::new(x - 1.0, area.y0, 3.0, area.height())
            };
            canvas.fill_rect(band.intersect(area), palette.line.with_alpha(0.3));
        }
        let Some(points) = self.points(pane) else {
            return;
        };
        if bars {
            paint_bars(canvas, plot, &points, 1.0, BarStyle::new(), |_| {
                palette.line
            });
            return;
        }
        let line = LineStyle::new(palette.line).with_width(1.5);
        paint_line(canvas, plot, &points, line);
        let shown = points_within(&points, &plot.x());
        if shown.len() > DOTS_UP_TO {
            return;
        }
        let (low, high) = (plot.y().start(), plot.y().end());
        for index in shown {
            let y = points.y(index);
            if !(low..=high).contains(&y) {
                continue;
            }
            let (x, y) = (plot.x_px(points.x(index)), plot.y_px(y));
            let dot = Rect::new((x - DOT / 2.0).round(), (y - DOT / 2.0).round(), DOT, DOT);
            canvas.fill_rect(dot, palette.line);
        }
    }

    fn pane_title(&self, pane: usize) -> &str {
        self.charted
            .as_ref()
            .and_then(|c| c.panes.get(pane))
            .map_or("", |(_, name)| name)
    }

    fn category(&self, index: usize) -> Option<&str> {
        let names = &self.charted.as_ref()?.shown.names;
        names.get(index).map(|name| &**name)
    }

    fn prepare(&mut self) {
        if let Some(charted) = &mut self.charted {
            charted.sort_in(false);
        }
    }

    fn describe(&self) -> String {
        format!("Chart of the SQL result: {}", self.caption)
    }
}

#[cfg(test)]
mod tests;
