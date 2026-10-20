//! The SQL result as a chart: its first column across, and each number column after it in a
//! pane of its own, filling as the result arrives, in order of the first column whatever
//! order the rows come in.
//!
//! Each column is read into plain numbers once, as its rows arrive: a chart reads every point
//! in sight each frame, and slices of numbers are the cheapest thing to read them from.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_cast::cast;
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use densa_chart::{
    paint_line, points_within, y_extent, Axis, AxisKind, ChartModel, ChartPalette, LineStyle, Plot,
    Points, Rect,
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
                       or timestamp), and each number column after it gets a pane";

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

/// Rows as numbers: each row's key, `x`, and a value per pane.
#[derive(Default)]
struct Rows {
    keys: Vec<u64>,
    xs: Vec<f64>,
    ys: Vec<Vec<f64>>,
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
        let kind = axis_kind(fields[x].data_type()).ok_or_else(|| {
            format!("Not charted: the first column, {x_name}, is not a number, date or timestamp")
        })?;
        let numeric: Vec<(usize, String)> = columns
            .filter(|&i| axis_kind(fields[i].data_type()) == Some(AxisKind::Number))
            .map(|i| (i, fields[i].name().clone()))
            .collect();
        if numeric.is_empty() {
            return Err(format!("Not charted: no number column after {x_name}"));
        }
        let caption = caption(&x_name, &numeric);
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
        let xs = numbers(batch.column(self.x).as_ref())?;
        let ys = self
            .panes
            .iter()
            .map(|&(column, _)| numbers(batch.column(column).as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
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
        self.keys.append(&mut other.keys);
        self.xs.append(&mut other.xs);
        for (column, other) in self.ys.iter_mut().zip(&mut other.ys) {
            column.append(other);
        }
    }

    /// Puts the rows in order of `x`; rows of the same `x` keep their order.
    fn sort(&mut self) {
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

/// What a chart of `x` across and `panes` says above it.
fn caption(x: &str, panes: &[(usize, String)]) -> String {
    let names: Vec<&str> = panes
        .iter()
        .take(MAX_PANES)
        .map(|(_, name)| name.as_str())
        .collect();
    let mut caption = format!("{x} across; {} below", names.join(", "));
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
fn numbers(array: &dyn Array) -> Result<Vec<f64>, String> {
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
        let xs = &self.charted.as_ref()?.shown.xs;
        Some((*xs.first()?, *xs.last()?))
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
        Some(Axis::number(low, high).padded(0.08))
    }

    fn paint(&self, pane: usize, plot: &Plot, canvas: &mut Canvas<'_>, palette: &ChartPalette) {
        let area = plot.area();
        let (start, end) = (plot.x().start(), plot.x().end());
        if let Some(mark) = self.mark.filter(|m| (start..=end).contains(m)) {
            let x = plot.x_px(mark).round();
            let band = Rect::new(x - 1.0, area.y0, 3.0, area.height());
            canvas.fill_rect(band, palette.line.with_alpha(0.3));
        }
        let Some(points) = self.points(pane) else {
            return;
        };
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
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Date32Array, Float64Array, Int64Array, StringArray};
    use arrow_array::{Decimal128Array, TimestampMillisecondArray, UInt64Array};
    use arrow_schema::{Field, Schema};
    use densa_draw::{Instance, InstanceBuffer};
    use densa_text::{AtlasConfig, GlyphCache};

    use super::*;

    /// A result of `columns` with the key `#` after them, numbered from `first`.
    fn batch(first: u64, columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let rows = columns[0].1.len() as u64;
        let mut columns = columns;
        columns.push((
            "#",
            Arc::new(UInt64Array::from_iter_values(first..first + rows)),
        ));
        RecordBatch::try_from_iter(columns).unwrap()
    }

    fn chart_of(columns: Vec<(&str, ArrayRef)>) -> Plotted {
        let batch = batch(0, columns);
        let mut chart = Plotted::new(&batch.schema(), "#");
        chart.add(&batch).unwrap();
        chart.finish();
        chart
    }

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn floats(values: &[f64]) -> ArrayRef {
        Arc::new(Float64Array::from(values.to_vec()))
    }

    fn texts(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    /// Pane `pane`'s points, by `x`.
    fn points(chart: &Plotted, pane: usize) -> Vec<(f64, f64)> {
        let (xs, ys) = chart.points(pane).unwrap();
        xs.iter().copied().zip(ys.iter().copied()).collect()
    }

    #[test]
    fn the_first_column_goes_across_and_each_number_column_gets_a_pane() {
        let chart = chart_of(vec![
            ("hour", ints(&[1, 2, 3])),
            ("service", texts(&["a", "b", "c"])),
            ("count", ints(&[10, 20, 30])),
            ("mean", floats(&[1.5, 2.5, 3.5])),
        ]);
        assert_eq!(chart.panes(), 2, "the text column is left out");
        assert_eq!(chart.pane_title(0), "count");
        assert_eq!(chart.pane_title(1), "mean");
        assert_eq!(chart.pane_title(2), "");
        assert_eq!(chart.caption(), "hour across; count, mean below");
        assert!(chart.describe().ends_with(chart.caption()));
        assert_eq!(chart.x_kind(), AxisKind::Number);
        assert_eq!(chart.x_extent(), Some((1.0, 3.0)));
        assert_eq!(points(&chart, 1), [(1.0, 1.5), (2.0, 2.5), (3.0, 3.5)]);
        let y = chart.y_axis(1, &Axis::number(0.0, 10.0)).unwrap();
        assert!(y.start() < 1.5 && y.end() > 3.5, "{y:?}");
        assert!(chart.y_axis(2, &Axis::number(0.0, 10.0)).is_none());
    }

    #[test]
    fn the_row_numbers_can_be_anywhere() {
        let numbers: ArrayRef = Arc::new(UInt64Array::from(vec![7, 8]));
        let rows = RecordBatch::try_from_iter([
            ("#", numbers),
            ("x", ints(&[1, 2])),
            ("y", ints(&[3, 4])),
        ])
        .unwrap();
        let mut chart = Plotted::new(&rows.schema(), "#");
        chart.add(&rows).unwrap();
        assert_eq!(chart.caption(), "x across; y below");
        assert_eq!(points(&chart, 0), [(1.0, 3.0), (2.0, 4.0)]);
        assert_eq!(chart.mark_row(Some(8)), Some(2.0));
    }

    #[test]
    fn times_dates_and_decimals_read_as_numbers() {
        let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_500, 2_000]));
        let chart = chart_of(vec![("at", times), ("n", ints(&[1, 2]))]);
        assert_eq!(chart.x_kind(), AxisKind::Time);
        assert_eq!(chart.x_extent(), Some((1.5, 2.0)), "seconds");
        let days: ArrayRef = Arc::new(Date32Array::from(vec![1, 2]));
        let money = Decimal128Array::from(vec![150, -25]).with_precision_and_scale(10, 2);
        let money: ArrayRef = Arc::new(money.unwrap());
        let chart = chart_of(vec![("day", days), ("money", money)]);
        assert_eq!(chart.x_kind(), AxisKind::Time);
        assert_eq!(points(&chart, 0), [(86_400.0, 1.5), (172_800.0, -0.25)]);
    }

    #[test]
    fn rows_in_order_show_at_once_and_others_once_sorted_in() {
        let first = batch(0, vec![("x", ints(&[1, 3])), ("y", ints(&[10, 30]))]);
        let mut chart = Plotted::new(&first.schema(), "#");
        chart.add(&first).unwrap();
        let next = batch(2, vec![("x", ints(&[3, 4])), ("y", ints(&[31, 40]))]);
        chart.add(&next).unwrap();
        assert_eq!(points(&chart, 0).len(), 4, "in order: shown at once");

        let early = batch(4, vec![("x", ints(&[2, 0])), ("y", ints(&[20, 0]))]);
        chart.add(&early).unwrap();
        chart.prepare();
        assert_eq!(points(&chart, 0).len(), 4, "sorted in a while later");
        chart.finish();
        let expected = [
            (0.0, 0.0),
            (1.0, 10.0),
            (2.0, 20.0),
            (3.0, 30.0),
            (3.0, 31.0),
            (4.0, 40.0),
        ];
        assert_eq!(
            points(&chart, 0),
            expected,
            "same x: in the order they came"
        );
        assert_eq!(chart.x_extent(), Some((0.0, 4.0)));

        // A row after one still waiting waits too, so the order holds.
        chart
            .add(&batch(6, vec![("x", ints(&[-1])), ("y", ints(&[1]))]))
            .unwrap();
        chart
            .add(&batch(7, vec![("x", ints(&[9])), ("y", ints(&[1]))]))
            .unwrap();
        assert_eq!(chart.x_extent(), Some((0.0, 4.0)));
        chart.charted.as_mut().unwrap().sorted_at -= SORT_EVERY;
        chart.prepare();
        assert_eq!(
            chart.x_extent(),
            Some((-1.0, 9.0)),
            "once it has been a while"
        );
    }

    #[test]
    fn rows_without_a_place_across_are_left_out() {
        let x = floats(&[f64::NAN, 2.0, f64::INFINITY, f64::NEG_INFINITY, 1.0]);
        let chart = chart_of(vec![("x", x), ("y", ints(&[1, 2, 3, 4, 5]))]);
        assert_eq!(points(&chart, 0), [(1.0, 5.0), (2.0, 2.0)]);
        let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
        let chart = chart_of(vec![("x", nulls), ("y", ints(&[1, 2]))]);
        assert_eq!(points(&chart, 0), [(4.0, 2.0)]);
        let gaps: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        let chart = chart_of(vec![("x", ints(&[1, 2])), ("y", gaps)]);
        assert!(points(&chart, 0)[1].1.is_nan(), "a missing value is a gap");
    }

    #[test]
    fn a_result_that_cannot_be_charted_says_why() {
        let text_first = chart_of(vec![("service", texts(&["a"])), ("n", ints(&[1]))]);
        assert_eq!(text_first.panes(), 1, "one empty pane");
        assert_eq!(
            text_first.caption(),
            "Not charted: the first column, service, is not a number, date or timestamp"
        );
        assert_eq!(text_first.x_extent(), None);
        assert_eq!(text_first.pane_title(0), "");

        let no_numbers = chart_of(vec![("x", ints(&[1])), ("name", texts(&["a"]))]);
        assert_eq!(
            no_numbers.caption(),
            "Not charted: no number column after x"
        );
        let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1]));
        let times_after = chart_of(vec![("x", ints(&[1])), ("at", times)]);
        assert_eq!(
            times_after.caption(),
            "Not charted: no number column after x"
        );
        let alone = chart_of(vec![("x", ints(&[1]))]);
        assert!(alone.caption().contains("no number column"));

        let schema = Arc::new(Schema::new(vec![Field::new("#", DataType::UInt64, false)]));
        assert_eq!(
            Plotted::new(&schema, "#").caption(),
            "The result has no columns to chart"
        );
        assert_eq!(
            Plotted::new(&schema, "row").caption(),
            "The result has no row numbers"
        );
        assert_eq!(Plotted::waiting().caption(), WAITING);
    }

    #[test]
    fn a_result_without_rows_to_chart_says_so_once_it_has_arrived() {
        let empty = batch(0, vec![("x", ints(&[])), ("y", ints(&[]))]);
        let mut chart = Plotted::new(&empty.schema(), "#");
        chart.add(&empty).unwrap();
        assert_eq!(chart.caption(), "x across; y below", "more may come");
        chart.finish();
        assert_eq!(chart.caption(), "Not charted: no row has a value for x");
        let mut waiting = Plotted::waiting();
        waiting.finish();
        assert_eq!(waiting.caption(), WAITING);
    }

    #[test]
    fn panes_stop_at_the_most_shown_and_the_caption_says_so() {
        let columns: Vec<(&str, ArrayRef)> = ["x", "a", "b", "c", "d", "e"]
            .into_iter()
            .map(|name| (name, ints(&[1, 2])))
            .collect();
        let chart = chart_of(columns);
        assert_eq!(chart.panes(), MAX_PANES);
        assert_eq!(chart.pane_title(3), "d");
        assert_eq!(
            chart.caption(),
            "x across; a, b, c, d below (the first 4 of 5 number columns)"
        );
    }

    #[test]
    fn a_row_is_marked_where_it_is_across() {
        let mut chart = chart_of(vec![
            ("x", floats(&[7.0, 2.0, f64::NAN])),
            ("y", ints(&[1, 2, 3])),
        ]);
        assert_eq!(chart.mark_row(Some(0)), Some(7.0));
        assert_eq!(chart.mark, Some(7.0));
        assert_eq!(chart.mark_row(Some(1)), Some(2.0));
        assert_eq!(chart.mark_row(None), None, "no row selected");
        assert_eq!(chart.mark, None);
        assert_eq!(chart.mark_row(Some(1)), Some(2.0));
        assert_eq!(chart.mark_row(Some(2)), None, "no place across");
        assert_eq!(chart.mark, None);
        assert_eq!(chart.mark_row(Some(99)), None, "no such row");
        let mut waiting = Plotted::waiting();
        assert_eq!(waiting.mark_row(Some(0)), None);

        chart
            .add(&batch(3, vec![("x", floats(&[0.5])), ("y", ints(&[1]))]))
            .unwrap();
        assert_eq!(
            chart.mark_row(Some(3)),
            Some(0.5),
            "a row waiting is sorted in"
        );
    }

    #[test]
    fn the_y_extent_is_found_again_when_rows_or_the_view_change() {
        let first = batch(0, vec![("x", ints(&[1, 2])), ("y", ints(&[10, 20]))]);
        let mut chart = Plotted::new(&first.schema(), "#");
        chart.add(&first).unwrap();
        let wide = Axis::number(0.0, 10.0);
        let range = |chart: &Plotted, x: &Axis| {
            let y = chart.y_axis(0, x).unwrap();
            (y.start(), y.end())
        };
        let before = range(&chart, &wide);
        assert_eq!(range(&chart, &wide), before, "the same again");
        chart
            .add(&batch(2, vec![("x", ints(&[3])), ("y", ints(&[90]))]))
            .unwrap();
        assert!(range(&chart, &wide).1 > 90.0, "a row came: found again");
        assert!(
            range(&chart, &Axis::number(0.0, 1.5)).1 < 20.0,
            "a narrower view"
        );
    }

    #[test]
    fn rows_for_a_chart_that_shows_nothing_are_ignored() {
        let rows = batch(0, vec![("service", texts(&["a"])), ("n", ints(&[1]))]);
        let mut chart = Plotted::new(&rows.schema(), "#");
        assert_eq!(chart.add(&rows), Ok(()));
    }

    #[test]
    fn rows_unlike_the_columns_are_refused() {
        let rows = batch(0, vec![("x", ints(&[1])), ("y", ints(&[1]))]);
        let mut chart = Plotted::new(&rows.schema(), "#");
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![0]));
        let signed =
            RecordBatch::try_from_iter([("x", ints(&[1])), ("y", ints(&[1])), ("#", keys)])
                .unwrap();
        assert!(chart.add(&signed).is_err());
    }

    /// What pane 0 of `chart` draws across 0..`x_end` on a 400 x 200 plot.
    fn drawn(chart: &Plotted, x_end: f64) -> Vec<Instance> {
        let mut cache = GlyphCache::new(AtlasConfig::default());
        let mut out = InstanceBuffer::with_capacity(65_536);
        let mut canvas = Canvas::begin(&mut cache, &mut out);
        let plot = Plot::new(
            Rect::new(0.0, 0.0, 400.0, 200.0),
            Axis::number(0.0, x_end),
            Axis::number(0.0, 10.0),
        );
        chart.paint(0, &plot, &mut canvas, &ChartPalette::new());
        out.as_slice().to_vec()
    }

    /// The squares drawn `DOT` wide.
    fn dots(instances: &[Instance]) -> usize {
        let dot = |i: &&Instance| i.rect[2] == DOT && i.rect[3] == DOT;
        instances.iter().filter(dot).count()
    }

    #[test]
    fn a_few_points_are_dots_on_a_line_and_many_a_line_alone() {
        let few = chart_of(vec![("x", ints(&[1, 2, 3])), ("y", ints(&[2, 5, 8]))]);
        let drawn_few = drawn(&few, 4.0);
        assert_eq!(dots(&drawn_few), 3);
        assert!(drawn_few.len() > 3, "and a line");

        let x: Vec<i64> = (0..300).collect();
        let y: Vec<i64> = x.iter().map(|x| x % 7).collect();
        let many = chart_of(vec![("x", ints(&x)), ("y", ints(&y))]);
        assert_eq!(dots(&drawn(&many, 300.0)), 0, "300 points in sight");
        assert_eq!(dots(&drawn(&many, 199.0)), 200, "200 in sight: 0 to 199");
        assert_eq!(dots(&drawn(&many, 200.0)), 0, "201 in sight");

        let off = chart_of(vec![("x", ints(&[1, 2])), ("y", ints(&[50, 5]))]);
        assert_eq!(
            dots(&drawn(&off, 4.0)),
            1,
            "a point above the plot has no dot"
        );
    }

    #[test]
    fn the_marked_row_is_a_band_across_the_pane() {
        let band = |instances: &[Instance]| {
            let color = ChartPalette::new().line.with_alpha(0.3).to_array();
            instances
                .iter()
                .filter(|i| i.color == color && i.rect[3] == 200.0)
                .map(|i| i.rect)
                .collect::<Vec<_>>()
        };
        let mut chart = chart_of(vec![("x", ints(&[1, 2, 3])), ("y", ints(&[2, 5, 8]))]);
        assert!(band(&drawn(&chart, 4.0)).is_empty());
        chart.mark_row(Some(1));
        assert_eq!(
            band(&drawn(&chart, 4.0)),
            [[199.0, 0.0, 3.0, 200.0]],
            "x 2 of 0..4"
        );
        assert!(band(&drawn(&chart, 1.5)).is_empty(), "out of sight");

        let mut waiting = Plotted::waiting();
        waiting.mark = Some(1.0);
        assert_eq!(
            band(&drawn(&waiting, 4.0)).len(),
            1,
            "drawn before any series"
        );
    }

    #[test]
    #[ignore = "a measurement: cargo test --release frame_of -- --ignored --nocapture"]
    fn a_frame_of_a_million_rows_in_four_panes() {
        let rows = 1_000_000_i64;
        let mut chart = None;
        let started = Instant::now();
        for start in (0..rows).step_by(8_192) {
            let x: Vec<i64> = (start..(start + 8_192).min(rows)).collect();
            let y = |k: i64| ints(&x.iter().map(|v| v * k % 977).collect::<Vec<_>>());
            let columns = vec![
                ("x", ints(&x)),
                ("a", y(3)),
                ("b", y(5)),
                ("c", y(7)),
                ("d", y(11)),
            ];
            let rows = batch(start as u64, columns);
            let chart = chart.get_or_insert_with(|| Plotted::new(&rows.schema(), "#"));
            chart.add(&rows).unwrap();
        }
        let chart = chart.unwrap();
        println!("taking the rows in: {:?}", started.elapsed());
        let mut cache = GlyphCache::new(AtlasConfig::default());
        let mut out = InstanceBuffer::with_capacity(200_000);
        let x = Axis::number(0.0, rows as f64);
        for _ in 0..3 {
            let started = Instant::now();
            let mut canvas = Canvas::begin(&mut cache, &mut out);
            for pane in 0..4 {
                let y = chart.y_axis(pane, &x).unwrap();
                let top = pane as f32 * 200.0;
                let plot = Plot::new(Rect::new(0.0, top, 1_600.0, 200.0), x, y);
                chart.paint(pane, &plot, &mut canvas, &ChartPalette::new());
            }
            println!("a frame: {:?}", started.elapsed());
        }
    }
}
