//! The SQL result as a chart: its first column across, and each number column after it in a
//! pane of its own, filling as the result arrives, in order of the first column whatever
//! order the rows come in.

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use densa_arrow::{LiveSeries, SeriesError};
use densa_chart::{
    paint_line, points_within, y_extent, Axis, AxisKind, ChartModel, ChartPalette, LineStyle, Plot,
    Points, Rect,
};
use densa_data::{LiveTable, Order, Scalar, ViewId, ViewSpec};
use densa_draw::Canvas;

/// Panes at most: number columns after these are left out.
pub(crate) const MAX_PANES: usize = 4;
/// While at most this many points are in sight, each is drawn as a dot too: a grouped
/// result reads better as dots on its line, and a single point has no line.
const DOTS_UP_TO: usize = 200;
/// The side of a dot, in pixels.
const DOT: f32 = 4.0;
/// What the chart says before a query has run.
const WAITING: &str = "Run a query to chart it: its first column goes across (a number, date \
                       or time), and each number column after it gets a pane";

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
    /// The result's rows (sharing their arrays with the grid's), sorted by `x` in `view`.
    table: LiveTable,
    view: ViewId,
    kind: AxisKind,
    panes: Vec<LiveSeries>,
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

    /// A chart for a result of columns `schema`, keyed by its column `key` (which is not
    /// charted); its rows come through [`add`](Self::add).
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
    pub(crate) fn add(&mut self, batch: RecordBatch) -> Result<(), String> {
        let Some(charted) = &mut self.charted else {
            return Ok(());
        };
        charted.table.upsert(batch).map_err(|e| e.to_string())?;
        charted.sync();
        Ok(())
    }

    /// Marks where across the row keyed `key` is, and returns it; clears the mark (and
    /// returns `None`) for no row, a row the chart does not have, or one with no `x`.
    pub(crate) fn mark_row(&mut self, key: Option<u64>) -> Option<f64> {
        self.mark = key.and_then(|key| self.charted.as_ref()?.x_of_key(key));
        self.mark
    }
}

#[cfg(test)]
impl Plotted {
    /// Where the mark is, if there is one.
    pub(crate) fn mark(&self) -> Option<f64> {
        self.mark
    }
}

impl Charted {
    /// The chart of a result of columns `schema` keyed by `key`, and its caption; or why it
    /// cannot be charted.
    fn new(schema: &SchemaRef, key: &str) -> Result<(Self, String), String> {
        let columns: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .filter(|name| *name != key)
            .collect();
        let Some((&x, rest)) = columns.split_first() else {
            return Err("The result has no columns to chart".to_owned());
        };
        let table = LiveTable::new(schema.clone(), key).map_err(|e| e.to_string())?;
        // A series of the first column against itself: whether it reads as numbers.
        if let Err(SeriesError::NotNumeric(_)) = LiveSeries::new(&table, x, x) {
            return Err(format!(
                "Not charted: the first column, {x}, is not a number, date or time"
            ));
        }
        let numeric: Vec<LiveSeries> = rest
            .iter()
            .filter_map(|y| LiveSeries::new(&table, x, y).ok())
            .collect();
        if numeric.is_empty() {
            return Err(format!("Not charted: no number column after {x}"));
        }
        let count = numeric.len();
        let panes: Vec<LiveSeries> = numeric.into_iter().take(MAX_PANES).collect();
        let names: Vec<&str> = panes.iter().map(LiveSeries::y_name).collect();
        let mut caption = format!("{x} across: {}", names.join(", "));
        if count > MAX_PANES {
            caption.push_str(&format!(
                " (the first {MAX_PANES} of {count} number columns)"
            ));
        }
        let kind = match schema.field_with_name(x).map(|field| field.data_type()) {
            Ok(DataType::Timestamp(..) | DataType::Date32 | DataType::Date64) => AxisKind::Time,
            _ => AxisKind::Number,
        };
        let mut table = table;
        let view = table
            .add_view(ViewSpec::new().sort_by(x, Order::Ascending))
            .map_err(|e| e.to_string())?;
        Ok((
            Self {
                table,
                view,
                kind,
                panes,
            },
            caption,
        ))
    }

    /// Brings the series up to date with the table.
    fn sync(&mut self) {
        for series in &mut self.panes {
            series.sync(&self.table);
        }
    }

    /// The `x` of the row keyed `key`, if it has one.
    fn x_of_key(&self, key: u64) -> Option<f64> {
        let record = self.table.record_of_key(&Scalar::UInt(key))?;
        let at = self.table.view(self.view)?.position(record)?;
        let points = self.panes.first()?.points(&self.table, self.view)?;
        Some(points.x(at)).filter(|x| x.is_finite())
    }
}

impl ChartModel for Plotted {
    fn x_kind(&self) -> AxisKind {
        self.charted.as_ref().map_or(AxisKind::Number, |c| c.kind)
    }

    fn x_extent(&self) -> Option<(f64, f64)> {
        let charted = self.charted.as_ref()?;
        let points = charted
            .panes
            .first()?
            .points(&charted.table, charted.view)?;
        points.x_extent()
    }

    fn panes(&self) -> usize {
        self.charted.as_ref().map_or(1, |c| c.panes.len())
    }

    fn y_axis(&self, pane: usize, x: &Axis) -> Option<Axis> {
        let charted = self.charted.as_ref()?;
        let points = charted
            .panes
            .get(pane)?
            .points(&charted.table, charted.view)?;
        let (low, high) = y_extent(&points, points_within(&points, x))?;
        Some(Axis::number(low, high).padded(0.08))
    }

    fn paint(&self, pane: usize, plot: &Plot, canvas: &mut Canvas<'_>, palette: &ChartPalette) {
        let area = plot.area();
        if let Some(mark) = self
            .mark
            .filter(|&m| m >= plot.x().start() && m <= plot.x().end())
        {
            let x = plot.x_px(mark).round();
            let band = Rect::new(x - 1.0, area.y0, 3.0, area.height());
            canvas.fill_rect(band, palette.line.with_alpha(0.3));
        }
        let Some(charted) = &self.charted else {
            return;
        };
        let Some(points) = charted
            .panes
            .get(pane)
            .and_then(|series| series.points(&charted.table, charted.view))
        else {
            return;
        };
        paint_line(
            canvas,
            plot,
            &points,
            LineStyle::new(palette.line).with_width(1.5),
        );
        let shown = points_within(&points, &plot.x());
        if shown.len() <= DOTS_UP_TO {
            for index in shown {
                let y = points.y(index);
                if !y.is_finite() || !(plot.y().start()..=plot.y().end()).contains(&y) {
                    continue;
                }
                let (x, y) = (plot.x_px(points.x(index)), plot.y_px(y));
                let dot = Rect::new((x - DOT / 2.0).round(), (y - DOT / 2.0).round(), DOT, DOT);
                canvas.fill_rect(dot, palette.line);
            }
        }
    }

    fn pane_title(&self, pane: usize) -> &str {
        self.charted
            .as_ref()
            .and_then(|c| c.panes.get(pane))
            .map_or("", LiveSeries::y_name)
    }

    fn prepare(&mut self) {
        if let Some(charted) = &mut self.charted {
            charted.sync();
        }
    }

    fn describe(&self) -> String {
        self.caption.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampSecondArray};
    use arrow_array::{Date32Array, UInt64Array};
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
        chart.add(batch).unwrap();
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
        assert_eq!(chart.caption(), "hour across: count, mean");
        assert_eq!(chart.x_kind(), AxisKind::Number);
        assert_eq!(chart.x_extent(), Some((1.0, 3.0)));
        let y = chart.y_axis(1, &Axis::number(0.0, 10.0)).unwrap();
        assert!(y.start() < 1.5 && y.end() > 3.5, "{y:?}");
    }

    #[test]
    fn times_and_dates_go_on_a_time_axis() {
        let times: ArrayRef = Arc::new(TimestampSecondArray::from(vec![100, 200]));
        let chart = chart_of(vec![("at", times), ("n", ints(&[1, 2]))]);
        assert_eq!(chart.x_kind(), AxisKind::Time);
        assert_eq!(chart.x_extent(), Some((100.0, 200.0)));
        let days: ArrayRef = Arc::new(Date32Array::from(vec![1, 2]));
        let chart = chart_of(vec![("day", days), ("n", ints(&[1, 2]))]);
        assert_eq!(chart.x_kind(), AxisKind::Time);
        assert_eq!(chart.x_extent(), Some((86_400.0, 172_800.0)));
    }

    #[test]
    fn rows_in_any_order_are_charted_in_order_across() {
        let first = batch(0, vec![("x", ints(&[5, 1])), ("y", ints(&[50, 10]))]);
        let mut chart = Plotted::new(&first.schema(), "#");
        chart.add(first).unwrap();
        chart
            .add(batch(2, vec![("x", ints(&[3, 9])), ("y", ints(&[30, 90]))]))
            .unwrap();
        let charted = chart.charted.as_ref().unwrap();
        let points = charted.panes[0]
            .points(&charted.table, charted.view)
            .unwrap();
        let shown: Vec<(f64, f64)> = (0..points.len())
            .map(|i| (points.x(i), points.y(i)))
            .collect();
        assert_eq!(shown, [(1.0, 10.0), (3.0, 30.0), (5.0, 50.0), (9.0, 90.0)]);
        assert_eq!(chart.x_extent(), Some((1.0, 9.0)));
    }

    #[test]
    fn a_result_that_cannot_be_charted_says_why() {
        let text_first = chart_of(vec![("service", texts(&["a"])), ("n", ints(&[1]))]);
        assert_eq!(text_first.panes(), 1, "one empty pane");
        assert!(
            text_first.caption().contains("service"),
            "{}",
            text_first.caption()
        );
        assert_eq!(text_first.x_extent(), None);
        assert_eq!(text_first.pane_title(0), "");

        let no_numbers = chart_of(vec![("x", ints(&[1])), ("name", texts(&["a"]))]);
        assert!(no_numbers.caption().contains("no number column after x"));
        let alone = chart_of(vec![("x", ints(&[1]))]);
        assert!(alone.caption().contains("no number column"));

        let schema = Arc::new(Schema::new(vec![Field::new("#", DataType::UInt64, false)]));
        let keyed_only = Plotted::new(&schema, "#");
        assert_eq!(keyed_only.caption(), "The result has no columns to chart");
        assert_eq!(Plotted::waiting().caption(), WAITING);
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
        assert!(chart
            .caption()
            .ends_with("(the first 4 of 5 number columns)"));
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
        chart.paint(0, &plot, &mut canvas, &palette());
        out.as_slice().to_vec()
    }

    fn palette() -> ChartPalette {
        ChartPalette::new()
    }

    /// The squares drawn `DOT` wide.
    fn dots(instances: &[Instance]) -> usize {
        instances
            .iter()
            .filter(|i| i.rect[2] == DOT && i.rect[3] == DOT)
            .count()
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
        assert_eq!(dots(&drawn(&many, 150.0)), 151, "fewer in sight: 0 to 150");

        let off = chart_of(vec![("x", ints(&[1, 2])), ("y", ints(&[50, 5]))]);
        assert_eq!(
            dots(&drawn(&off, 4.0)),
            1,
            "a point above the plot has no dot"
        );
    }

    #[test]
    fn the_marked_row_is_a_band_across_every_pane() {
        let band = |instances: &[Instance]| {
            let color = palette().line.with_alpha(0.3).to_array();
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
    fn rows_for_a_chart_that_shows_nothing_are_ignored() {
        let rows = batch(0, vec![("service", texts(&["a"])), ("n", ints(&[1]))]);
        let mut chart = Plotted::new(&rows.schema(), "#");
        assert_eq!(chart.add(rows), Ok(()));
    }
}
