//! The SQL panel: a query over every source, run by DataFusion, its result streamed into a
//! grid as it is computed, and charted in a tab of its own.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use densa_arrow::LiveModel;
use densa_chart::{Axis, ChartModel};
use densa_data::LiveTable;
use densa_datafusion::{Engine, QueryEvent, QueryStream};
use densa_ui::{Action, ActionKind, Align, EventCx, Style, Ui, WidgetId};
use densa_widgets::{Chart, DockArea, DockAt, DockPanel, DockSide, Label, Panel, TextInput};
use densa_window::Waker;

use crate::chart::Plotted;
use crate::source::{grouped, Grid, Source};

type ResultChart = Chart<Plotted>;

/// The key column of a result: its rows are numbered as they arrive. Not `row`, which every
/// source has.
const NUMBER: &str = "#";
/// Rows shown at most: a larger result is cut off (its query cancelled).
const MAX_ROWS: usize = 1_000_000;

/// The SQL panel's widgets and the query running.
pub(crate) struct Query {
    pub(crate) panel: WidgetId,
    pub(crate) input: WidgetId,
    grid: WidgetId,
    status: WidgetId,
    /// The chart (in a tab of its own) and the line above it saying what it shows.
    chart: WidgetId,
    caption: WidgetId,
    /// Whether the chart shows the whole result as it grows: until the user moves it.
    fit: bool,
    stream: Option<QueryStream>,
    /// Where the query stands.
    state: String,
    started: Instant,
}

impl Query {
    /// Adds the panel to `dock`, placed `at`, holding `query`.
    pub(crate) fn add(
        ui: &mut Ui,
        dock: WidgetId,
        at: DockAt,
        query: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let spec = DockPanel::new("SQL").with_key("sql");
        let panel = DockArea::add_panel(ui, dock, Panel::new(), spec, at)?;
        let body = ui.add(panel, Panel::transparent(), Style::column().grow(1.0))?;
        let bar = Style::row()
            .padding(6.0)
            .gap(8.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let bar = ui.add(body, Panel::transparent(), bar)?;
        let input = TextInput::new(query).with_placeholder("SELECT ... FROM a source, then Enter");
        let input = ui.add(bar, input, Style::row().grow(1.0))?;
        let model = LiveModel::new(LiveTable::new(waiting(), NUMBER)?)?;
        let grid = ui.add(body, Grid::new(model), Grid::style())?;
        let line = Style::row().padding(4.0).shrink(0.0);
        let line = ui.add(body, Panel::transparent(), line)?;
        let status = ui.add(line, Label::new("").dim(), Style::row())?;

        let spec = DockPanel::new("Chart").with_key("chart");
        let right = DockAt::Beside(panel, DockSide::Right);
        let chart_panel = DockArea::add_panel(ui, dock, Panel::new(), spec, right)?;
        let body = ui.add(chart_panel, Panel::transparent(), Style::column().grow(1.0))?;
        let line = Style::row().padding(6.0).shrink(0.0);
        let line = ui.add(body, Panel::transparent(), line)?;
        let waiting = Plotted::waiting();
        let caption = Label::new(waiting.caption()).wrapping().dim();
        let caption = ui.add(line, caption, Style::row().grow(1.0))?;
        let chart = ResultChart::new(waiting, 1.0);
        let chart = ui.add(body, chart, ResultChart::style())?;
        Ok(Self {
            panel,
            input,
            grid,
            status,
            chart,
            caption,
            fit: true,
            stream: None,
            state: "Type a query and press Enter".to_owned(),
            started: Instant::now(),
        })
    }

    pub(crate) fn is_running(&self) -> bool {
        self.stream.is_some()
    }

    /// Runs the query in the input over the sources as they are now, replacing the one
    /// running; `waker` wakes the window as the result comes.
    pub(crate) fn run(
        &mut self,
        ui: &mut Ui,
        engine: &Engine,
        sources: &mut [Source],
        waker: Waker,
    ) {
        // Dropping the stream cancels the query running.
        self.stream = None;
        let query = ui
            .widget::<TextInput>(self.input)
            .map(|input| input.text().trim().to_owned())
            .unwrap_or_default();
        if query.is_empty() {
            self.state = "Type a query and press Enter".to_owned();
            return;
        }
        // Sources that changed since the last query: SQL sees their rows as they are now. One
        // that cannot be registered is named, and the query runs over the others.
        let mut skipped = Vec::new();
        for source in sources.iter_mut().filter(|s| s.changed) {
            let registered = source.snapshot(ui).and_then(|(schema, batches)| {
                let _ = engine.deregister(&source.name);
                let registered = engine.register_batches(&source.name, &schema, &batches);
                registered.map_err(|e| e.to_string())
            });
            match registered {
                Ok(()) => source.changed = false,
                Err(error) => skipped.push(format!("{} is not queryable ({error})", source.name)),
            }
        }
        let stream = engine.sql(query).row_numbers(NUMBER).run(move || {
            waker.wake();
        });
        self.stream = Some(stream);
        self.state = match skipped.is_empty() {
            true => "Running".to_owned(),
            false => format!("Running; {}", skipped.join(", ")),
        };
        self.started = Instant::now();
    }

    /// Takes in what the query has sent, until `deadline`. Whether some is left.
    pub(crate) fn take(&mut self, ui: &mut Ui, deadline: Instant) -> bool {
        loop {
            let Some(stream) = self.stream.as_mut() else {
                return false;
            };
            if Instant::now() >= deadline {
                return true;
            }
            let Some(event) = stream.try_next() else {
                return false;
            };
            match event {
                QueryEvent::Schema(schema) => {
                    if let Err(error) = self.replace(ui, schema) {
                        // Its rows would land in the last result's table: stop it, and
                        // clear that result so it is not taken for this one's.
                        self.stream = None;
                        let _ = self.replace(ui, waiting());
                        let caption = "Not charted: the result cannot be shown".to_owned();
                        self.set_chart(ui, Plotted::not_charted(caption));
                        self.state = format!("Cannot show the result: {error}");
                    }
                }
                QueryEvent::Batch(batch) => {
                    self.chart_rows(ui, &batch);
                    let mut refused = None;
                    let mut rows = 0;
                    ui.update::<Grid, _>(self.grid, |grid, cx| {
                        let table = grid.model_mut(cx).table_mut();
                        refused = table.upsert(batch).err();
                        rows = table.len();
                    });
                    if let Some(error) = refused {
                        self.state = format!("Rows refused: {error}");
                    }
                    if rows >= MAX_ROWS {
                        self.stream = None;
                        self.finish_chart(ui);
                        self.state = format!("Cut off at {} rows", grouped(MAX_ROWS as u64));
                    }
                }
                QueryEvent::Finished => {
                    let seconds = self.started.elapsed().as_secs_f64();
                    self.state = format!("Done in {seconds:.2} s");
                    self.stream = None;
                    self.finish_chart(ui);
                }
                QueryEvent::Failed { reason } => {
                    let _ = self.replace(ui, waiting());
                    let failed = "Not charted: the query failed".to_owned();
                    self.set_chart(ui, Plotted::not_charted(failed));
                    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
                    self.state = format!("Failed: {reason}");
                    self.stream = None;
                }
                _ => {}
            }
        }
    }

    /// A new result: a new table and chart, the last result's rows gone.
    fn replace(&mut self, ui: &mut Ui, schema: Arc<Schema>) -> Result<(), String> {
        let fresh = LiveTable::new(schema.clone(), NUMBER).map_err(|e| e.to_string())?;
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            *grid.model_mut(cx).table_mut() = fresh
        });
        self.set_chart(ui, Plotted::new(&schema, NUMBER));
        Ok(())
    }

    /// Charts something else, showing all of it as it comes.
    fn set_chart(&mut self, ui: &mut Ui, plotted: Plotted) {
        let caption = plotted.caption().to_owned();
        ui.update::<ResultChart, _>(self.chart, |chart, cx| {
            *chart.model_mut(cx) = plotted;
        });
        ui.update::<Label, _>(self.caption, |label, cx| label.set_text(cx, caption));
        self.fit = true;
    }

    /// Charts rows of the result, showing all of it unless the user moved the chart. Rows
    /// it cannot take in end the chart, saying why: a chart missing rows would mislead.
    fn chart_rows(&mut self, ui: &mut Ui, batch: &RecordBatch) {
        let fit = self.fit;
        let mut refused = None;
        ui.update::<ResultChart, _>(self.chart, |chart, cx| {
            refused = chart.model_mut(cx).add(batch).err();
            if fit {
                show_all(chart, cx);
            }
        });
        if let Some(error) = refused {
            self.set_chart(ui, Plotted::not_charted(format!("Not charted: {error}")));
        }
    }

    /// The whole result has arrived: the chart shows the last of it.
    fn finish_chart(&mut self, ui: &mut Ui) {
        let fit = self.fit;
        let mut caption = String::new();
        ui.update::<ResultChart, _>(self.chart, |chart, cx| {
            chart.model_mut(cx).finish();
            if fit {
                show_all(chart, cx);
            }
            chart.model().caption().clone_into(&mut caption);
        });
        ui.update::<Label, _>(self.caption, |label, cx| label.set_text(cx, caption));
    }

    /// Follows what happened to the result's grid and chart: a row selected in the grid is
    /// marked on the chart, and a chart the user moved stays where they put it, until it
    /// shows all of the result again.
    pub(crate) fn action(&mut self, ui: &mut Ui, action: &Action) {
        if action.source == self.grid && matches!(action.kind, ActionKind::Selected(_)) {
            self.mark_selected(ui);
        } else if action.source == self.chart && action.kind == ActionKind::Changed {
            let chart = ui.widget::<ResultChart>(self.chart);
            self.fit = chart.is_some_and(shows_all);
        }
    }

    /// Marks the grid's selected row on the chart, moving the chart to it if it is out of
    /// sight.
    fn mark_selected(&mut self, ui: &mut Ui) {
        let key = ui.widget::<Grid>(self.grid).and_then(selected_key);
        let mut moved = false;
        ui.update::<ResultChart, _>(self.chart, |chart, cx| {
            let Some(x) = chart.model_mut(cx).mark_row(key) else {
                return;
            };
            if let Some(shown) = chart.x_axis() {
                if x < shown.start() || x > shown.end() {
                    let middle = shown.start() + shown.span() / 2.0;
                    chart.show(cx, shown.shifted(x - middle));
                    moved = true;
                }
            }
        });
        if moved {
            self.fit = false;
        }
    }

    /// Writes where the query stands under its result.
    pub(crate) fn show_status(&self, ui: &mut Ui) {
        let rows = ui
            .widget::<Grid>(self.grid)
            .map_or(0, |grid| grid.model().table().len());
        let text = format!("{}: {} rows", self.state, grouped(rows as u64));
        ui.update::<Label, _>(self.status, |label, cx| label.set_text(cx, text));
    }
}

/// Shows all of the chart's data.
fn show_all(chart: &mut ResultChart, cx: &mut EventCx<'_>) {
    let model = chart.model();
    if let Some((low, high)) = model.x_extent() {
        let all = Axis::new(low, high, model.x_kind()).padded(0.02);
        chart.show(cx, all);
    }
}

/// Whether the chart shows all of its data.
fn shows_all(chart: &ResultChart) -> bool {
    let (Some(shown), Some((low, high))) = (chart.x_axis(), chart.model().x_extent()) else {
        return false;
    };
    shown.start() <= low && shown.end() >= high
}

/// The key of the row selected in `grid`, if one is.
fn selected_key(grid: &Grid) -> Option<u64> {
    let model = grid.model();
    let table = model.table();
    let at = table.row_of(model.record(grid.selected()?)?)?;
    let keys = table.batches().get(at.batch)?.column_by_name(NUMBER)?;
    let keys = keys.as_any().downcast_ref::<UInt64Array>()?;
    keys.is_valid(at.row).then(|| keys.value(at.row))
}

/// Columns come with a result: until then, an empty table of the key alone.
fn waiting() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        NUMBER,
        DataType::UInt64,
        false,
    )]))
}

#[cfg(test)]
mod tests {
    use arrow_array::{ArrayRef, Float64Array, Int64Array};
    use densa_draw::{Canvas, InstanceBuffer, Size};
    use densa_paragraph::TextSystem;
    use densa_text::{AtlasConfig, GlyphCache};

    use super::*;

    /// A SQL panel in a dock, showing a result of `x` and `y` (numbered from 0) as it would
    /// come from a query.
    fn panel(x: &[i64], y: &[f64]) -> (Ui, Query) {
        let mut ui = Ui::new(TextSystem::bundled().unwrap());
        ui.resize(Size::new(1_000.0, 600.0));
        let dock = ui
            .add(ui.root(), DockArea::new(), DockArea::style())
            .unwrap();
        let mut query = Query::add(&mut ui, dock, DockAt::First, "").unwrap();
        let rows = rows(0, x, y);
        query.replace(&mut ui, rows.schema()).unwrap();
        take(&mut ui, &mut query, rows);
        (ui, query)
    }

    fn rows(first: u64, x: &[i64], y: &[f64]) -> RecordBatch {
        let count = x.len() as u64;
        RecordBatch::try_from_iter([
            ("x", Arc::new(Int64Array::from(x.to_vec())) as ArrayRef),
            ("y", Arc::new(Float64Array::from(y.to_vec()))),
            (
                NUMBER,
                Arc::new(UInt64Array::from_iter_values(first..first + count)),
            ),
        ])
        .unwrap()
    }

    /// Takes in rows as `take` does: into the grid and the chart.
    fn take(ui: &mut Ui, query: &mut Query, rows: RecordBatch) {
        query.chart_rows(ui, &rows);
        ui.update::<Grid, _>(query.grid, |grid, cx| {
            grid.model_mut(cx).table_mut().upsert(rows).unwrap();
        });
    }

    /// Lays out and paints a frame: the grid catches up with its rows when painted.
    fn frame(ui: &mut Ui) {
        let mut cache = GlyphCache::new(AtlasConfig::default());
        let mut out = InstanceBuffer::with_capacity(65_536);
        ui.layout();
        ui.paint(&mut Canvas::begin(&mut cache, &mut out));
    }

    fn chart(ui: &Ui, query: &Query) -> (f64, f64) {
        let x = ui
            .widget::<ResultChart>(query.chart)
            .unwrap()
            .x_axis()
            .unwrap();
        (x.start(), x.end())
    }

    fn caption(ui: &Ui, query: &Query) -> String {
        ui.widget::<Label>(query.caption).unwrap().text().to_owned()
    }

    #[test]
    fn the_chart_shows_the_whole_result_until_the_user_moves_it() {
        let (mut ui, mut query) = panel(&[10, 20], &[1.0, 2.0]);
        assert_eq!(caption(&ui, &query), "x across; y below");
        let (start, end) = chart(&ui, &query);
        assert!(
            start < 10.0 && start > 9.0 && end > 20.0 && end < 21.0,
            "{start}..{end}"
        );

        take(&mut ui, &mut query, rows(2, &[30, 40], &[0.0, 4.0]));
        let (start, end) = chart(&ui, &query);
        assert!(
            start < 10.0 && end > 40.0,
            "grew with the result: {start}..{end}"
        );

        let moved = Action {
            source: query.chart,
            kind: ActionKind::Changed,
        };
        ui.update::<ResultChart, _>(query.chart, |chart, cx| {
            chart.show(cx, Axis::number(10.0, 20.0));
        });
        query.action(&mut ui, &moved);
        take(&mut ui, &mut query, rows(4, &[100], &[1.0]));
        let (start, end) = chart(&ui, &query);
        assert!(
            start > 9.0 && end < 21.0,
            "left where the user put it: {start}..{end}"
        );

        // Zoomed out to all of it: it follows the result again.
        ui.update::<ResultChart, _>(query.chart, |chart, cx| {
            chart.show(cx, Axis::number(-10.0, 110.0));
        });
        query.action(&mut ui, &moved);
        take(&mut ui, &mut query, rows(5, &[200], &[1.0]));
        let (start, end) = chart(&ui, &query);
        assert!(
            start > 5.0 && start < 10.0 && end > 200.0,
            "fits again: {start}..{end}"
        );

        query.replace(&mut ui, rows(0, &[], &[]).schema()).unwrap();
        take(&mut ui, &mut query, rows(0, &[5, 6], &[1.0, 1.0]));
        let (start, end) = chart(&ui, &query);
        assert!(
            start < 5.0 && start > 4.0 && end > 6.0,
            "a new result fits again"
        );
    }

    #[test]
    fn the_row_selected_in_the_grid_is_marked_and_brought_into_sight() {
        let (mut ui, mut query) = panel(&[10, 20, 30], &[1.0, 2.0, 3.0]);
        frame(&mut ui);
        let select = |ui: &mut Ui, query: &mut Query, row: Option<usize>| {
            ui.update::<Grid, _>(query.grid, |grid, cx| grid.select(cx, row));
            let selected = Action {
                source: query.grid,
                kind: ActionKind::Selected(row.unwrap_or(0)),
            };
            query.action(ui, &selected);
            ui.widget::<ResultChart>(query.chart)
                .unwrap()
                .model()
                .mark()
        };
        assert_eq!(select(&mut ui, &mut query, Some(1)), Some(20.0));
        assert_eq!(select(&mut ui, &mut query, None), None);

        ui.update::<ResultChart, _>(query.chart, |chart, cx| {
            chart.show(cx, Axis::number(10.0, 14.0));
        });
        // Away from the newest row: a view reaching the newest follows it.
        assert_eq!(select(&mut ui, &mut query, Some(1)), Some(20.0));
        let (start, end) = chart(&ui, &query);
        assert!(start < 20.0 && end > 20.0, "moved to it: {start}..{end}");
        assert!(end - start < 5.0, "as wide as it was: {start}..{end}");
        take(&mut ui, &mut query, rows(3, &[1_000], &[1.0]));
        assert_eq!(chart(&ui, &query), (start, end), "and left there");
    }

    #[test]
    fn a_view_must_reach_both_ends_to_fit_again_and_a_new_result_always_does() {
        let (mut ui, mut query) = panel(&[10, 20], &[1.0, 2.0]);
        let moved = Action {
            source: query.chart,
            kind: ActionKind::Changed,
        };
        ui.update::<ResultChart, _>(query.chart, |chart, cx| {
            chart.show(cx, Axis::number(16.0, 21.0));
        });
        query.action(&mut ui, &moved);
        assert!(!query.fit, "the start is out of sight");

        query.replace(&mut ui, rows(0, &[], &[]).schema()).unwrap();
        take(&mut ui, &mut query, rows(0, &[100, 200], &[1.0, 1.0]));
        let (start, end) = chart(&ui, &query);
        assert!(
            start < 100.0 && end > 200.0,
            "a new result fits: {start}..{end}"
        );
    }

    #[test]
    fn a_fitted_chart_is_not_taken_for_one_following_the_newest() {
        let (ui, query) = panel(&[10, 20, 30], &[1.0, 2.0, 3.0]);
        let chart = ui.widget::<ResultChart>(query.chart).unwrap();
        assert!(
            !chart.is_following(),
            "it would show only the newest span as rows come"
        );
        assert!(shows_all(chart));
    }

    #[test]
    fn a_new_result_clears_the_mark_and_an_empty_one_says_so() {
        let (mut ui, mut query) = panel(&[10, 20], &[1.0, 2.0]);
        frame(&mut ui);
        ui.update::<Grid, _>(query.grid, |grid, cx| grid.select(cx, Some(1)));
        let selected = Action {
            source: query.grid,
            kind: ActionKind::Selected(1),
        };
        query.action(&mut ui, &selected);
        let mark = |ui: &Ui, query: &Query| {
            ui.widget::<ResultChart>(query.chart)
                .unwrap()
                .model()
                .mark()
        };
        assert_eq!(mark(&ui, &query), Some(20.0));
        query.replace(&mut ui, rows(0, &[], &[]).schema()).unwrap();
        assert_eq!(mark(&ui, &query), None);
        query.finish_chart(&mut ui);
        assert_eq!(
            caption(&ui, &query),
            "Not charted: no row has a value for x"
        );
    }

    #[test]
    fn rows_the_chart_cannot_take_end_it_and_say_why() {
        let (mut ui, mut query) = panel(&[1], &[1.0]);
        let signed = RecordBatch::try_from_iter([
            ("x", Arc::new(Int64Array::from(vec![2])) as ArrayRef),
            ("y", Arc::new(Float64Array::from(vec![2.0]))),
            (NUMBER, Arc::new(Int64Array::from(vec![1]))),
        ])
        .unwrap();
        query.chart_rows(&mut ui, &signed);
        assert!(
            caption(&ui, &query).starts_with("Not charted: "),
            "{}",
            caption(&ui, &query)
        );
        assert!(ui
            .widget::<ResultChart>(query.chart)
            .unwrap()
            .x_axis()
            .is_none());
    }

    #[test]
    fn a_failed_query_says_so_in_the_chart() {
        let (mut ui, mut query) = panel(&[1], &[1.0]);
        let failed = "Not charted: the query failed".to_owned();
        query.set_chart(&mut ui, Plotted::not_charted(failed.clone()));
        assert_eq!(caption(&ui, &query), failed);
        assert!(ui
            .widget::<ResultChart>(query.chart)
            .unwrap()
            .x_axis()
            .is_none());
    }
}
