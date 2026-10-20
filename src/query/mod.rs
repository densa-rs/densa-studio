//! The SQL panel: a query over every source, run by DataFusion, its result streamed into a
//! grid as it is computed, and charted in a tab of its own; live, it runs again each second
//! while its sources change, its result updated in place.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use densa_arrow::LiveModel;
use densa_chart::{Axis, ChartModel};
use densa_data::LiveTable;
use densa_datafusion::{Engine, QueryEvent, QueryStream};
use densa_ui::{Action, ActionKind, Align, EventCx, Style, Ui, WidgetId};
use densa_widgets::{
    Button, Chart, DockArea, DockAt, DockPanel, DockSide, Label, Panel, TextInput,
};
use densa_window::Waker;

use crate::chart::Plotted;
use crate::source::{grouped, Grid, Source};

type ResultChart = Chart<Plotted>;

/// The key column of a result: its rows are numbered as they arrive. Not `row`, which every
/// source has.
const NUMBER: &str = "#";
/// Rows shown at most: a larger result is cut off (its query cancelled).
const MAX_ROWS: usize = 1_000_000;
/// How often a live query runs again, at most, while its sources change.
pub(crate) const LIVE_EVERY: Duration = Duration::from_secs(1);

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
    /// Whether the query runs again while its sources change, and the button saying so.
    live: bool,
    live_button: WidgetId,
    /// The query last run from the input (a live query runs it again, not what is being
    /// typed).
    ran: String,
    /// The run under way, if it runs again in place of the last one's result.
    refresh: Option<Refresh>,
    /// The last result's columns, as the query gave them (its table makes them nullable).
    shown: Option<Arc<Schema>>,
}

/// A run in place of the last one's result: its rows replace those of the same number as
/// they come, rows past its last are deleted when it ends, and its chart, made aside, takes
/// the last one's place then.
struct Refresh {
    /// Whether its columns are the last result's (until they are known, `false`).
    same: bool,
    rows: u64,
    chart: Plotted,
}

impl Query {
    /// Adds the panel to `dock`, placed `at`, holding `query`, running it again while its
    /// sources change if `live`.
    pub(crate) fn add(
        ui: &mut Ui,
        dock: WidgetId,
        at: DockAt,
        query: &str,
        live: bool,
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
        let live_button = ui.add(bar, Button::new(live_label(live)), Style::row().shrink(0.0))?;
        let model = LiveModel::new(LiveTable::new(waiting(), NUMBER)?)?;
        let grid = ui.add(body, Grid::new(model), Grid::style())?;
        let line = Style::row().padding(4.0).shrink(0.0);
        let line = ui.add(body, Panel::transparent(), line)?;
        let status = ui.add(line, Label::new("").dim(), Style::row())?;

        let spec = DockPanel::new("Chart").with_key("chart");
        let right = DockAt::Beside(panel, DockSide::Right);
        let chart_panel = DockArea::add_panel(ui, dock, Panel::new(), spec, right)?;
        let body = Style::column().grow(1.0).padding(6.0).gap(6.0);
        let body = ui.add(chart_panel, Panel::transparent(), body)?;
        let line = ui.add(body, Panel::transparent(), Style::row().shrink(0.0))?;
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
            live,
            live_button,
            ran: String::new(),
            refresh: None,
            shown: None,
        })
    }

    pub(crate) fn is_running(&self) -> bool {
        self.stream.is_some()
    }

    /// Whether the query runs again while its sources change.
    pub(crate) fn is_live(&self) -> bool {
        self.live
    }

    /// Whether a live query should run again: it is not running, a source changed since,
    /// and it last ran a while ago.
    pub(crate) fn is_due(&self, changed: bool) -> bool {
        self.live
            && changed
            && self.stream.is_none()
            && !self.ran.is_empty()
            && self.started.elapsed() >= LIVE_EVERY
    }

    /// Runs the query in the input over the sources as they are now, replacing the one
    /// running; `waker` wakes the window as the result comes. With `again`, runs the query
    /// last run instead, its result updating the last one's in place.
    pub(crate) fn run(
        &mut self,
        ui: &mut Ui,
        engine: &Engine,
        sources: &mut [Source],
        waker: Waker,
        again: bool,
    ) {
        // Dropping the stream cancels the query running.
        self.stream = None;
        self.refresh = None;
        let query = match again {
            true => self.ran.clone(),
            false => ui
                .widget::<TextInput>(self.input)
                .map(|input| input.text().trim().to_owned())
                .unwrap_or_default(),
        };
        if again {
            self.refresh = Some(Refresh {
                same: false,
                rows: 0,
                chart: Plotted::waiting(),
            });
        }
        self.ran.clone_from(&query);
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
        self.started = Instant::now();
        if again {
            // The last result stays in sight: no word of a run each second.
            return;
        }
        self.state = match skipped.is_empty() {
            true => "Running".to_owned(),
            false => format!("Running; {}", skipped.join(", ")),
        };
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
                QueryEvent::Schema(schema) => self.columns(ui, schema),
                QueryEvent::Batch(batch) => self.rows(ui, batch),
                QueryEvent::Finished => {
                    let seconds = self.started.elapsed().as_secs_f64();
                    self.state = format!("Done in {seconds:.2} s");
                    self.end(ui);
                }
                QueryEvent::Failed { reason } => self.failed(ui, &reason),
                _ => {}
            }
        }
    }

    /// The query failed: its result is cleared, unless it ran again, whose result stays (its
    /// rows may have changed in part already) and runs again next time.
    fn failed(&mut self, ui: &mut Ui, reason: &str) {
        let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
        self.stream = None;
        if self.refresh.take().is_some() {
            self.state = format!("The last run failed: {reason}");
            return;
        }
        let _ = self.replace(ui, waiting());
        let failed = "Not charted: the query failed".to_owned();
        self.set_chart(ui, Plotted::not_charted(failed));
        self.state = format!("Failed: {reason}");
    }

    /// The result's columns: a new table and chart, unless a run again brings the last
    /// result's columns, whose rows it updates in place.
    fn columns(&mut self, ui: &mut Ui, schema: Arc<Schema>) {
        if let Some(refresh) = &mut self.refresh {
            if self.shown.as_ref() == Some(&schema) {
                refresh.same = true;
                refresh.chart = Plotted::new(&schema, NUMBER);
                return;
            }
            // Other columns: a result of its own.
            self.refresh = None;
        }
        if let Err(error) = self.replace(ui, schema) {
            // Its rows would land in the last result's table: stop it, and clear that
            // result so it is not taken for this one's.
            self.stream = None;
            let _ = self.replace(ui, waiting());
            let caption = "Not charted: the result cannot be shown".to_owned();
            self.set_chart(ui, Plotted::not_charted(caption));
            self.state = format!("Cannot show the result: {error}");
        }
    }

    /// Rows of the result, into the grid and the chart (the one being made aside, for a
    /// run again).
    fn rows(&mut self, ui: &mut Ui, batch: RecordBatch) {
        match &mut self.refresh {
            Some(refresh) => {
                refresh.rows += batch.num_rows() as u64;
                if let Err(error) = refresh.chart.add(&batch) {
                    refresh.chart = Plotted::not_charted(format!("Not charted: {error}"));
                }
            }
            None => self.chart_rows(ui, &batch),
        }
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
            self.state = format!("Cut off at {} rows", grouped(MAX_ROWS as u64));
            self.end(ui);
        }
    }

    /// The result has ended: the chart shows the last of it; a run again's rows past its
    /// last are deleted, and its chart takes the last one's place.
    fn end(&mut self, ui: &mut Ui) {
        self.stream = None;
        match self.refresh.take() {
            Some(refresh) => self.end_refresh(ui, refresh),
            None => self.finish_chart(ui),
        }
    }

    /// A run again has ended: rows past its last are deleted (rows are numbered from 0 each
    /// run, so the table's keys are always `0..len`), and its chart takes the last one's
    /// place where the user left it, the selected row marked again.
    fn end_refresh(&mut self, ui: &mut Ui, mut refresh: Refresh) {
        let kept = refresh.rows;
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            let table = grid.model_mut(cx).table_mut();
            let len = table.len() as u64;
            if len > kept {
                // Keys of the table's own type: deleting them cannot fail.
                let _ = table.delete(&UInt64Array::from_iter_values(kept..len));
            }
        });
        refresh.chart.finish();
        let caption = refresh.chart.caption().to_owned();
        let key = ui.widget::<Grid>(self.grid).and_then(selected_key);
        let fit = self.fit;
        ui.update::<ResultChart, _>(self.chart, |chart, cx| {
            let model = chart.model_mut(cx);
            *model = refresh.chart;
            model.mark_row(key);
            if fit {
                show_all(chart, cx);
            }
        });
        ui.update::<Label, _>(self.caption, |label, cx| label.set_text(cx, caption));
    }

    /// A new result: a new table and chart, the last result's rows gone.
    fn replace(&mut self, ui: &mut Ui, schema: Arc<Schema>) -> Result<(), String> {
        let fresh = LiveTable::new(schema.clone(), NUMBER).map_err(|e| e.to_string())?;
        self.shown = Some(schema.clone());
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
        } else if action.source == self.live_button && action.kind == ActionKind::Clicked {
            self.live = !self.live;
            let label = live_label(self.live);
            ui.update::<Button, _>(self.live_button, |button, cx| button.set_label(cx, label));
            self.show_status(ui);
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
        let mut text = format!("{}: {} rows", self.state, grouped(rows as u64));
        if self.live {
            text.push_str("; live, run again each second while its sources change");
        }
        ui.update::<Label, _>(self.status, |label, cx| label.set_text(cx, text));
    }
}

/// What the live button says.
fn live_label(live: bool) -> &'static str {
    match live {
        true => "Live: on",
        false => "Live: off",
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
mod tests;
