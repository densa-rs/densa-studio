//! The SQL panel: a query over every source, run by DataFusion, its result streamed into a
//! grid as it is computed.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field, Schema};
use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_datafusion::{Engine, QueryEvent, QueryStream};
use densa_ui::{Align, Style, Ui, WidgetId};
use densa_widgets::{DockArea, DockAt, DockPanel, Label, Panel, TextInput};
use densa_window::Waker;

use crate::source::{grouped, Grid, Source};

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
        Ok(Self {
            panel,
            input,
            grid,
            status,
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
                        self.state = format!("Cannot show the result: {error}");
                    }
                }
                QueryEvent::Batch(batch) => {
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
                        self.state = format!("Cut off at {} rows", grouped(MAX_ROWS as u64));
                    }
                }
                QueryEvent::Finished => {
                    let seconds = self.started.elapsed().as_secs_f64();
                    self.state = format!("Done in {seconds:.2} s");
                    self.stream = None;
                }
                QueryEvent::Failed { reason } => {
                    let _ = self.replace(ui, waiting());
                    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
                    self.state = format!("Failed: {reason}");
                    self.stream = None;
                }
                _ => {}
            }
        }
    }

    /// A new result: a new table, the last result's rows gone.
    fn replace(&mut self, ui: &mut Ui, schema: Arc<Schema>) -> Result<(), String> {
        let fresh = LiveTable::new(schema, NUMBER).map_err(|e| e.to_string())?;
        ui.update::<Grid, _>(self.grid, |grid, cx| {
            *grid.model_mut(cx).table_mut() = fresh
        });
        Ok(())
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

/// Columns come with a result: until then, an empty table of the key alone.
fn waiting() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        NUMBER,
        DataType::UInt64,
        false,
    )]))
}
