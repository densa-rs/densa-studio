//! Tests of the SQL panel.

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
    let mut query = Query::add(&mut ui, dock, DockAt::First, "", false).unwrap();
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
    query.action(&mut ui, &moved, || {});
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
    query.action(&mut ui, &moved, || {});
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
        query.action(ui, &selected, || {});
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
    query.action(&mut ui, &moved, || {});
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
    query.action(&mut ui, &selected, || {});
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

/// Starts a run again, as `run` does with `again`, without DataFusion.
fn again(query: &mut Query) {
    query.refresh = Some(Refresh {
        rows: 0,
        chart: Plotted::waiting(),
    });
}

fn table_len(ui: &Ui, query: &Query) -> usize {
    ui.widget::<Grid>(query.grid).unwrap().model().table().len()
}

#[test]
fn a_run_again_updates_the_result_in_place() {
    let (mut ui, mut query) = panel(&[10, 20, 30], &[1.0, 2.0, 3.0]);
    frame(&mut ui);
    ui.update::<Grid, _>(query.grid, |grid, cx| grid.select(cx, Some(1)));
    again(&mut query);
    query.columns(&mut ui, rows(0, &[], &[]).schema());
    assert!(query.refresh.is_some(), "the same columns: in place");
    query.rows(&mut ui, rows(0, &[11, 21], &[1.0, 2.0]));
    assert_eq!(
        table_len(&ui, &query),
        3,
        "the last row stays until the run ends"
    );
    let shown = |ui: &Ui, query: &Query| {
        ui.widget::<ResultChart>(query.chart)
            .unwrap()
            .model()
            .x_extent()
    };
    assert_eq!(
        shown(&ui, &query),
        Some((10.0, 30.0)),
        "the last chart, meanwhile"
    );
    query.end(&mut ui);
    assert_eq!(table_len(&ui, &query), 2);
    assert_eq!(shown(&ui, &query), Some((11.0, 21.0)));
    let grid = ui.widget::<Grid>(query.grid).unwrap();
    assert_eq!(grid.selected(), Some(1), "the selection stays");
    let mark = ui
        .widget::<ResultChart>(query.chart)
        .unwrap()
        .model()
        .mark();
    assert_eq!(mark, Some(21.0), "and is marked on the new chart");
    assert!(query.refresh.is_none() && query.stream.is_none());

    again(&mut query);
    query.columns(&mut ui, rows(0, &[], &[]).schema());
    query.rows(&mut ui, rows(0, &[5, 6, 7, 8], &[1.0; 4]));
    query.end(&mut ui);
    assert_eq!(table_len(&ui, &query), 4, "a run with more rows");
    assert_eq!(shown(&ui, &query), Some((5.0, 8.0)));
}

#[test]
fn a_run_again_with_other_columns_is_a_result_of_its_own() {
    let (mut ui, mut query) = panel(&[10, 20], &[1.0, 2.0]);
    again(&mut query);
    let other = RecordBatch::try_from_iter([
        ("z", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (NUMBER, Arc::new(UInt64Array::from(vec![0]))),
    ])
    .unwrap();
    query.columns(&mut ui, other.schema());
    assert!(query.refresh.is_none());
    assert_eq!(table_len(&ui, &query), 0, "a new table");
    assert_eq!(
        caption(&ui, &query),
        "Not charted: no number column after z"
    );
}

#[test]
fn a_failed_run_again_keeps_the_last_result() {
    let (mut ui, mut query) = panel(&[10, 20], &[1.0, 2.0]);
    again(&mut query);
    query.failed(&mut ui, "table   gone");
    assert!(
        query.state.ends_with("it runs again: table gone"),
        "{}",
        query.state
    );
    assert_eq!(table_len(&ui, &query), 2);
    assert_eq!(caption(&ui, &query), "x across; y below");
    query.failed(&mut ui, "bad");
    assert_eq!(query.state, "Failed: bad");
    assert_eq!(table_len(&ui, &query), 0);
}

#[test]
fn a_live_query_runs_again_when_a_source_changed_a_while_after_its_last_run() {
    let (mut ui, mut query) = panel(&[10], &[1.0]);
    query.ran = "SELECT 1".to_owned();
    query.ended -= LIVE_EVERY;
    assert!(!query.is_due(true), "not live");
    let click = Action {
        source: query.live_button,
        kind: ActionKind::Clicked,
    };
    query.action(&mut ui, &click, || {});
    assert!(query.is_live());
    assert_eq!(
        ui.widget::<Button>(query.live_button).unwrap().label(),
        "Live: on"
    );
    assert!(query.is_due(true));
    assert!(!query.is_due(false), "nothing changed");
    query.ended = Instant::now();
    assert!(!query.is_due(true), "it ran a moment ago");
    query.ended -= LIVE_EVERY;
    query.ran.clear();
    assert!(!query.is_due(true), "nothing has run");
    query.action(&mut ui, &click, || {});
    assert_eq!(
        ui.widget::<Button>(query.live_button).unwrap().label(),
        "Live: off"
    );
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

#[test]
fn a_run_again_after_a_cut_off_result_counts_its_own_rows() {
    let (mut ui, mut query) = panel(&[1, 2, 3, 4], &[1.0; 4]);
    query.max_rows = 4;
    again(&mut query);
    query.columns(&mut ui, rows(0, &[], &[]).schema());
    query.rows(&mut ui, rows(0, &[5, 6], &[1.0; 2]));
    assert!(query.refresh.is_some(), "two rows of four: still running");
    query.rows(&mut ui, rows(2, &[7], &[1.0]));
    query.end(&mut ui);
    assert_eq!(table_len(&ui, &query), 3, "every row of this run kept");
    again(&mut query);
    query.columns(&mut ui, rows(0, &[], &[]).schema());
    query.rows(&mut ui, rows(0, &[1, 2, 3, 4, 5], &[1.0; 5]));
    assert!(query.refresh.is_none(), "this run reached the most shown");
    assert!(
        query.state.starts_with("Cut off at 4 rows"),
        "{}",
        query.state
    );
}

#[test]
fn rows_refused_in_a_run_again_fail_it_and_it_runs_again() {
    let (mut ui, mut query) = panel(&[1, 2], &[1.0; 2]);
    query.live = true;
    query.ran = "SELECT 1".to_owned();
    again(&mut query);
    query.columns(&mut ui, rows(0, &[], &[]).schema());
    let unnumbered = RecordBatch::try_from_iter([
        ("x", Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef),
        ("y", Arc::new(Float64Array::from(vec![1.0]))),
        (NUMBER, Arc::new(UInt64Array::from(vec![None]))),
    ])
    .unwrap();
    query.rows(&mut ui, unnumbered);
    assert!(query.refresh.is_none());
    assert!(query.state.contains("runs again"), "{}", query.state);
    query.ended -= LIVE_EVERY;
    assert!(query.is_due(false), "even with no source changed");
    assert_eq!(table_len(&ui, &query), 2, "the last result stays");
}

#[test]
fn sql_reads_a_file_read_a_page_at_a_time_itself() {
    use arrow_array::StringArray;
    use parquet::arrow::ArrowWriter;

    let name = format!("densa-studio-{}-sql.parquet", std::process::id());
    let path = std::env::temp_dir().join(name);
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1_000));
    let names: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..1_000).map(|i| format!("n{i}")),
    ));
    let batch = RecordBatch::try_from_iter([("id", ids), ("name", names)]).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut ui = Ui::new(TextSystem::bundled().unwrap());
    let dock = ui
        .add(ui.root(), DockArea::new(), DockArea::style())
        .unwrap();
    let opened = Source::open_file(&mut ui, dock, &path, "big".into(), DockAt::First, || {}, 0);
    let engine = Engine::new().unwrap();
    let mut sources = [opened.unwrap()];
    let skipped = register(&ui, &engine, &mut sources);
    let schema = engine.schema("SELECT * FROM big");
    let _ = std::fs::remove_file(&path);
    assert!(skipped.is_empty(), "{skipped:?}");
    let schema = schema.unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        ["id", "name"],
        "the file's own columns: no row numbers"
    );
    assert!(!sources[0].changed);
}

#[test]
fn sources_sql_cannot_see_are_named_and_not_tried_until_they_change() {
    use densa_datafusion::Engine;

    let mut ui = Ui::new(TextSystem::bundled().unwrap());
    let dock = ui
        .add(ui.root(), DockArea::new(), DockArea::style())
        .unwrap();
    // Nothing answers there: the stream has no rows, so no columns for SQL.
    let spec = crate::stream::StreamSpec::parse("ws://127.0.0.1:9/quiet").unwrap();
    let mut source = Source::open_stream(
        &mut ui,
        dock,
        &spec,
        "quiet".to_owned(),
        DockAt::First,
        || {},
    )
    .unwrap();
    source.changed = true;
    let engine = Engine::new().unwrap();
    let mut sources = [source];
    let skipped = register(&ui, &engine, &mut sources);
    assert_eq!(skipped, ["quiet is not queryable (no rows yet)"]);
    assert!(!sources[0].changed, "not tried again each second");
}

#[test]
fn a_live_query_does_not_run_again_while_it_runs_and_a_run_forgets_the_last_failure() {
    use densa_datafusion::Engine;

    let (mut ui, mut query) = panel(&[1], &[1.0]);
    query.live = true;
    query.retry = true;
    query.ran = "SELECT 1".to_owned();
    query.ended -= LIVE_EVERY;
    assert!(query.is_due(false), "a failed run runs again");
    let engine = Engine::new().unwrap();
    query.saved = Some("saved 1 rows to x.csv".to_owned());
    query.run(&mut ui, &engine, &mut [], || {}, true);
    assert!(query.is_running());
    assert!(!query.retry, "this run is the retry");
    assert!(query.saved.is_some(), "a live run again keeps the word");
    ui.update::<TextInput, _>(query.input, |input, cx| input.set_text(cx, "SELECT 2"));
    query.run(&mut ui, &engine, &mut [], || {}, false);
    assert!(query.saved.is_none(), "a run of one's own starts afresh");
    query.ended -= LIVE_EVERY;
    assert!(!query.is_due(true), "not while it runs");
}

/// The SQL panel's status line.
fn status(ui: &Ui, query: &Query) -> String {
    ui.widget::<Label>(query.status).unwrap().text().to_owned()
}

/// Saves as the save box asks, and waits for the save to end as frames would.
fn save_and_wait(ui: &mut Ui, query: &mut Query) {
    query.save(ui, || {});
    let until = Instant::now() + Duration::from_secs(10);
    while query.saving.is_some() && Instant::now() < until {
        query.take(ui, Instant::now());
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn the_result_is_saved_in_the_file_named_beside_the_query() {
    let (mut ui, mut query) = panel(&[1, 2, 3], &[0.5, 1.5, 2.5]);
    frame(&mut ui);
    let folder = std::env::temp_dir().join(format!("densa-studio-saved-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder).unwrap();
    let path = folder.join("out.csv");
    let name = path.display().to_string();

    save_and_wait(&mut ui, &mut query);
    assert!(
        !status(&ui, &query).contains("sav"),
        "no name, nothing done"
    );
    ui.update::<TextInput, _>(query.save_as, |input, cx| {
        input.set_text(cx, format!(" \"{name}\" "))
    });
    query.save(&mut ui, || {});
    let status_now = status(&ui, &query);
    assert!(
        status_now.ends_with(&format!("; saving 3 rows to {name}")),
        "{status_now}"
    );
    let until = Instant::now() + Duration::from_secs(10);
    while query.saving.is_some() && Instant::now() < until {
        query.take(&mut ui, Instant::now());
    }
    let status_now = status(&ui, &query);
    assert!(
        status_now.ends_with(&format!("; saved 3 rows to {name}")),
        "{status_now}"
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "x,y\n1,0.5\n2,1.5\n3,2.5\n"
    );

    save_and_wait(&mut ui, &mut query);
    let status_now = status(&ui, &query);
    assert!(
        status_now.ends_with(&format!("; not saved: {name} exists: choose another name")),
        "{status_now}"
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "x,y\n1,0.5\n2,1.5\n3,2.5\n",
        "kept"
    );

    ui.update::<TextInput, _>(query.save_as, |input, cx| input.set_text(cx, "out.json"));
    save_and_wait(&mut ui, &mut query);
    assert!(status(&ui, &query).ends_with("; not saved: name a .parquet or .csv file"));
}

#[test]
fn a_relative_name_is_saved_under_the_current_folder_and_said_in_full() {
    let (mut ui, mut query) = panel(&[1], &[0.5]);
    frame(&mut ui);
    let name = format!("densa-studio-relative-{}.csv", std::process::id());
    let full = std::env::current_dir().unwrap().join(&name);
    let _ = std::fs::remove_file(&full);
    ui.update::<TextInput, _>(query.save_as, |input, cx| input.set_text(cx, name.clone()));
    save_and_wait(&mut ui, &mut query);
    let saved = full.exists();
    let _ = std::fs::remove_file(&full);
    assert!(saved);
    let status_now = status(&ui, &query);
    assert!(
        status_now.ends_with(&format!("; saved 1 rows to {}", full.display())),
        "{status_now}"
    );
}

#[test]
fn a_second_save_waits_for_the_first() {
    let (mut ui, mut query) = panel(&[1], &[0.5]);
    frame(&mut ui);
    let (_never_sent, outcome) = mpsc::channel();
    query.saving = Some(("elsewhere".to_owned(), 1, outcome));
    ui.update::<TextInput, _>(query.save_as, |input, cx| input.set_text(cx, "x.csv"));
    query.save(&mut ui, || {});
    assert!(status(&ui, &query).ends_with("; still saving the last result"));
    query.take(&mut ui, Instant::now());
    assert!(query.saving.is_some(), "nothing came: still saving");
    drop(_never_sent);
    query.take(&mut ui, Instant::now());
    assert!(status(&ui, &query).ends_with("; not saved: the save stopped"));
    assert!(query.saving.is_none());
}

#[test]
fn there_is_nothing_to_save_before_a_result() {
    let mut ui = Ui::new(TextSystem::bundled().unwrap());
    let dock = ui
        .add(ui.root(), DockArea::new(), DockArea::style())
        .unwrap();
    let mut query = Query::add(&mut ui, dock, DockAt::First, "", false).unwrap();
    ui.update::<TextInput, _>(query.save_as, |input, cx| input.set_text(cx, "out.csv"));
    save_and_wait(&mut ui, &mut query);
    assert!(status(&ui, &query).ends_with("; not saved: there is no result yet"));
}
