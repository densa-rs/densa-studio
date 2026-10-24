use std::sync::Arc;

use arrow_array::{ArrayRef, TimestampSecondArray, UInt64Array};
use arrow_schema::{Field, Schema};
use densa_paragraph::TextSystem;

use super::*;

/// Counts of buckets `every` seconds long starting at `starts`, `rows` each.
fn counts(every: u64, starts: &[f64], rows: &[f64]) -> Counts {
    let mut counts = Counts::new(Duration::from_secs(every), &format!("{every}s"));
    counts.set(starts, rows.to_vec());
    counts
}

#[test]
fn bars_fill_their_buckets_across() {
    let counts = counts(60, &[0.0, 60.0, 180.0], &[5.0, 7.0, 2.0]);
    assert_eq!(counts.bars().0, [30.0, 90.0, 210.0], "a bar at each middle");
    assert_eq!(
        counts.x_extent(),
        Some((0.0, 240.0)),
        "the whole of each bucket"
    );
    assert_eq!(counts.x_step(), Some(60.0));
    assert_eq!(counts.x_kind(), AxisKind::Time);
    assert_eq!(counts.pane_title(0), "rows per 60s");
    assert_eq!(counts.describe(), "Chart of the rows per 60s");
}

#[test]
fn bars_rise_from_zero_to_the_highest_in_sight() {
    let counts = counts(1, &[0.0, 1.0, 2.0, 3.0], &[5.0, 9.0, 40.0, 3.0]);
    let all = counts.y_axis(0, &Axis::time(0.0, 4.0)).unwrap();
    assert!(all.start() < 0.0 && all.start() > -5.0, "{all:?}");
    assert!(all.end() >= 40.0, "{all:?}");
    let first = counts.y_axis(0, &Axis::time(0.0, 2.0)).unwrap();
    assert!(first.end() >= 9.0 && first.end() < 40.0, "{first:?}");
    let none = Counts::new(Duration::from_secs(1), "1s");
    assert_eq!(none.x_extent(), None);
    assert!(none.y_axis(0, &Axis::time(0.0, 1.0)).is_none());
}

#[test]
fn starts_and_counts_are_read_from_the_groups() {
    let starts: ArrayRef = Arc::new(TimestampSecondArray::from(vec![Some(60), Some(120), None]));
    let rows: ArrayRef = Arc::new(UInt64Array::from(vec![3, 4, 5]));
    let batch = RecordBatch::try_from_iter([("at", starts), ("rows", rows.clone())]).unwrap();
    let read = bars(&batch).unwrap();
    assert_eq!(
        (read.starts, read.rows),
        (vec![60.0, 120.0], vec![3.0, 4.0])
    );
    assert_eq!(read.untimed, 5, "rows without a time are counted apart");
    let wrong = RecordBatch::try_from_iter([("at", rows.clone()), ("at2", rows)]).unwrap();
    assert!(bars(&wrong).is_ok(), "numbers read as numbers");
    let floats: ArrayRef = Arc::new(arrow_array::Float64Array::from(vec![1.0]));
    let wrong = RecordBatch::try_from_iter([("at", floats.clone()), ("rows", floats)]).unwrap();
    assert_eq!(bars(&wrong).err(), Some("the counts are not counts".into()));
}

/// A dock with a tab counting rows per second of `at`, and nothing counted yet.
fn opened() -> (Ui, Counted) {
    opened_with("at", Duration::from_secs(1))
}

/// A dock with a tab counting rows per `every` of `column`, and nothing counted yet.
fn opened_with(column: &str, every: Duration) -> (Ui, Counted) {
    let mut ui = Ui::new(TextSystem::bundled().unwrap());
    let dock = ui
        .add(ui.root(), DockArea::new(), DockArea::style())
        .unwrap();
    let source = DockPanel::new("source");
    let source = DockArea::add_panel(&mut ui, dock, Panel::new(), source, DockAt::First).unwrap();
    let counted = Counted::open(&mut ui, dock, source, ("t", "o"), (column, every, None)).unwrap();
    (ui, counted)
}

/// A table keyed by `id` of rows at `seconds` since the epoch.
fn table_at(seconds: impl IntoIterator<Item = i64>) -> LiveTable {
    let at = TimestampSecondArray::from_iter_values(seconds).with_timezone("+00:00");
    table_of("at", Arc::new(at))
}

/// A table keyed by `id` of a column `name` of `values`.
fn table_of(name: &str, values: ArrayRef) -> LiveTable {
    let ids = UInt64Array::from_iter_values(0..values.len() as u64);
    let batch = RecordBatch::try_from_iter([(name, values), ("id", Arc::new(ids) as _)]).unwrap();
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    table.upsert(batch).unwrap();
    table
}

fn charted(ui: &Ui, counted: &Counted) -> Vec<f64> {
    let chart = ui.widget::<CountChart>(counted.chart()).unwrap();
    chart.model().bars().1.to_vec()
}

#[test]
fn a_table_made_anew_is_counted_anew() {
    let (mut ui, mut counted) = opened();
    let mut first = table_at([0, 0, 1]);
    counted.count(&mut ui, &mut first);
    let read = counted.read(&first);
    assert!(!counted.show(&mut ui, read));
    assert_eq!(charted(&ui, &counted), [2.0, 1.0]);
    // The same rows again: the new counts' revision may well be the old one's.
    let mut again = table_at([0, 0, 1]);
    counted.count(&mut ui, &mut again);
    assert!(
        charted(&ui, &counted).is_empty(),
        "the old counts are gone at once"
    );
    let read = counted.read(&again);
    assert!(!counted.show(&mut ui, read));
    assert_eq!(
        charted(&ui, &counted),
        [2.0, 1.0],
        "and the new ones charted"
    );
    let read = counted.read(&again);
    assert!(matches!(read, Read::Same), "until they change");
}

#[test]
fn a_table_that_can_be_counted_clears_what_the_last_one_said() {
    let (mut ui, mut counted) = opened();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let mut without = LiveTable::new(schema, "id").unwrap();
    counted.count(&mut ui, &mut without);
    assert_eq!(
        counted.off.as_deref(),
        Some("the time column at is not in the rows")
    );
    counted.count(&mut ui, &mut table_at([5]));
    assert_eq!(counted.off, None);
}

#[test]
fn many_buckets_are_read_again_only_after_a_while() {
    let (mut ui, mut counted) = opened();
    let mut table = table_at(0..=READ_ALWAYS as i64);
    counted.count(&mut ui, &mut table);
    let read = counted.read(&table);
    assert!(
        matches!(read, Read::New(Ok(_))),
        "the first read is at once"
    );
    assert!(!counted.show(&mut ui, read));
    assert_eq!(charted(&ui, &counted).len(), READ_ALWAYS + 1);
    table.upsert(batch_at(9_999_999, u64::MAX)).unwrap();
    let read = counted.read(&table);
    assert!(matches!(read, Read::Later));
    assert!(counted.show(&mut ui, read), "another frame is wanted");
    let mut again = table_at(0..=READ_ALWAYS as i64);
    counted.count(&mut ui, &mut again);
    let read = counted.read(&again);
    assert!(
        matches!(read, Read::New(Ok(_))),
        "a table made anew is read at once"
    );
    assert!(!counted.show(&mut ui, read));
    counted.count(&mut ui, &mut table);
    refreshed(&mut ui, &mut counted, &table);
    table.upsert(batch_at(9_999_998, u64::MAX - 1)).unwrap();
    assert!(counted.read_every >= READ_EVERY);
    std::thread::sleep(counted.read_every);
    let read = counted.read(&table);
    assert!(!counted.show(&mut ui, read));
    assert_eq!(charted(&ui, &counted).len(), READ_ALWAYS + 3);
}

/// A row keyed `id` at `second` since the epoch.
fn batch_at(second: i64, id: u64) -> RecordBatch {
    let at = TimestampSecondArray::from(vec![second]).with_timezone("+00:00");
    let ids = UInt64Array::from(vec![id]);
    RecordBatch::try_from_iter([("at", Arc::new(at) as ArrayRef), ("id", Arc::new(ids) as _)])
        .unwrap()
}

#[test]
fn slow_reads_wait_longer_for_the_next() {
    assert_eq!(pause(Duration::from_millis(1)), READ_EVERY);
    assert_eq!(
        pause(Duration::from_millis(100)),
        Duration::from_millis(400)
    );
}

/// Reads and charts what `counted` counts in `table`.
fn refreshed(ui: &mut Ui, counted: &mut Counted, table: &LiveTable) {
    let read = counted.read(table);
    assert!(!counted.show(ui, read));
}

#[test]
fn rows_without_a_time_are_left_out_and_said_to_be() {
    let (mut ui, mut counted) = opened();
    let at = TimestampSecondArray::from(vec![Some(0), None, Some(1), None]).with_timezone("+00:00");
    let mut table = table_of("at", Arc::new(at));
    counted.count(&mut ui, &mut table);
    assert_eq!(
        counted.said,
        "Rows per 1s of at: waiting for the first rows"
    );
    refreshed(&mut ui, &mut counted, &table);
    assert_eq!(charted(&ui, &counted), [1.0, 1.0]);
    assert_eq!(
        counted.said,
        "Rows per 1s of at; 2 rows have no time and are left out"
    );
    let chart = ui.widget::<CountChart>(counted.chart()).unwrap();
    assert_eq!(
        chart.model().x_extent(),
        Some((0.0, 2.0)),
        "only times across"
    );
}

#[test]
fn counts_wait_while_their_table_starts_over() {
    let (mut ui, mut counted) = opened();
    let mut table = table_at([0, 1]);
    counted.count(&mut ui, &mut table);
    refreshed(&mut ui, &mut counted, &table);
    assert_eq!(charted(&ui, &counted), [1.0, 1.0]);
    counted.wait(&mut ui);
    assert!(
        charted(&ui, &counted).is_empty(),
        "the old counts are not shown"
    );
    assert_eq!(
        counted.said,
        "Rows per 1s of at: waiting for the first rows"
    );
    assert!(
        matches!(counted.read(&table), Read::Same),
        "nothing to read"
    );
}

#[test]
fn a_read_that_works_again_clears_what_a_failed_one_said() {
    let (mut ui, mut counted) = opened();
    let mut table = table_at([0]);
    counted.count(&mut ui, &mut table);
    assert!(!counted.show(&mut ui, Read::New(Err("no".into()))));
    assert_eq!(counted.off.as_deref(), Some("no"));
    assert_eq!(counted.said, "Not counted: no");
    refreshed(&mut ui, &mut counted, &table);
    assert_eq!(counted.off, None);
    assert_eq!(counted.said, "Rows per 1s of at");
}

#[test]
fn a_time_column_named_rows_is_counted_too() {
    let (mut ui, mut counted) = opened_with("rows", Duration::from_secs(1));
    let at = TimestampSecondArray::from(vec![0, 0]).with_timezone("+00:00");
    let mut table = table_of("rows", Arc::new(at));
    counted.count(&mut ui, &mut table);
    assert_eq!(counted.off, None);
    refreshed(&mut ui, &mut counted, &table);
    assert_eq!(charted(&ui, &counted), [2.0]);
}

#[test]
fn spans_that_do_not_fit_the_columns_unit_name_the_fix() {
    let off = |column: ArrayRef, every: Duration| {
        let (mut ui, mut counted) = opened_with("at", every);
        counted.count(&mut ui, &mut table_of("at", column));
        counted.off
    };
    let days: ArrayRef = Arc::new(arrow_array::Date32Array::from(vec![0]));
    assert_eq!(
        off(days.clone(), Duration::from_secs(3_600)).as_deref(),
        Some("per=1h is not a whole number of days, which at counts: write per=1d or a multiple of it")
    );
    assert_eq!(off(days, Duration::from_secs(2 * 86_400)), None);
    let millis: ArrayRef = Arc::new(arrow_array::TimestampMillisecondArray::from(vec![0]));
    let half =
        "per=0.5ms is not a whole number of milliseconds, which at counts: write per=1ms or \
                a multiple of it";
    assert_eq!(
        off(millis.clone(), Duration::from_micros(500)).as_deref(),
        Some(half)
    );
    assert_eq!(off(millis, Duration::from_millis(1)), None);
    let micros: ArrayRef = Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![0]));
    assert_eq!(off(micros, Duration::from_micros(500)), None);
    let nanos: ArrayRef = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![0]));
    assert_eq!(off(nanos, Duration::from_nanos(1)), None);
}
