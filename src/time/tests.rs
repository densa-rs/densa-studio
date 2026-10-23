use arrow_array::types::TimestampMillisecondType;
use arrow_array::{
    BooleanArray, Float64Array, Int64Array, StringArray, StringViewArray, TimestampSecondArray,
    UInt32Array,
};

use super::*;

fn time(options: &[(&str, &str)]) -> Result<Option<TimeSpec>, String> {
    let mut read = TimeOptions::default();
    for (name, value) in options {
        assert!(read.take(name, value)?, "{name} is a time option");
    }
    read.finish(None)
}

fn spec(options: &[(&str, &str)]) -> TimeSpec {
    time(options).unwrap().unwrap()
}

fn batch(time: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..time.len() as i64)) as ArrayRef,
        ),
        ("time", time),
    ])
    .unwrap()
}

/// The time column of `batch`, as milliseconds since the epoch.
fn millis(batch: &RecordBatch) -> Vec<Option<i64>> {
    use arrow_array::cast::AsArray;
    let column = batch.column_by_name("time").unwrap();
    let ms = cast(column, &timestamp(TimeUnit::Millisecond)).unwrap();
    ms.as_primitive::<TimestampMillisecondType>()
        .iter()
        .collect()
}

fn converted(spec: &TimeSpec, time: ArrayRef) -> (RecordBatch, Option<String>) {
    spec.convert(batch(time))
}

/// A table of `batch` (made timestamps by `spec`), kept to its window.
fn windowed(spec: &TimeSpec, time: ArrayRef) -> LiveTable {
    let (batch, problem) = converted(spec, time);
    assert_eq!(problem, None);
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    spec.apply(&mut table).unwrap();
    table.upsert(batch).unwrap();
    table
}

#[test]
fn spans_are_a_number_and_a_unit() {
    let secs = |text| span(text).map(|d| d.as_secs_f64());
    assert_eq!(secs("500ms"), Ok(0.5));
    assert_eq!(secs("30s"), Ok(30.0));
    assert_eq!(secs("10m"), Ok(600.0));
    assert_eq!(secs("10min"), Ok(600.0));
    assert_eq!(secs("1.5h"), Ok(5_400.0));
    assert_eq!(secs("2d"), Ok(172_800.0));
    assert_eq!(secs("366d"), Ok(MOST.as_secs_f64()));
    let huge = format!("{}d", "9".repeat(400));
    assert!(span(&huge).unwrap_err().contains("longer than a year"));
    for wrong in ["10", "m", "10x", "1.2.3s", "-5m", "", "10 m"] {
        assert!(span(wrong).unwrap_err().contains("such as"), "{wrong}");
    }
    assert!(span("0s").unwrap_err().contains("keeps nothing"));
    assert!(span("367d").unwrap_err().contains("longer than a year"));
}

#[test]
fn the_options_go_together() {
    assert_eq!(time(&[]), Ok(None));
    let given = spec(&[("time", "t"), ("window", "10m"), ("unit", "s")]);
    assert_eq!(given.window, Some(Duration::from_secs(600)));
    assert_eq!(given.options(), ["time=t", "window=10m", "unit=s"]);
    assert_eq!(given.describe().as_deref(), Some("last 10m by t"));
    let alone = spec(&[("time", "t")]);
    assert_eq!(alone.options(), ["time=t"]);
    assert_eq!(alone.describe(), None, "no window to tell of");
    let same = spec(&[("time", "t"), ("window", "600s")]);
    assert_eq!(
        same.options(),
        ["time=t", "window=10m"],
        "one span, one way"
    );
    let spaced = spec(&[("time", "a b"), ("unit", "us")]);
    assert_eq!(spaced.options(), ["time=a%20b", "unit=us"]);

    let error = time(&[("window", "1m")]).unwrap_err();
    assert!(error.contains("time=COLUMN"), "{error}");
    let error = time(&[("unit", "s")]).unwrap_err();
    assert_eq!(error, "unit= goes with the column of times: time=COLUMN");
    assert!(time(&[("unit", "h")])
        .unwrap_err()
        .contains("s, ms, us or ns"));
    assert_eq!(time(&[("time", "")]), Err("time needs a value".into()));
    for (twice, value) in [("time", "t"), ("unit", "s"), ("window", "1m")] {
        let error = time(&[(twice, value), (twice, value)]).unwrap_err();
        assert_eq!(error, format!("{twice}= is given twice"));
    }
    assert_eq!(TimeOptions::default().take("key", "id"), Ok(false));
}

#[test]
fn spans_are_written_in_their_largest_whole_unit() {
    let text = |s| written(span(s).unwrap());
    assert_eq!(text("1.5h"), "90m");
    assert_eq!(text("48h"), "2d");
    assert_eq!(text("0.25s"), "250ms");
    assert_eq!(text("1001ms"), "1001ms");
    assert_eq!(text("0.5ms"), "0.5ms");
    assert_eq!(text("90s"), "90s");
    for given in ["1.5h", "0.5ms", "1d", "1001ms"] {
        assert_eq!(span(&text(given)), span(given), "{given} reads back");
    }
}

#[test]
fn numbers_become_utc_timestamps_in_their_unit() {
    let ms = spec(&[("time", "time")]);
    let (batch, problem) = converted(&ms, Arc::new(Float64Array::from(vec![Some(1_500.0), None])));
    assert_eq!(problem, None);
    let field = batch.schema().field(1).clone();
    assert_eq!(field.data_type(), &timestamp(TimeUnit::Millisecond));
    assert_eq!(millis(&batch), [Some(1_500), None], "nulls stay null");

    let seconds = spec(&[("time", "time"), ("unit", "s")]);
    let (batch, _) = converted(&seconds, Arc::new(UInt32Array::from(vec![2_u32])));
    assert_eq!(
        batch.schema().field(1).data_type(),
        &timestamp(TimeUnit::Second)
    );
    assert_eq!(millis(&batch), [Some(2_000)]);
    for (unit, ms) in [("us", 1), ("ns", 0)] {
        let given = spec(&[("time", "time"), ("unit", unit)]);
        let (batch, _) = converted(&given, Arc::new(Int64Array::from(vec![1_000])));
        assert_eq!(millis(&batch), [Some(ms)], "{unit}");
    }
    assert_eq!(
        batch_ids(&converted(&ms, Arc::new(Int64Array::from(vec![7]))).0),
        [0]
    );
}

fn batch_ids(batch: &RecordBatch) -> Vec<i64> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    ids.values().to_vec()
}

#[test]
fn iso_text_becomes_timestamps_and_other_text_is_counted() {
    let iso = spec(&[("time", "time")]);
    let texts = StringArray::from(vec![
        Some("1970-01-01T00:00:01.250Z"),
        Some("1970-01-01T00:00:02+01:00"),
        Some("yesterday"),
        None,
    ]);
    let (batch, problem) = converted(&iso, Arc::new(texts));
    assert_eq!(millis(&batch), [Some(1_250), Some(-3_598_000), None, None]);
    assert_eq!(
        problem.as_deref(),
        Some("1 times in time neither ISO 8601 nor numbers")
    );
}

#[test]
fn text_of_numbers_counts_the_unit_as_numbers_do() {
    // A JSON feed whose first time was null makes the column text, then sends numbers.
    let ms = spec(&[("time", "time")]);
    let texts = StringArray::from(vec!["1500", "2.0", "1970-01-01T00:00:03Z"]);
    let (batch, problem) = converted(&ms, Arc::new(texts));
    assert_eq!(problem, None);
    assert_eq!(millis(&batch), [Some(1_500), Some(2), Some(3_000)]);
    let seconds = spec(&[("time", "time"), ("unit", "s")]);
    let (batch, problem) = converted(&seconds, Arc::new(StringViewArray::from(vec!["7"])));
    assert_eq!(problem, None);
    assert_eq!(
        batch.schema().field(1).data_type(),
        &timestamp(TimeUnit::Millisecond)
    );
    assert_eq!(millis(&batch), [Some(7_000)]);
}

#[test]
fn a_key_cannot_be_the_time_column() {
    let mut read = TimeOptions::default();
    read.take("time", "id").unwrap();
    let error = read.finish(Some("id")).unwrap_err();
    assert!(error.contains("a key cannot be a time"), "{error}");
    let mut read = TimeOptions::default();
    read.take("time", "at").unwrap();
    assert!(read.finish(Some("id")).unwrap().is_some());
    assert_eq!(TimeOptions::default().finish(Some("id")), Ok(None));
}

#[test]
fn numbers_out_of_range_are_counted() {
    let iso = spec(&[("time", "time"), ("unit", "s")]);
    let huge = Float64Array::from(vec![1.0, 1e300, f64::NAN]);
    let (batch, problem) = converted(&iso, Arc::new(huge));
    assert_eq!(millis(&batch), [Some(1_000), None, None]);
    assert_eq!(problem.as_deref(), Some("2 times in time out of range"));
}

#[test]
fn timestamps_stay_as_they_are_and_a_unit_for_them_is_refused() {
    let seconds = Arc::new(TimestampSecondArray::from(vec![5]));
    let (batch, problem) = converted(&spec(&[("time", "time")]), seconds.clone());
    assert_eq!(problem, None);
    assert_eq!(
        batch.column(1).data_type(),
        seconds.data_type(),
        "untouched"
    );
    let (batch, problem) = converted(&spec(&[("time", "time"), ("unit", "ms")]), seconds);
    let problem = problem.unwrap();
    assert!(
        problem.contains("unit= is for a column of numbers"),
        "{problem}"
    );
    assert_eq!(millis(&batch), [Some(5_000)], "the rows come all the same");
}

#[test]
fn a_column_that_cannot_hold_times_or_is_not_there_is_named() {
    let flags = Arc::new(BooleanArray::from(vec![true]));
    let (batch, problem) = converted(&spec(&[("time", "time")]), flags);
    assert_eq!(problem.as_deref(), Some("time holds Boolean, not times"));
    assert_eq!(batch.num_rows(), 1, "the rows come all the same");
    let elsewhere = spec(&[("time", "when")]);
    let (_, problem) = converted(&elsewhere, Arc::new(Int64Array::from(vec![1])));
    assert_eq!(
        problem.as_deref(),
        Some("the time column when is not in the rows")
    );
}

#[test]
fn a_window_keeps_the_newest_times() {
    let minute = spec(&[("time", "time"), ("window", "1m")]);
    let table = windowed(&minute, Arc::new(Float64Array::from(vec![0.0, 90.0])));
    assert_eq!(table.len(), 2, "90 ms apart");
    let table = windowed(&minute, Arc::new(Float64Array::from(vec![0.0, 90_000.0])));
    assert_eq!(table.len(), 1, "90,000 ms apart: the first goes");
    let seconds = spec(&[("time", "time"), ("window", "1m"), ("unit", "s")]);
    let table = windowed(&seconds, Arc::new(Float64Array::from(vec![0.0, 90.0])));
    assert_eq!(table.len(), 1, "90 s apart: the first goes");
    let exact = spec(&[("time", "time"), ("window", "1.001s")]);
    let table = windowed(&exact, Arc::new(Int64Array::from(vec![0, 1_001])));
    assert_eq!(table.len(), 2, "1,001 ms old: exactly the span");
    let no_window = spec(&[("time", "time")]);
    let table = windowed(&no_window, Arc::new(Int64Array::from(vec![0, 1_000_000])));
    assert_eq!(table.len(), 2);
    assert!(table.window().is_none());
}

#[test]
fn a_window_over_what_is_not_times_says_why() {
    let minute = spec(&[("time", "time"), ("window", "1m")]);
    let flags = batch(Arc::new(BooleanArray::from(vec![true])));
    let mut table = LiveTable::new(flags.schema(), "id").unwrap();
    let error = minute.apply(&mut table).unwrap_err();
    assert_eq!(error, "a window needs times; time holds Boolean");
    let elsewhere = spec(&[("time", "when"), ("window", "1m")]);
    let error = elsewhere.apply(&mut table).unwrap_err();
    assert_eq!(error, "the window's column when is not in the rows");
    assert!(table.window().is_none());
}
