use std::path::PathBuf;

use arrow_array::{Float64Array, StringArray, UInt64Array};
use densa_data::{Order, ViewSpec};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::*;

/// A result as the SQL panel keeps it: rows numbered in `#`, then the query's columns.
fn result() -> (LiveTable, Schema) {
    let query = Schema::new(vec![
        Field::new("name", arrow_schema::DataType::Utf8, false),
        Field::new("ms", arrow_schema::DataType::Float64, true),
    ]);
    let batch = |numbers: Vec<u64>, names: Vec<&str>, ms: Vec<Option<f64>>| {
        RecordBatch::try_from_iter([
            ("#", Arc::new(UInt64Array::from(numbers)) as ArrayRef),
            ("name", Arc::new(StringArray::from(names)) as ArrayRef),
            ("ms", Arc::new(Float64Array::from(ms)) as ArrayRef),
        ])
        .unwrap()
    };
    let first = batch(vec![0, 1], vec!["a", "b"], vec![Some(2.5), None]);
    let mut table = LiveTable::new(first.schema(), "#").unwrap();
    table.upsert(first).unwrap();
    table
        .upsert(batch(vec![2], vec!["c, \"quoted\""], vec![Some(1.0)]))
        .unwrap();
    (table, query)
}

/// A file in a folder of this test's own.
fn file(test: &str, name: &str) -> PathBuf {
    let folder =
        std::env::temp_dir().join(format!("densa-studio-save-{test}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&folder);
    std::fs::create_dir_all(&folder).unwrap();
    folder.join(name)
}

#[test]
fn a_result_is_saved_as_csv_in_the_grids_order_without_row_numbers() {
    let (mut table, columns) = result();
    let by_ms = table
        .add_view(ViewSpec::new().sort_by("ms", Order::Descending))
        .unwrap();
    let path = file("csv", "result.csv");
    let rows = save(&table, table.view(by_ms).unwrap(), &columns, &path).unwrap();
    assert_eq!(rows, 3);
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text, "name,ms\na,2.5\n\"c, \"\"quoted\"\"\",1.0\nb,\n");
    let leftovers = std::fs::read_dir(path.parent().unwrap()).unwrap().count();
    assert_eq!(leftovers, 1, "no partial file left beside it");
}

#[test]
fn a_result_is_saved_as_parquet_whatever_the_case_of_its_extension() {
    let (mut table, columns) = result();
    let all = table.add_view(ViewSpec::new()).unwrap();
    let path = file("parquet", "Result.PARQUET");
    assert_eq!(
        save(&table, table.view(all).unwrap(), &columns, &path),
        Ok(3)
    );
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(Result::unwrap).collect();
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["name", "ms"]);
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn only_parquet_and_csv_files_are_written() {
    assert_eq!(Format::of(Path::new("a.csv")), Ok(Format::Csv));
    assert_eq!(Format::of(Path::new("a.CsV")), Ok(Format::Csv));
    for name in ["a.json", "a", "csv", "a.parquet.tmp"] {
        assert_eq!(
            Format::of(Path::new(name)),
            Err("name a .parquet or .csv file".to_owned()),
            "{name}"
        );
    }
    let (mut table, columns) = result();
    let all = table.add_view(ViewSpec::new()).unwrap();
    let path = file("json", "result.json");
    assert!(save(&table, table.view(all).unwrap(), &columns, &path).is_err());
    assert!(!path.exists());
}

#[test]
fn a_file_that_cannot_be_written_leaves_nothing_behind() {
    let (mut table, columns) = result();
    let all = table.add_view(ViewSpec::new()).unwrap();
    let folder = file("missing", "x").parent().unwrap().to_owned();
    let path = folder.join("no such folder").join("result.csv");
    assert!(save(&table, table.view(all).unwrap(), &columns, &path).is_err());
    assert_eq!(std::fs::read_dir(&folder).unwrap().count(), 0);

    let other = Schema::new(vec![Field::new(
        "nowhere",
        arrow_schema::DataType::Utf8,
        true,
    )]);
    let path = folder.join("result.csv");
    let error = save(&table, table.view(all).unwrap(), &other, &path).unwrap_err();
    assert_eq!(error, "the result has no column nowhere");
    assert!(!path.exists());
}

#[test]
fn an_empty_result_is_saved_with_its_columns() {
    let (mut table, columns) = result();
    let none = table
        .add_view(ViewSpec::new().filter(densa_data::Filter::compare(
            "ms",
            densa_data::CompareOp::Gt,
            densa_data::Scalar::Float(100.0),
        )))
        .unwrap();
    let path = file("empty", "empty.csv");
    assert_eq!(
        save(&table, table.view(none).unwrap(), &columns, &path),
        Ok(0)
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "name,ms\n");
    let mut fresh = LiveTable::new(table.schema().clone(), "#").unwrap();
    let all = fresh.add_view(ViewSpec::new()).unwrap();
    let path = file("fresh", "fresh.parquet");
    assert_eq!(
        save(&fresh, fresh.view(all).unwrap(), &columns, &path),
        Ok(0)
    );
    assert!(path.exists());
}

#[test]
fn a_long_result_is_written_in_parts_every_row_once() {
    let count = ROWS_PER_BATCH + 4_464;
    let numbers: Vec<u64> = (0..count as u64).collect();
    let names: Vec<String> = numbers.iter().map(|n| format!("n{n}")).collect();
    let batch = RecordBatch::try_from_iter([
        ("#", Arc::new(UInt64Array::from(numbers)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
    ])
    .unwrap();
    let mut table = LiveTable::new(batch.schema(), "#").unwrap();
    table.upsert(batch).unwrap();
    let all = table.add_view(ViewSpec::new()).unwrap();
    let columns = Schema::new(vec![Field::new("name", arrow_schema::DataType::Utf8, true)]);
    let path = file("long", "long.csv");
    assert_eq!(
        save(&table, table.view(all).unwrap(), &columns, &path),
        Ok(count)
    );
    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), count + 1, "one header");
    assert_eq!(lines[ROWS_PER_BATCH + 1], format!("n{ROWS_PER_BATCH}"));
    assert_eq!(
        lines.last().copied(),
        Some(format!("n{}", count - 1).as_str())
    );
}

#[test]
fn a_write_that_fails_midway_leaves_nothing_behind() {
    use arrow_array::types::Int32Type;
    use arrow_array::ListArray;
    let lists = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1)])]);
    let batch = RecordBatch::try_from_iter([
        ("#", Arc::new(UInt64Array::from(vec![0_u64])) as ArrayRef),
        ("list", Arc::new(lists) as ArrayRef),
    ])
    .unwrap();
    let mut table = LiveTable::new(batch.schema(), "#").unwrap();
    table.upsert(batch.clone()).unwrap();
    let all = table.add_view(ViewSpec::new()).unwrap();
    let columns = Schema::new(vec![batch.schema().field(1).clone()]);
    let path = file("midway", "lists.csv");
    let error = save(&table, table.view(all).unwrap(), &columns, &path);
    assert!(error.is_err(), "CSV cannot hold lists");
    let left = std::fs::read_dir(path.parent().unwrap()).unwrap().count();
    assert_eq!(left, 0, "the partial file went");
}
