//! A Flight SQL service's list of tables, made readable: the schema it sends with each table
//! (Arrow IPC bytes) becomes a `columns` column naming each column and its type.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_ipc::convert::try_schema_from_ipc_buffer;
use arrow_schema::{DataType, Field, Schema};

/// The column Flight SQL sends each table's schema in.
const SCHEMA_COLUMN: &str = "table_schema";
/// The column the schemas are written in instead.
pub(crate) const COLUMNS: &str = "columns";
/// Characters of a table's columns written, about: no more could be read in a cell.
const MOST_CHARS: usize = 1_000;
/// Characters of why a schema could not be read, at most.
const REASON_CHARS: usize = 80;

/// `batch` of a list of tables with its `table_schema` column (if it has one) written out as
/// a `columns` column of text, in its place: `id int64, name text, at timestamp`. A schema
/// that cannot be read says so; a missing one is null.
pub(crate) fn readable(batch: RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let Some((at, _)) = schema.column_with_name(SCHEMA_COLUMN) else {
        return batch;
    };
    let encoded = batch.column(at);
    let columns: StringArray = match encoded.data_type() {
        DataType::Binary => described(encoded.as_binary::<i32>().iter()),
        DataType::LargeBinary => described(encoded.as_binary::<i64>().iter()),
        DataType::BinaryView => described(encoded.as_binary_view().iter()),
        _ => return batch,
    };
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[at] = Field::new(COLUMNS, DataType::Utf8, true);
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();
    arrays[at] = Arc::new(columns);
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    // Same rows, one column swapped for another as long: cannot fail.
    let readable = RecordBatch::try_new(schema, arrays);
    debug_assert!(readable.is_ok(), "{readable:?}");
    readable.unwrap_or(batch)
}

/// The schemas `encoded` (IPC bytes, or none) as text.
fn described<'a>(encoded: impl Iterator<Item = Option<&'a [u8]>>) -> StringArray {
    encoded
        .map(|bytes| {
            bytes.map(|bytes| match try_schema_from_ipc_buffer(bytes) {
                Ok(schema) => columns(&schema),
                Err(error) => {
                    let reason: String = error.to_string().chars().take(REASON_CHARS).collect();
                    format!("? (unreadable schema: {reason})")
                }
            })
        })
        .collect()
}

/// `schema`'s columns as text: each one's name and type, in order, as many as fit in about
/// [`MOST_CHARS`] (then how many more there are). Control characters in names are spaces.
fn columns(schema: &Schema) -> String {
    let fields = schema.fields();
    if fields.is_empty() {
        return "(no columns)".to_owned();
    }
    let mut text = String::new();
    for (shown, field) in fields.iter().enumerate() {
        if text.len() >= MOST_CHARS {
            text.push_str(&format!(", and {} more", fields.len() - shown));
            break;
        }
        if shown > 0 {
            text.push_str(", ");
        }
        let name = field.name().chars().take(MOST_CHARS);
        text.extend(name.map(|ch| if ch.is_control() { ' ' } else { ch }));
        text.push(' ');
        text.push_str(&type_name(field.data_type()));
    }
    text
}

/// What a column of `data_type` holds, in a word or two.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "text".to_owned(),
        DataType::Boolean => "bool".to_owned(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "bytes".to_owned(),
        DataType::Date32 | DataType::Date64 => "date".to_owned(),
        DataType::Time32(_) | DataType::Time64(_) => "time".to_owned(),
        DataType::Timestamp(..) => "timestamp".to_owned(),
        DataType::Duration(_) => "duration".to_owned(),
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => format!("decimal({p},{s})"),
        DataType::Dictionary(_, values) => type_name(values),
        DataType::RunEndEncoded(_, values) => type_name(values.data_type()),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _) => format!("list of {}", type_name(item.data_type())),
        DataType::Struct(_) => "struct".to_owned(),
        // The rest (numbers, intervals, maps, unions, null) by their name alone: `int64`,
        // `map`, not the fields inside.
        other => {
            let named = other.to_string();
            named.split('(').next().unwrap_or_default().to_lowercase()
        }
    }
}

/// The catalog, schema and name of the table in `row` of a list of tables (as Flight SQL
/// lists them); `None` without a name. Values of any type are read as text.
pub(crate) fn listed(
    batch: &RecordBatch,
    row: usize,
) -> Option<(Option<String>, Option<String>, String)> {
    let text = |name: &str| -> Option<String> {
        let column = batch.column_by_name(name)?.slice(row, 1);
        let column = arrow_cast::cast(&column, &DataType::Utf8).ok()?;
        let column = column.as_any().downcast_ref::<arrow_array::StringArray>()?;
        column.is_valid(0).then(|| column.value(0).to_owned())
    };
    let table = text("table_name")?;
    Some((text("catalog_name"), text("db_schema_name"), table))
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, BinaryViewArray, LargeBinaryArray, StringArray};
    use arrow_ipc::writer::{IpcDataGenerator, IpcWriteOptions};
    use arrow_schema::TimeUnit;

    use super::*;

    /// `schema` as Flight SQL sends it: an IPC schema message.
    fn encoded(schema: &Schema) -> Vec<u8> {
        let options = IpcWriteOptions::default();
        let data = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut arrow_ipc::writer::DictionaryTracker::new(false),
            &options,
        );
        let mut bytes = Vec::new();
        arrow_ipc::writer::write_message(&mut bytes, data, &options).unwrap();
        bytes
    }

    fn listing(schemas: Vec<Option<Vec<u8>>>) -> RecordBatch {
        let names: Vec<String> = (0..schemas.len()).map(|i| format!("t{i}")).collect();
        let schemas: BinaryArray = schemas.iter().map(|s| s.as_deref()).collect();
        RecordBatch::try_from_iter([
            ("table_name", Arc::new(StringArray::from(names)) as ArrayRef),
            (
                "table_type",
                Arc::new(StringArray::from(vec!["TABLE"; schemas.len()])),
            ),
            ("table_schema", Arc::new(schemas)),
        ])
        .unwrap()
    }

    fn texts(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        let column = batch.column_by_name(column).unwrap().as_string::<i32>();
        column.iter().map(|v| v.map(str::to_owned)).collect()
    }

    #[test]
    fn each_tables_schema_is_written_out_in_its_place() {
        let trips = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("city", DataType::Utf8View, true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("fare", DataType::Decimal128(10, 2), true),
            Field::new("paid", DataType::Boolean, true),
        ]);
        let empty = Schema::empty();
        let batch = readable(listing(vec![
            Some(encoded(&trips)),
            Some(encoded(&empty)),
            Some(b"not a schema".to_vec()),
            None,
        ]));
        let names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            ["table_name", "table_type", "columns"],
            "in its place"
        );
        let columns = texts(&batch, COLUMNS);
        assert_eq!(
            columns[..2],
            [
                Some("id int64, city text, at timestamp, fare decimal(10,2), paid bool".to_owned()),
                Some("(no columns)".to_owned()),
            ]
        );
        let unreadable = columns[2].as_deref().unwrap();
        assert!(
            unreadable.starts_with("? (unreadable schema: "),
            "{unreadable}"
        );
        assert!(
            unreadable.chars().count() < 30 + REASON_CHARS,
            "{unreadable}"
        );
        assert_eq!(columns[3], None);
        assert_eq!(texts(&batch, "table_name")[3].as_deref(), Some("t3"));
    }

    #[test]
    fn large_and_viewed_bytes_and_a_list_with_no_rows_are_read_too() {
        let bytes = encoded(&Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        let large: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(bytes.as_slice())]));
        let viewed: ArrayRef = Arc::new(BinaryViewArray::from(vec![Some(bytes.as_slice())]));
        for schemas in [large, viewed] {
            let batch = RecordBatch::try_from_iter([("table_schema", schemas)]).unwrap();
            assert_eq!(
                texts(&readable(batch), COLUMNS),
                [Some("n int32".to_owned())]
            );
        }
        // The empty batch a list's columns arrive in turns the same way as its rows.
        let empty = readable(listing(Vec::new()));
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.schema(), readable(listing(vec![None])).schema());
    }

    #[test]
    fn a_huge_schema_is_cut_short_and_odd_names_are_cleaned() {
        let wide: Vec<Field> = (0..100_000)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, true))
            .collect();
        let text = columns(&Schema::new(wide));
        assert!(text.len() < MOST_CHARS + 100, "{}", text.len());
        assert!(text.starts_with("c0 int64, c1 int64"));
        assert!(text.ends_with(" more"), "{text}");
        let left: usize = text
            .rsplit(", and ")
            .next()
            .unwrap()
            .trim_end_matches(" more")
            .parse()
            .unwrap();
        let shown = text.matches(" int64").count();
        assert_eq!(shown + left, 100_000);

        let odd = Schema::new(vec![
            Field::new("a\nb\tc", DataType::Utf8, true),
            Field::new("x".repeat(5_000), DataType::Utf8, true),
        ]);
        let text = columns(&odd);
        assert!(text.starts_with("a b c text, xxx"), "{text}");
        assert!(text.len() < MOST_CHARS + 100);
    }

    #[test]
    fn a_list_without_schemas_is_left_as_it_is() {
        let names = Arc::new(StringArray::from(vec!["t"])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("table_name", names)]).unwrap();
        assert_eq!(readable(batch.clone()), batch);
        let odd = Arc::new(StringArray::from(vec!["x"])) as ArrayRef;
        let text_schema = RecordBatch::try_from_iter([("table_schema", odd)]).unwrap();
        assert_eq!(
            readable(text_schema.clone()),
            text_schema,
            "not bytes: kept"
        );
    }

    #[test]
    fn types_are_named_in_a_word_or_two() {
        let cases = [
            (DataType::UInt8, "uint8"),
            (DataType::Float64, "float64"),
            (DataType::LargeUtf8, "text"),
            (DataType::Date32, "date"),
            (DataType::Time64(TimeUnit::Nanosecond), "time"),
            (DataType::Duration(TimeUnit::Second), "duration"),
            (DataType::FixedSizeBinary(16), "bytes"),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                "text",
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                "list of float32",
            ),
            (DataType::Struct(Vec::<Field>::new().into()), "struct"),
            (DataType::Null, "null"),
            (DataType::Int64, "int64"),
            (
                DataType::Interval(arrow_schema::IntervalUnit::DayTime),
                "interval",
            ),
            (
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int8, true)), 3),
                "list of int8",
            ),
            (
                DataType::LargeListView(Arc::new(Field::new("item", DataType::Utf8, true))),
                "list of text",
            ),
            (
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("k", DataType::Utf8, false),
                                Field::new("v", DataType::Int8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                "map",
            ),
            (
                DataType::Union(
                    arrow_schema::UnionFields::empty(),
                    arrow_schema::UnionMode::Dense,
                ),
                "union",
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Utf8, true)),
                ),
                "text",
            ),
        ];
        for (data_type, name) in cases {
            assert_eq!(type_name(&data_type), name, "{data_type}");
        }
    }
}
