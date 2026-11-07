//! Pairs: each of the source's rows with every row of the other table holding its value, kept
//! as rows arrive, change and leave on either side.

use super::*;

/// The readings `rows` in a table of their own, paired by `option` (`pairs=`'s value) with
/// `other`.
fn paired_to(
    other: &mut Other,
    option: &str,
    rows: &[(i64, Option<&str>, i64)],
) -> (Joined, LiveTable) {
    let batch = readings(rows);
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    table.upsert(batch).unwrap();
    let mut joined = Joined::new(JoinSpec::pairs(option).unwrap());
    step(&mut joined, &mut table, other);
    (joined, table)
}

/// Each output row as (reading, the other table's `value` it is shown with), sorted.
fn pairs(table: &LiveTable) -> Vec<(i64, Option<String>)> {
    let mut rows = Vec::new();
    for batch in table.snapshot().unwrap() {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let values = as_text(batch.column_by_name("sites_value").unwrap()).unwrap();
        for row in 0..batch.num_rows() {
            let value = values.is_valid(row).then(|| values.value(row).to_owned());
            rows.push((ids.value(row), value));
        }
    }
    rows.sort();
    rows
}

fn some(id: i64, value: &str) -> (i64, Option<String>) {
    (id, Some(value.to_owned()))
}

#[test]
fn each_row_is_shown_with_every_row_holding_its_value_or_once_alone() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "north", "N2"), (2, "south", "S1")]);
    let (mut joined, table) = paired_to(
        &mut other,
        "site:sites",
        &[
            (1, Some("north"), 10),
            (2, Some("south"), 20),
            (3, Some("east"), 30),
        ],
    );
    // The other table's numbers name its rows here (a lookup leaves them out).
    assert_eq!(
        names(&table),
        ["id", "site", "value", "row", "sites_value", "pair"]
    );
    assert_eq!(
        pairs(&table),
        [some(1, "N1"), some(1, "N2"), some(2, "S1"), (3, None)]
    );
    assert_eq!(joined.describe(), "paired with rows of sites on site");
    assert!(joined.join_for(&table).is_some());
}

#[test]
fn rows_arriving_changing_and_leaving_on_either_side_change_the_pairs() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "north", "N2")]);
    let (mut joined, mut table) = paired_to(
        &mut other,
        "site:sites",
        &[(1, Some("north"), 10), (2, Some("south"), 20)],
    );
    // A third north, and N2 moving south: arrived rows are taken in at once.
    other.upsert(&[(2, "north", "N3"), (1, "south", "N2")]);
    assert!(step(&mut joined, &mut table, &mut other));
    assert_eq!(pairs(&table), [some(1, "N1"), some(1, "N3"), some(2, "N2")]);
    // The source's own rows go in through the join.
    let join = joined.join_for(&table).unwrap();
    join.upsert_left(&mut table, readings(&[(3, Some("north"), 30)]))
        .unwrap();
    assert_eq!(pairs(&table).len(), 5);
    // Rows that leave the other table leave the pairs once looked for.
    other.delete(&[0, 1]);
    due(&mut joined);
    assert!(step(&mut joined, &mut table, &mut other));
    assert_eq!(pairs(&table), [some(1, "N3"), (2, None), some(3, "N3")]);
}

#[test]
fn a_numbered_source_s_old_rows_leave_every_pair_they_are_in() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "north", "N2")]);
    let (mut joined, mut table) = paired_to(
        &mut other,
        "site:sites",
        &[(1, Some("north"), 10), (2, Some("north"), 20)],
    );
    assert_eq!(pairs(&table).len(), 4);
    // As `Source::upsert` deletes rows too old to keep: by the source's key.
    let changes = delete_rows(Some(&mut joined), &mut table, &Int64Array::from(vec![1])).unwrap();
    assert_eq!(changes.deleted, 2);
    assert_eq!(pairs(&table), [some(2, "N1"), some(2, "N2")]);
    // A lookup's rows, and a table not joined, are keyed as the source is.
    let (mut looked_up, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let keys = Int64Array::from(vec![1]);
    let changes = delete_rows(Some(&mut looked_up), &mut table, &keys).unwrap();
    assert_eq!((changes.deleted, table.len()), (1, 0));
    let mut plain = LiveTable::new(readings(&[]).schema(), "id").unwrap();
    plain.upsert(readings(&[(1, Some("north"), 10)])).unwrap();
    assert_eq!(delete_rows(None, &mut plain, &keys).unwrap().deleted, 1);
}

#[test]
fn columns_that_cannot_be_paired_turn_the_join_off_and_say_why() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    // Text with the other table's numbers.
    let (joined, table) = paired_to(&mut other, "site:sites.row", &[(1, Some("north"), 10)]);
    let said = joined.describe();
    assert!(said.starts_with("join off: "), "{said}");
    assert!(
        said.contains("both unsigned integers or both text"),
        "{said}"
    );
    assert_eq!(names(&table), ["id", "site", "value"], "its own columns");
    // A column the other table does not have names the option to fix.
    let (joined, _) = paired_to(&mut other, "site:sites.place", &[(1, Some("north"), 10)]);
    let said = joined.describe();
    assert!(said.contains("pairs=COLUMN:sites.OTHER"), "{said}");
}

#[test]
fn a_lookup_keeps_the_last_row_of_a_value_where_pairs_keep_them_all() {
    let rows = [(0, "north", "N1"), (1, "north", "N2")];
    let readings = [(1, Some("north"), 10)];
    let (_, looked_up) = joined_to(&mut Other::new(&rows), &readings);
    assert_eq!(looked_up.len(), 1);
    let (_, paired) = paired_to(&mut Other::new(&rows), "site:sites", &readings);
    assert_eq!(paired.len(), 2);
}

#[test]
fn another_table_of_the_same_name_is_paired_anew_from_the_source_s_own_rows() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "north", "N2")]);
    let (mut joined, mut table) = paired_to(
        &mut other,
        "site:sites",
        &[(1, Some("north"), 10), (2, Some("east"), 20)],
    );
    // The east reading's only row leaves the joined rows (as an app deleting from it would):
    // the reading itself is the join's still, and comes back when paired anew.
    let alone = table.snapshot().unwrap()[0].clone();
    let at = (0..alone.num_rows())
        .find(|&row| alone.column(0).as_primitive::<Int64Type>().value(row) == 2)
        .unwrap();
    let pair = alone
        .column_by_name("pair")
        .unwrap()
        .as_primitive::<arrow_array::types::UInt64Type>();
    table
        .delete(&UInt64Array::from(vec![pair.value(at)]))
        .unwrap();
    assert_eq!(pairs(&table), [some(1, "N1"), some(1, "N2")]);
    // A Flight result read again: a table made anew, three norths and an east.
    let mut again = Other::new(&[(0, "north", "A"), (1, "north", "B"), (2, "east", "C")]);
    assert!(step(&mut joined, &mut table, &mut again));
    assert_eq!(
        pairs(&table),
        [some(1, "A"), some(1, "B"), some(2, "C")],
        "each reading once per pair, the east one too"
    );
    // Given back its own columns, each reading once, when it cannot be paired any more.
    let places = RecordBatch::try_from_iter([
        ("row", Arc::new(UInt64Array::from(vec![0])) as ArrayRef),
        (
            "place",
            Arc::new(StringArray::from(vec!["north"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut unpaired = Other::of(places);
    assert!(step(&mut joined, &mut table, &mut unpaired));
    assert_eq!(names(&table), ["id", "site", "value"]);
    assert_eq!(table.len(), 2);
    assert!(joined.describe().contains("sites has no column site"));
}

#[test]
fn a_window_is_refused_with_pairs_and_says_what_to_do() {
    use crate::flight::FlightSpec;
    use crate::stream::StreamSpec;

    let refused = "window= cannot go with pairs= yet";
    let stream = StreamSpec::parse("ws://h/r#key=k&time=t&window=10m&pairs=a:b").unwrap_err();
    assert!(
        stream.starts_with(refused) && stream.contains("use join="),
        "{stream}"
    );
    let flight = FlightSpec::parse("grpc://h#path=a&key=k&time=t&window=10m&pairs=a:b");
    assert!(flight.unwrap_err().starts_with(refused));
    // A lookup takes a window, and pairs take times without one.
    assert!(StreamSpec::parse("ws://h/r#key=k&time=t&window=10m&join=a:b").is_ok());
    assert!(StreamSpec::parse("ws://h/r#key=k&time=t&pairs=a:b").is_ok());
}
