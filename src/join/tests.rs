use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, UInt64Array};

use super::*;

#[test]
fn a_join_names_a_column_and_the_table_it_looks_up() {
    let spec = JoinSpec::parse("site:sites").unwrap();
    assert_eq!(
        spec,
        JoinSpec {
            column: "site".into(),
            table: "sites".into(),
            key: "site".into(),
        },
        "the table's column of the same name"
    );
    let spec = JoinSpec::parse("place:sites.site").unwrap();
    assert_eq!((spec.column.as_str(), spec.key.as_str()), ("place", "site"));
    assert_eq!(spec.option(), "join=place:sites.site");
    let spaced = JoinSpec::parse("a b:t").unwrap();
    assert_eq!(spaced.option(), "join=a%20b:t.a%20b");
    for bad in ["site", ":sites", "site:", "site:sites.", "site:.site"] {
        let error = JoinSpec::parse(bad).unwrap_err();
        assert!(error.contains("join=COLUMN:TABLE"), "{bad}: {error}");
    }
}

#[test]
fn remote_sources_take_one_join_and_are_known_by_it() {
    use crate::flight::FlightSpec;
    use crate::stream::StreamSpec;

    let stream = StreamSpec::parse("ws://h/r#key=sensor&join=site:sites").unwrap();
    assert_eq!(stream.join, Some(JoinSpec::parse("site:sites").unwrap()));
    assert_eq!(stream.origin(), "ws://h/r#key=sensor&join=site:sites.site");
    let twice = StreamSpec::parse("ws://h/r#join=a:t&join=b:t").unwrap_err();
    assert!(twice.contains("twice"), "{twice}");
    let empty = StreamSpec::parse("ws://h/r#join").unwrap_err();
    assert!(empty.contains("needs a value"), "{empty}");
    let unknown = StreamSpec::parse("ws://h/r#jion=a:t").unwrap_err();
    assert!(unknown.contains("join=COLUMN:TABLE"), "{unknown}");

    let flight = FlightSpec::parse("grpc://h#path=a&join=site:sites.id").unwrap();
    assert_eq!(flight.join, Some(JoinSpec::parse("site:sites.id").unwrap()));
    assert!(
        flight.origin().ends_with("&join=site:sites.id"),
        "{}",
        flight.origin()
    );
    let twice = FlightSpec::parse("grpc://h#path=a&join=a:t&join=b:t").unwrap_err();
    assert!(twice.contains("twice"), "{twice}");
    let unknown = FlightSpec::parse("grpc://h#path=a&jion=a:t").unwrap_err();
    assert!(unknown.contains("join=COLUMN:TABLE"), "{unknown}");
}

/// Readings keyed by `id`, each naming a site.
fn readings(rows: &[(i64, Option<&str>, i64)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "site",
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// Sites as a file reads them: numbered in `row`, with a `value` of their own.
fn sites(rows: &[(u64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "row",
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "site",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

/// The table of the other side, as a source offers it.
struct Other {
    table: LiveTable,
    arrived: Vec<RecordBatch>,
    deletions: u64,
}

impl Other {
    fn new(rows: &[(u64, &str, &str)]) -> Self {
        Self::of(sites(rows))
    }

    /// A table of `batch`'s columns, keyed by `row`, holding it.
    fn of(batch: RecordBatch) -> Self {
        let mut table = LiveTable::new(batch.schema(), "row").unwrap();
        table.upsert(batch).unwrap();
        Self {
            table,
            arrived: Vec::new(),
            deletions: 0,
        }
    }

    fn upsert(&mut self, rows: &[(u64, &str, &str)]) {
        self.upsert_batch(sites(rows));
    }

    fn upsert_batch(&mut self, batch: RecordBatch) {
        self.table.upsert(batch.clone()).unwrap();
        self.arrived.push(batch);
    }

    fn delete(&mut self, rows: &[u64]) {
        self.table
            .delete(&UInt64Array::from(rows.to_vec()))
            .unwrap();
        self.deletions += 1;
    }

    fn offer(&mut self) -> Offer {
        Offer {
            instance: Some(self.table.instance()),
            schema: self.table.schema().clone(),
            arrived: std::mem::take(&mut self.arrived),
            deletions: self.deletions,
            updates: 0,
            key: "row".into(),
            numbered: true,
            joined: false,
            paged: false,
        }
    }

    fn all(&self) -> Vec<RecordBatch> {
        self.table.snapshot().unwrap()
    }
}

/// What a frame does with `offer` (and `all` the other table's rows) for the source's
/// `table`, as `Source::link` does it, the table replaced when the step says so. Whether
/// the table changed.
fn go(
    joined: &mut Joined,
    table: &mut LiveTable,
    offer: Option<&Offer>,
    all: Option<Vec<RecordBatch>>,
) -> bool {
    let Some(ready) = joined.ready(offer) else {
        return false;
    };
    if joined.is_idle(ready.0, ready.1, all.is_some()) {
        return false;
    }
    match joined.step(table, "id", ready, all) {
        Stepped::Nothing => false,
        Stepped::Changed => true,
        Stepped::Replaced(new) => {
            *table = *new;
            true
        }
    }
}

/// One frame of `joined` over `table`: all the other table's rows only when asked for.
fn step(joined: &mut Joined, table: &mut LiveTable, other: &mut Other) -> bool {
    let offer = other.offer();
    let all = joined.wants_all(&offer).then(|| other.all());
    go(joined, table, Some(&offer), all)
}

type Shown = (i64, Vec<Option<String>>);

fn names(table: &LiveTable) -> Vec<String> {
    let schema = table.schema();
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// `table`'s rows by id, every other column as text.
fn shown(table: &LiveTable) -> Vec<Shown> {
    let mut rows = Vec::new();
    for batch in table.snapshot().unwrap() {
        let ids = batch.column(0).as_primitive::<Int64Type>();
        let text: Vec<StringArray> = (1..batch.num_columns())
            .map(|c| as_text(batch.column(c)).unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let values = text
                .iter()
                .map(|column| column.is_valid(row).then(|| column.value(row).to_owned()))
                .collect();
            rows.push((ids.value(row), values));
        }
    }
    rows.sort();
    rows
}

fn text(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_owned)).collect()
}

/// The readings `rows` in a table of their own, joined to `other`.
fn joined_to(other: &mut Other, rows: &[(i64, Option<&str>, i64)]) -> (Joined, LiveTable) {
    let batch = readings(rows);
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    table.upsert(batch).unwrap();
    let mut joined = Joined::new(JoinSpec::parse("site:sites").unwrap());
    assert!(step(&mut joined, &mut table, other), "joined at once");
    assert!(joined.join_for(&table).is_some());
    (joined, table)
}

#[test]
fn a_table_read_a_page_at_a_time_is_not_joined_and_the_join_says_why() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let mut offer = other.offer();
    offer.instance = None;
    offer.paged = true;
    let mut joined = Joined::new(JoinSpec::parse("site:sites").unwrap());
    assert!(joined.ready(Some(&offer)).is_none());
    assert_eq!(
        joined.describe(),
        "join off: sites is read a page at a time (too large to load), and Studio joins only \
         tables it holds: join them in SQL instead"
    );
    // Said once: a frame after does not make it anew.
    assert!(joined.ready(Some(&offer)).is_none());
    assert!(joined.describe().ends_with("join them in SQL instead"));
}

#[test]
fn rows_show_the_columns_of_the_row_they_name_named_apart_from_their_own() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "south", "S1")]);
    let (joined, table) = joined_to(
        &mut other,
        &[(1, Some("north"), 10), (2, Some("east"), 20), (3, None, 30)],
    );
    assert_eq!(
        names(&table),
        ["id", "site", "value", "sites_value"],
        "the numbers Studio gave the file's rows are left out; its value is named apart"
    );
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("north"), Some("10"), Some("N1")])),
            (2, text(&[Some("east"), Some("20"), None])),
            (3, text(&[None, Some("30"), None])),
        ]
    );
    assert_eq!(joined.describe(), "joined with sites on site");
}

#[test]
fn rows_arriving_on_either_side_are_joined_as_they_come() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let join = joined.join_for(&table).unwrap();
    join.upsert_left(&mut table, readings(&[(2, Some("east"), 20)]))
        .unwrap();

    other.upsert(&[(1, "east", "E1"), (0, "north", "N2")]);
    assert!(step(&mut joined, &mut table, &mut other), "rows came");
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("north"), Some("10"), Some("N2")])),
            (2, text(&[Some("east"), Some("20"), Some("E1")])),
        ]
    );
    assert!(!step(&mut joined, &mut table, &mut other), "nothing came");
    let offer = other.offer();
    let instance = offer.instance.unwrap();
    assert!(joined.is_idle(&offer, instance, false));
    assert!(!joined.is_idle(&offer, instance, true), "rows to look over");
}

/// Lets `joined` look for rows that left at its next step.
fn due(joined: &mut Joined) {
    if let State::On(linked) = &mut joined.state {
        linked.pruned_at = Instant::now() - linked.prune_every;
    }
}

#[test]
fn rows_that_leave_the_other_table_leave_the_joined_rows_once_looked_for() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "south", "S1"), (2, "east", "E1")]);
    let (mut joined, mut table) = joined_to(
        &mut other,
        &[
            (1, Some("north"), 10),
            (2, Some("south"), 20),
            (3, Some("east"), 30),
        ],
    );
    other.delete(&[1]);
    // Another row names east: it stays.
    other.upsert(&[(3, "east", "E2")]);
    other.delete(&[2]);
    let before = [
        (1, text(&[Some("north"), Some("10"), Some("N1")])),
        (2, text(&[Some("south"), Some("20"), Some("S1")])),
        (3, text(&[Some("east"), Some("30"), Some("E2")])),
    ];
    step(&mut joined, &mut table, &mut other);
    assert_eq!(shown(&table), before, "not looked for yet: once a second");
    due(&mut joined);
    assert!(step(&mut joined, &mut table, &mut other));
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("north"), Some("10"), Some("N1")])),
            (2, text(&[Some("south"), Some("20"), None])),
            (3, text(&[Some("east"), Some("30"), Some("E2")])),
        ]
    );
    // Looked for: not again until rows leave again.
    due(&mut joined);
    assert!(!joined.wants_all(&other.offer()));
}

#[test]
fn a_row_replaced_by_one_naming_another_key_is_let_go_when_keyed_apart() {
    // The other table is keyed by `row`, not by the column looked up: a replaced row may
    // name another site, leaving its old one.
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    other.upsert(&[(0, "south", "S1")]);
    step(&mut joined, &mut table, &mut other);
    let mut offer = other.offer();
    offer.updates = 1;
    due(&mut joined);
    assert!(joined.wants_all(&offer));
    go(&mut joined, &mut table, Some(&offer), Some(other.all()));
    assert_eq!(
        shown(&table),
        [(1, text(&[Some("north"), Some("10"), None]))]
    );

    // Keyed by the column looked up, a replaced row keeps its key: nothing to look for.
    offer.key = "site".into();
    offer.updates = 2;
    due(&mut joined);
    assert!(!joined.wants_all(&offer));
}

#[test]
fn another_table_of_the_same_name_takes_the_old_ones_place() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "south", "S1")]);
    let (mut joined, mut table) = joined_to(
        &mut other,
        &[(1, Some("north"), 10), (2, Some("south"), 20)],
    );
    // A Flight result read again: a table made anew, with south alone.
    let mut again = Other::new(&[(0, "south", "S2")]);
    assert!(joined.wants_all(&again.offer()));
    assert!(step(&mut joined, &mut table, &mut again));
    assert_eq!(names(&table), ["id", "site", "value", "sites_value"]);
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("north"), Some("10"), None])),
            (2, text(&[Some("south"), Some("20"), Some("S2")])),
        ]
    );
}

/// Sites with other columns: `site` and `height` (a number), numbered in `row`.
fn heights(rows: &[(u64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "row",
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "height",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
        (
            "site",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[test]
fn another_table_of_other_columns_is_joined_anew() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let mut again = Other::of(heights(&[(0, "north", 420)]));
    assert!(step(&mut joined, &mut table, &mut again));
    assert_eq!(names(&table), ["id", "site", "value", "height"]);
    assert_eq!(
        shown(&table),
        [(1, text(&[Some("north"), Some("10"), Some("420")]))]
    );
    // Its rows go on coming, in its columns.
    again.upsert_batch(heights(&[(1, "north", 421)]));
    assert!(step(&mut joined, &mut table, &mut again));
    assert_eq!(
        shown(&table),
        [(1, text(&[Some("north"), Some("10"), Some("421")]))]
    );
}

#[test]
fn another_table_that_cannot_be_joined_gives_the_source_back_its_own_columns() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) =
        joined_to(&mut other, &[(1, Some("north"), 10), (2, Some("east"), 20)]);
    // Read again without the column looked up in.
    let batch =
        RecordBatch::try_from_iter([("row", Arc::new(UInt64Array::from(vec![0])) as ArrayRef)])
            .unwrap();
    let mut again = Other::of(batch);
    assert!(step(&mut joined, &mut table, &mut again));
    assert_eq!(names(&table), ["id", "site", "value"]);
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("north"), Some("10")])),
            (2, text(&[Some("east"), Some("20")])),
        ]
    );
    assert!(joined.join_for(&table).is_none());
    assert!(
        joined
            .describe()
            .starts_with("join off: sites has no column site"),
        "{}",
        joined.describe()
    );
    // The source's rows go on into its own table.
    table.upsert(readings(&[(3, Some("west"), 30)])).unwrap();
    assert!(
        !step(&mut joined, &mut table, &mut again),
        "not tried again"
    );
}

#[test]
fn a_join_made_goes_on_while_the_other_table_has_no_columns() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let mut offer = other.offer();
    offer.instance = None;
    assert!(!go(&mut joined, &mut table, Some(&offer), None));
    assert!(!go(&mut joined, &mut table, None, None));
    assert_eq!(joined.describe(), "joined with sites on site");
    // The source's rows still go in through it, joined to what it has.
    let join = joined.join_for(&table).expect("still made");
    join.upsert_left(&mut table, readings(&[(2, Some("north"), 20)]))
        .unwrap();
    assert_eq!(
        shown(&table)[1],
        (2, text(&[Some("north"), Some("20"), Some("N1")]))
    );
}

#[test]
fn rows_of_the_other_table_without_a_key_name_nothing() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let batch = RecordBatch::try_from_iter([
        ("row", Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef),
        (
            "site",
            Arc::new(StringArray::from(vec![None, Some("north")])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec!["x", "N2"])) as ArrayRef,
        ),
    ])
    .unwrap();
    other.upsert_batch(batch);
    assert!(step(&mut joined, &mut table, &mut other));
    assert_eq!(
        shown(&table),
        [(1, text(&[Some("north"), Some("10"), Some("N2")]))]
    );
    assert_eq!(
        joined.describe(),
        "joined with sites on site",
        "nothing refused"
    );
}

#[test]
fn a_join_waits_for_the_other_table_and_says_why_it_cannot_be() {
    let batch = readings(&[(1, Some("north"), 10)]);
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    let mut joined = Joined::new(JoinSpec::parse("site:sites").unwrap());
    assert_eq!(
        joined.describe(),
        "join waiting: waiting for this source's rows"
    );
    assert!(!go(&mut joined, &mut table, None, None), "nothing changed");
    assert_eq!(
        joined.describe(),
        "join waiting: no table named sites is open"
    );
    let mut other = Other::new(&[]);
    let mut offer = other.offer();
    offer.instance = None;
    assert!(!joined.wants_all(&offer));
    assert!(!go(&mut joined, &mut table, Some(&offer), None));
    assert_eq!(joined.describe(), "join waiting: sites has no rows yet");

    let mut off = |spec: &str| {
        let mut joined = Joined::new(JoinSpec::parse(spec).unwrap());
        assert!(!step(&mut joined, &mut table.clone(), &mut other), "{spec}");
        let offer = other.offer();
        assert!(!joined.wants_all(&offer), "not tried again: {spec}");
        assert!(
            joined.is_idle(&offer, offer.instance.unwrap(), false),
            "{spec}"
        );
        joined.describe()
    };
    assert_eq!(
        off("site:sites.place"),
        "join off: sites has no column place: join=COLUMN:sites.KEY names its column"
    );
    assert_eq!(
        off("place:sites.site"),
        "join off: this source's rows have no column place"
    );
    let said = off("value:sites.site");
    assert!(
        said.starts_with("join off: column \"value\" cannot name"),
        "{said}"
    );

    let mut joined = Joined::new(JoinSpec::parse("site:sites").unwrap());
    let mut offer = other.offer();
    offer.joined = true;
    assert!(!joined.wants_all(&offer));
    go(&mut joined, &mut table, Some(&offer), None);
    assert!(
        joined
            .describe()
            .contains("sites is itself joined to another table"),
        "{}",
        joined.describe()
    );
    joined.refuse("a table cannot be joined to itself");
    assert!(joined.is_idle(&offer, offer.instance.unwrap(), false));
    joined.restart();
    assert!(joined.describe().starts_with("join waiting"));
}

#[test]
fn rows_the_join_cannot_take_are_said_until_rows_are_taken() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, mut table) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    let mut offer = other.offer();
    // Rows of other columns than the other table's.
    offer.arrived = vec![readings(&[(5, Some("north"), 1)])];
    go(&mut joined, &mut table, Some(&offer), None);
    let said = joined.describe();
    assert!(
        said.starts_with("joined with sites on site, sites's rows refused:"),
        "{said}"
    );
    assert_eq!(
        shown(&table),
        [(1, text(&[Some("north"), Some("10"), Some("N1")]))],
        "nothing of them joined"
    );
    other.upsert(&[(0, "north", "N2")]);
    step(&mut joined, &mut table, &mut other);
    assert_eq!(joined.describe(), "joined with sites on site");
}

#[test]
fn a_source_table_made_anew_is_joined_anew() {
    let mut other = Other::new(&[(0, "north", "N1")]);
    let (mut joined, _) = joined_to(&mut other, &[(1, Some("north"), 10)]);
    // A Flight result read again: the source's own columns, in a table of their own.
    let batch = readings(&[(7, Some("north"), 70)]);
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    assert!(joined.join_for(&table).is_none(), "not the join's table");
    assert_eq!(
        joined.describe(),
        "join waiting: waiting for this source's rows"
    );
    table.upsert(batch).unwrap();
    assert!(step(&mut joined, &mut table, &mut other));
    assert_eq!(
        shown(&table),
        [(7, text(&[Some("north"), Some("70"), Some("N1")]))]
    );
    assert!(joined.join_for(&table).is_some());
}

#[test]
fn rows_may_name_a_file_row_by_its_number() {
    let mut other = Other::new(&[(0, "north", "N1"), (1, "south", "S1")]);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("n", Arc::new(UInt64Array::from(vec![1, 5])) as ArrayRef),
    ])
    .unwrap();
    let mut table = LiveTable::new(batch.schema(), "id").unwrap();
    table.upsert(batch).unwrap();
    let mut joined = Joined::new(JoinSpec::parse("n:sites.row").unwrap());
    assert!(
        step(&mut joined, &mut table, &mut other),
        "{}",
        joined.describe()
    );
    assert_eq!(names(&table), ["id", "n", "site", "value"]);
    assert_eq!(
        shown(&table),
        [
            (1, text(&[Some("1"), Some("south"), Some("S1")])),
            (2, text(&[Some("5"), None, None])),
        ]
    );
}

mod live;
