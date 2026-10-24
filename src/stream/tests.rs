use arrow_array::types::{Int64Type, UInt64Type};
use arrow_array::{Float64Array, StringArray};

use super::*;

#[test]
fn a_stream_url_says_how_to_read_it_after_the_hash() {
    let spec = StreamSpec::parse(" wss://feed.example.com:9443/v1/trades?x=1#key=id&subscribe=%7B%22op%22%3A1%7D&subscribe=b&arrow ").unwrap();
    assert_eq!(spec.url, "wss://feed.example.com:9443/v1/trades?x=1");
    assert_eq!(spec.key.as_deref(), Some("id"));
    assert_eq!(spec.subscribe, [r#"{"op":1}"#, "b"]);
    assert!(spec.arrow);
    assert_eq!(spec.title(), "feed.example.com:9443/v1/trades");
    assert_eq!(spec.name_hint(), "trades");

    let plain = StreamSpec::parse("ws://127.0.0.1:9000").unwrap();
    assert_eq!(
        (plain.key, plain.arrow, plain.subscribe.len()),
        (None, false, 0)
    );
    assert_eq!(
        StreamSpec::parse("ws://127.0.0.1:9000/")
            .unwrap()
            .name_hint(),
        "127"
    );
    assert_eq!(
        StreamSpec::parse("ws://localhost:9000")
            .unwrap()
            .name_hint(),
        "localhost"
    );
}

#[test]
fn a_user_and_password_or_a_query_are_neither_shown_nor_kept() {
    let refused = StreamSpec::parse("wss://me:secret@feed.example.com/v1#key=id");
    assert!(refused
        .unwrap_err()
        .contains("no user and password in a URL"));
    let slashed = StreamSpec::parse("wss://me:p/w@feed.example.com/v1").unwrap_err();
    assert!(slashed.contains("no user and password"), "{slashed}");
    let spec =
        StreamSpec::parse("WSS://Feed.Example.com:9443/v1/trades?token=t#key=id&arrow&subscribe=s")
            .unwrap();
    assert_eq!(spec.title(), "Feed.Example.com:9443/v1/trades");
    assert_eq!(spec.name_hint(), "trades");
    assert_eq!(
        spec.origin(),
        "wss://feed.example.com:9443/v1/trades#key=id&arrow"
    );
    let bare = StreamSpec::parse("ws://host").unwrap();
    assert_eq!(
        (bare.title().as_str(), bare.name_hint().as_str()),
        ("host", "host")
    );
    assert_eq!(bare.origin(), "ws://host/");
    let other_key = StreamSpec::parse("ws://host#key=x").unwrap();
    assert_ne!(
        other_key.origin(),
        bare.origin(),
        "another key: another source"
    );
}

#[test]
fn unknown_or_broken_options_are_named() {
    let error = |text| StreamSpec::parse(text).unwrap_err();
    assert!(error("ws://h/p#keys=a").contains("unknown stream option keys=..."));
    assert!(error("ws://h/p#key=").contains("key needs a value"));
    assert!(error("ws://h/p#subscribe").contains("subscribe needs a value"));
    assert!(error("ws://h/p#arrow=yes").contains("unknown stream option"));
    assert!(error("ws://h/p#key=%G0").contains("not percent-encoded"));
    assert!(error("ws://h/p#key=%E2%82").contains("not percent-encoded"));
    assert!(error("ws://h/p#key=a%2").contains("not percent-encoded"));
    assert!(error("ws://h/p#key=%+1").contains("not percent-encoded"));
    assert!(error("ws://h/p#a&b").contains("%26"));
}

#[test]
fn headers_are_given_after_the_hash_and_never_kept() {
    let spec = StreamSpec::parse(
        "ws://h/p#header=authorization:%20Bearer%20$X&header=sec-websocket-protocol:json",
    )
    .unwrap();
    assert!(spec.headers.iter().all(|(name, _)| !name.is_empty()));
    assert_eq!(
        spec.headers,
        [
            ("authorization".to_owned(), "Bearer $X".to_owned()),
            ("sec-websocket-protocol".to_owned(), "json".to_owned())
        ]
    );
    assert_eq!(
        spec.origin(),
        "ws://h/p",
        "headers are not what a stream is known by"
    );
    assert_eq!(header_value("json").unwrap(), "json");
    assert_eq!(
        header_value("$PATH").unwrap(),
        std::env::var("PATH").unwrap()
    );
    let unset = StreamSpec::parse("ws://127.0.0.1:9/x#header=a:$DENSA_STUDIO_TEST_UNSET");
    let error = unset.unwrap().connect(|| {}).unwrap_err();
    assert_eq!(error, "$DENSA_STUDIO_TEST_UNSET is not set");
    let error = StreamSpec::parse("ws://h/p#header=:x").unwrap_err();
    assert!(error.contains("header=NAME:VALUE"), "{error}");
    let error = StreamSpec::parse("ws://h/p#header").unwrap_err();
    assert_eq!(error, "header needs a value");
}

#[test]
fn header_values_read_the_environment_wherever_named() {
    let path = std::env::var("PATH").unwrap();
    assert_eq!(
        header_value("Bearer $PATH").unwrap(),
        format!("Bearer {path}")
    );
    assert_eq!(header_value("a${PATH}b").unwrap(), format!("a{path}b"));
    assert_eq!(header_value("$PATH-x").unwrap(), format!("{path}-x"));
    assert_eq!(header_value("cost: $$5").unwrap(), "cost: $5");
    assert_eq!(header_value("plain").unwrap(), "plain");
    assert_eq!(
        header_value("$DENSA_STUDIO_TEST_UNSET").unwrap_err(),
        "$DENSA_STUDIO_TEST_UNSET is not set"
    );
    assert!(header_value("${PATH")
        .unwrap_err()
        .contains("without its }"));
    assert!(header_value("a $ b").unwrap_err().contains("write $$"));
    assert!(header_value("${}").unwrap_err().contains("no name"));
}

#[test]
fn a_value_that_is_not_percent_encoded_is_not_repeated() {
    let error = StreamSpec::parse("ws://h/p#header=authorization:secret%ZZ").unwrap_err();
    assert_eq!(error, "header= is not percent-encoded");
    let twice = StreamSpec::parse("ws://h/p#key=a&key=b").unwrap_err();
    assert_eq!(twice, "key= is given twice");
}

#[test]
fn option_values_are_written_back_as_they_read() {
    let value = "a&b#c%d e";
    assert_eq!(encode(value), "a%26b%23c%25d%20e");
    let (_, options) = options(&format!("ws://h#key={}", encode(value))).unwrap();
    assert_eq!(options, [("key".to_owned(), value.to_owned())]);
    let spec = StreamSpec::parse("ws://h/p#key=a%26b").unwrap();
    assert_eq!(spec.origin(), "ws://h/p#key=a%26b");
}

#[test]
fn a_stream_can_name_its_table() {
    let spec = StreamSpec::parse("ws://h/readings#name=recent&key=id").unwrap();
    assert_eq!(spec.name_hint(), "recent");
    assert_eq!(spec.origin(), "ws://h/readings#key=id&name=recent");
    assert!(format!("{spec:?}").contains("name: Some(\"recent\")"));
    let twice = StreamSpec::parse("ws://h/p#name=a&name=b").unwrap_err();
    assert_eq!(twice, "name= is given twice");
    assert_eq!(
        StreamSpec::parse("ws://h/p#name").unwrap_err(),
        "name needs a value"
    );
    assert_eq!(
        StreamSpec::parse("ws://h/readings").unwrap().name_hint(),
        "readings"
    );
}

#[test]
fn a_time_column_is_part_of_what_a_stream_is_known_by() {
    let spec = StreamSpec::parse("ws://h/p#key=id&time=at&window=10m&unit=s").unwrap();
    let time = spec.time.as_ref().unwrap();
    assert_eq!(time.column, "at");
    assert_eq!(time.window, Some(Duration::from_secs(600)));
    assert_eq!(spec.origin(), "ws://h/p#key=id&time=at&window=10m&unit=s");
    assert!(format!("{spec:?}").contains("time: Some"));
    let alone = StreamSpec::parse("ws://h/p#time=at").unwrap();
    assert_eq!(alone.origin(), "ws://h/p#time=at");
    let keyed = StreamSpec::parse("ws://h/p#key=at&time=at").unwrap_err();
    assert!(keyed.contains("a key cannot be a time"), "{keyed}");
    let error = StreamSpec::parse("ws://h/p#window=10m").unwrap_err();
    assert!(error.contains("time=COLUMN"), "{error}");
    let error = StreamSpec::parse("ws://h/p#later").unwrap_err();
    assert!(
        error.ends_with("name=TABLE, time=COLUMN, unit=s, window=10m and per=1m"),
        "{error}"
    );
}

#[test]
fn a_debug_print_leaves_header_values_out() {
    let spec = StreamSpec::parse("ws://h/p#header=authorization:secret&subscribe=s").unwrap();
    let printed = format!("{spec:?}");
    assert!(
        printed.contains("authorization") && !printed.contains("secret"),
        "{printed}"
    );
}

#[test]
fn urls_are_split_into_scheme_host_and_path() {
    assert_eq!(parts("wss://u:p@h:1/a/b?t=1#x"), ("wss", "h:1", "a/b"));
    assert_eq!(parts("grpc://h"), ("grpc", "h", ""));
    assert_eq!(parts("no-scheme/x"), ("", "no-scheme", "x"));
    assert_eq!(option_text("key", "id"), "key=...");
    assert_eq!(option_text("arrow", ""), "arrow");
}

#[test]
fn streams_are_told_from_files_by_their_scheme() {
    assert!(StreamSpec::is_stream("ws://a/b"));
    assert!(StreamSpec::is_stream("  WSS://a/b"));
    assert!(!StreamSpec::is_stream("data/ws.csv"));
    assert!(!StreamSpec::is_stream("https://a/b"));
}

fn rows(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn rows_without_a_key_are_numbered_and_the_oldest_let_go() {
    let batch = rows(vec![(
        "row",
        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
    )]);
    let numbered = numbered(&batch, "row_", 7).unwrap();
    let numbers = numbered.column(1).as_primitive::<UInt64Type>();
    assert_eq!(numbers.values(), &[7, 8]);
    assert_eq!(numbered.schema().field(1).name(), "row_");

    assert!(
        too_old(0, KEPT_ROWS, KEPT_ROWS).is_none(),
        "as many as are kept"
    );
    let gone = too_old(KEPT_ROWS - 1, KEPT_ROWS + 2, KEPT_ROWS).unwrap();
    assert_eq!(gone.values(), &[0, 1]);
    let gone = too_old(KEPT_ROWS + 2, KEPT_ROWS + 5, KEPT_ROWS).unwrap();
    assert_eq!(gone.values(), &[2, 3, 4]);
}

/// A stream on a port nothing answers on.
fn quiet(options: &str) -> Live {
    let spec = StreamSpec::parse(&format!("ws://127.0.0.1:9/quiet{options}")).unwrap();
    Live::connect(&spec, || {}).unwrap()
}

#[test]
fn numbers_take_a_name_the_rows_do_not_have() {
    let mut live = quiet("");
    assert_eq!(live.key(), "row");
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let Arrived::Rows { batch, deleted } = live.ready(rows(vec![("row", names)])) else {
        panic!("refused");
    };
    assert_eq!(live.key(), "row_");
    assert_eq!(batch.schema().field(1).name(), "row_");
    assert!(deleted.is_none());
    assert_eq!(live.rows, 2);
    let keyed = quiet("#key=row");
    assert_eq!(keyed.key(), "row", "a key is the rows' own");
}

#[test]
fn a_reason_from_the_server_is_cut_short() {
    assert_eq!(shortened("bad"), "bad");
    let long = "x".repeat(REASON_CHARS + 5);
    let short = shortened(&long);
    assert_eq!(short.chars().count(), REASON_CHARS + 1);
    assert!(short.ends_with('…'));
    assert_eq!(
        shortened(&"é".repeat(REASON_CHARS)),
        "é".repeat(REASON_CHARS)
    );
}

#[test]
fn the_status_says_how_the_stream_stands() {
    let mut live = quiet("");
    assert_eq!(
        live.status(),
        "connecting",
        "no rate before rows, nothing skipped"
    );
    live.count(5);
    assert_eq!(live.rate, 0.0, "no rate until a second has passed");
    live.counted.0 -= RATE_OVER;
    live.count(5);
    assert!(live.rate > 5.0 && live.rate <= 10.0, "{}", live.rate);
    assert!(
        live.status().starts_with("connecting, 10 rows/s"),
        "{}",
        live.status()
    );
    live.skipped = (2, "bad".to_owned());
    assert!(
        live.status().ends_with(", 2 messages skipped (bad)"),
        "{}",
        live.status()
    );
    live.counted.0 -= 2 * RATE_OVER;
    assert!(
        live.status().starts_with("connecting, 0 rows/s"),
        "a quiet stream: {}",
        live.status()
    );
}

#[test]
fn only_rows_kept_for_a_while_are_let_go() {
    let many: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![
        0;
        KEPT_ROWS as usize + 1
    ]));
    let batch = rows(vec![("x", many)]);
    let (_, deleted) = Keying::new(None, None).ready(&batch).unwrap();
    assert!(deleted.is_none(), "every row kept");
    let (_, deleted) = Keying::new(None, Some(KEPT_ROWS)).ready(&batch).unwrap();
    assert_eq!(deleted.unwrap().values(), &[0], "the oldest let go");
    let mut keying = Keying::new(None, None);
    keying
        .ready(&rows(vec![(
            "x",
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
        )]))
        .unwrap();
    keying.restart();
    let (numbered, _) = keying
        .ready(&rows(vec![(
            "x",
            Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
        )]))
        .unwrap();
    assert_eq!(
        numbered.column(1).as_primitive::<UInt64Type>().values(),
        &[0]
    );
}

#[test]
fn a_key_of_floats_becomes_whole_numbers() {
    let ids: ArrayRef = Arc::new(Float64Array::from(vec![Some(3.0), None, Some(-7.0)]));
    let batch = rows(vec![
        ("id", ids),
        ("x", Arc::new(Float64Array::from(vec![1.5; 3]))),
    ]);
    let keyed = whole_key(&batch, "id").unwrap();
    let ids = keyed.column(0).as_primitive::<Int64Type>();
    assert_eq!(ids.iter().collect::<Vec<_>>(), [Some(3), None, Some(-7)]);
    assert_eq!(keyed.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(keyed.schema().field(1).data_type(), &DataType::Float64);

    let fraction: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
    assert!(whole_key(&rows(vec![("id", fraction)]), "id")
        .unwrap_err()
        .contains("1.5"));
    let huge: ArrayRef = Arc::new(Float64Array::from(vec![MAX_WHOLE_FLOAT + 2.0]));
    assert!(whole_key(&rows(vec![("id", huge)]), "id").is_err());
    let largest: ArrayRef = Arc::new(Float64Array::from(vec![-MAX_WHOLE_FLOAT]));
    assert!(whole_key(&rows(vec![("id", largest)]), "id").is_ok());
    let nan: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
    assert!(whole_key(&rows(vec![("id", nan)]), "id").is_err());
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let text = rows(vec![("id", names)]);
    assert_eq!(whole_key(&text, "id").unwrap(), text, "text keys stay");
    assert!(whole_key(&text, "sensor")
        .unwrap_err()
        .contains("no column sensor"));
}
