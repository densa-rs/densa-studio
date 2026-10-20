//! Live sources: WebSocket streams of JSON or Arrow IPC rows, opened from a URL whose part
//! after `#` (never sent to the server) says how to read them:
//! `ws://host/path#key=sensor&subscribe=TEXT&arrow`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use densa_ws::{Stream, StreamEvent, StreamOptions};

/// The column that numbers a stream's rows when it has no key (`row_`, `row__` and so on
/// when its rows have a column of that name).
const ROW: &str = "row";
/// Rows of a stream without a key kept at most: older ones are deleted as new ones come.
pub(crate) const KEPT_ROWS: u64 = 1_000_000;
/// How long the rate of rows is counted over.
const RATE_OVER: Duration = Duration::from_secs(1);
/// Characters of a skipped message's reason shown, at most: the server writes it.
const REASON_CHARS: usize = 120;
/// The largest whole number a float key may hold: past it, floats skip whole numbers, and
/// two keys could be one.
const MAX_WHOLE_FLOAT: f64 = 9_007_199_254_740_992.0;

/// How to read a stream: what its URL says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamSpec {
    /// The URL without the part after `#`.
    pub(crate) url: String,
    /// The column whose rows replace each other (the latest row of each key is kept); none
    /// keeps every row, numbered.
    pub(crate) key: Option<String>,
    /// Arrow IPC messages rather than JSON.
    pub(crate) arrow: bool,
    /// Messages sent each time the connection opens.
    pub(crate) subscribe: Vec<String>,
}

impl StreamSpec {
    /// Whether `text` names a stream rather than a file.
    pub(crate) fn is_stream(text: &str) -> bool {
        let lower = text.trim_start().to_ascii_lowercase();
        lower.starts_with("ws://") || lower.starts_with("wss://")
    }

    /// Reads a stream's URL: `key=COLUMN`, `subscribe=TEXT` (percent-encoded, any number of
    /// times) and `arrow`, separated by `&` after `#`.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let (url, options) = text.split_once('#').unwrap_or((text, ""));
        let mut spec = Self {
            url: url.to_owned(),
            key: None,
            arrow: false,
            subscribe: Vec::new(),
        };
        for option in options.split('&').filter(|o| !o.is_empty()) {
            let (name, value) = option.split_once('=').unwrap_or((option, ""));
            let value = decode(value).ok_or_else(|| format!("{option} is not percent-encoded"))?;
            match (name, value.is_empty()) {
                ("key", false) => spec.key = Some(value),
                ("subscribe", false) => spec.subscribe.push(value),
                ("arrow", true) => spec.arrow = true,
                ("key" | "subscribe", true) => return Err(format!("{name} needs a value")),
                _ => {
                    return Err(format!(
                        "unknown stream option {option}: Studio knows key=COLUMN, \
                         subscribe=TEXT (with & written %26) and arrow"
                    ))
                }
            }
        }
        Ok(spec)
    }

    /// The host and path, without a user and password or a query: what the stream's tab
    /// says.
    pub(crate) fn title(&self) -> String {
        let rest = self
            .url
            .split_once("://")
            .map_or(&*self.url, |(_, rest)| rest);
        let rest = rest.split(['?', '#']).next().unwrap_or(rest);
        let (host, path) = rest.split_once('/').map_or((rest, ""), |(h, p)| (h, p));
        // A user and password come before the host's `@`.
        let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
        match path.is_empty() {
            true => host.to_owned(),
            false => format!("{host}/{path}"),
        }
    }

    /// What the stream is known by, to find it open again and in a saved arrangement: its
    /// scheme and host in lower case, its path, and how its rows are keyed and read; not a
    /// user and password, a query or a subscription, which may hold secrets.
    pub(crate) fn origin(&self) -> String {
        let scheme = self
            .url
            .split_once("://")
            .map_or("ws", |(scheme, _)| scheme);
        let title = self.title();
        let (host, path) = title.split_once('/').map_or((&*title, ""), |(h, p)| (h, p));
        let mut origin = format!(
            "{}://{}/{path}",
            scheme.to_ascii_lowercase(),
            host.to_ascii_lowercase()
        );
        let mut options = Vec::new();
        if let Some(key) = &self.key {
            options.push(format!("key={key}"));
        }
        if self.arrow {
            options.push("arrow".to_owned());
        }
        if !options.is_empty() {
            origin.push('#');
            origin.push_str(&options.join("&"));
        }
        origin
    }

    /// What its table is named after: the last part of the path, or the host.
    pub(crate) fn name_hint(&self) -> String {
        let title = self.title();
        let mut parts = title.split('/').filter(|p| !p.is_empty());
        let host = parts.next().unwrap_or("stream").to_owned();
        let last = parts.next_back().map(str::to_owned);
        // A host's dots and port are not a table name's.
        last.unwrap_or_else(|| host.split([':', '.']).next().unwrap_or("stream").to_owned())
    }

    /// Starts reading the stream, calling `wake` after each event.
    pub(crate) fn connect(&self, wake: impl Fn() + Send + 'static) -> Result<Stream, String> {
        let mut options = match self.arrow {
            true => StreamOptions::arrow_ipc(self.url.clone()),
            false => StreamOptions::json_inferred(self.url.clone()),
        };
        for message in &self.subscribe {
            options = options.on_connect(message.clone());
        }
        options.connect(wake).map_err(|e| e.to_string())
    }
}

/// `%XX` escapes in `text` decoded; `None` for a broken escape or bytes that are not UTF-8.
fn decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' {
            let hex = text.get(at + 1..at + 3)?;
            if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            out.push(u8::from_str_radix(hex, 16).ok()?);
            at += 3;
        } else {
            out.push(bytes[at]);
            at += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// How a stream's rows are keyed in its table.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Keying {
    /// By a column of the rows.
    Column(String),
    /// By a number added to each row, counting from 0.
    Numbered { name: String, next: u64 },
}

/// A stream being read: its rows readied for a table, and how it stands.
pub(crate) struct Live {
    stream: Stream,
    keying: Keying,
    /// Whether the rows' columns have been seen (the key is settled with them).
    started: bool,
    state: String,
    /// Messages skipped, and why the last one was.
    skipped: (u64, String),
    /// Rows taken in.
    rows: u64,
    /// Rows counted since `since`, and the rate last worked out.
    counted: (Instant, u64),
    rate: f64,
}

/// What came from a stream.
pub(crate) enum Arrived {
    /// Rows to upsert; `deleted` is the numbered rows now too old to keep.
    Rows {
        batch: RecordBatch,
        deleted: Option<UInt64Array>,
    },
    /// Rows that cannot go into the table, and why.
    Refused { rows: usize, reason: String },
}

impl Live {
    /// Reads `spec`'s stream, calling `wake` after each event.
    pub(crate) fn connect(
        spec: &StreamSpec,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, String> {
        let keying = match &spec.key {
            Some(key) => Keying::Column(key.clone()),
            None => Keying::Numbered {
                name: ROW.to_owned(),
                next: 0,
            },
        };
        Ok(Self {
            stream: spec.connect(wake)?,
            keying,
            started: false,
            state: "connecting".to_owned(),
            skipped: (0, String::new()),
            rows: 0,
            counted: (Instant::now(), 0),
            rate: 0.0,
        })
    }

    /// The key column of the stream's table.
    pub(crate) fn key(&self) -> &str {
        match &self.keying {
            Keying::Column(name) | Keying::Numbered { name, .. } => name,
        }
    }

    /// The next rows that arrived, taking in the events before them; `None` when there are
    /// none for now.
    pub(crate) fn next(&mut self) -> Option<Arrived> {
        loop {
            match self.stream.try_next()? {
                StreamEvent::Connected => "connected".clone_into(&mut self.state),
                StreamEvent::Batch(batch) => return Some(self.ready(batch)),
                StreamEvent::Invalid { reason, .. } => {
                    self.skipped.0 += 1;
                    self.skipped.1 = reason.chars().take(REASON_CHARS).collect();
                }
                StreamEvent::Disconnected { reason, retry_in } => {
                    let seconds = retry_in.as_secs_f64();
                    self.state = format!("reconnecting in {seconds:.1} s ({reason})");
                }
                StreamEvent::Stopped { reason } => self.state = format!("stopped: {reason}"),
                _ => {}
            }
        }
    }

    /// `batch` keyed as the table is.
    fn ready(&mut self, batch: RecordBatch) -> Arrived {
        let rows = batch.num_rows();
        if !self.started {
            self.started = true;
            if let Keying::Numbered { name, .. } = &mut self.keying {
                while batch.schema().column_with_name(name).is_some() {
                    name.push('_');
                }
            }
        }
        let keyed = match &mut self.keying {
            Keying::Column(key) => whole_key(&batch, key).map(|batch| (batch, None)),
            Keying::Numbered { name, next } => numbered(&batch, name, *next).map(|batch| {
                let first = *next;
                *next += rows as u64;
                (batch, too_old(first, *next))
            }),
        };
        match keyed {
            Ok((batch, deleted)) => {
                self.count(rows);
                Arrived::Rows { batch, deleted }
            }
            Err(reason) => Arrived::Refused { rows, reason },
        }
    }

    fn count(&mut self, rows: usize) {
        self.rows += rows as u64;
        self.counted.1 += rows as u64;
        let elapsed = self.counted.0.elapsed();
        if elapsed >= RATE_OVER {
            // Lossless enough: a rate to show.
            self.rate = self.counted.1 as f64 / elapsed.as_secs_f64();
            self.counted = (Instant::now(), 0);
        }
    }

    /// How the stream stands, for the status bar: its connection, rows per second, and
    /// messages skipped.
    pub(crate) fn status(&self) -> String {
        let mut text = self.state.clone();
        // A stream gone quiet: its rate is what has come since.
        let since = self.counted.0.elapsed();
        let rate = match since >= 2 * RATE_OVER {
            // Lossless enough: a rate to show.
            true => self.counted.1 as f64 / since.as_secs_f64(),
            false => self.rate,
        };
        if self.rows > 0 {
            // Lossless enough: a rate to show.
            text.push_str(&format!(
                ", {} rows/s",
                crate::source::grouped(rate.round() as u64)
            ));
        }
        if self.skipped.0 > 0 {
            let count = crate::source::grouped(self.skipped.0);
            text.push_str(&format!(", {count} messages skipped ({})", self.skipped.1));
        }
        text
    }
}

/// The numbered rows to delete once `next` rows have come, `first` of them before this
/// batch: those that fell out of the newest [`KEPT_ROWS`].
fn too_old(first: u64, next: u64) -> Option<UInt64Array> {
    let from = first.saturating_sub(KEPT_ROWS);
    let to = next.saturating_sub(KEPT_ROWS);
    (from < to).then(|| UInt64Array::from_iter_values(from..to))
}

/// `batch` with a column `name` numbering its rows from `first`.
fn numbered(batch: &RecordBatch, name: &str, first: u64) -> Result<RecordBatch, String> {
    let count = batch.num_rows() as u64;
    let numbers: ArrayRef = Arc::new(UInt64Array::from_iter_values(first..first + count));
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(name, DataType::UInt64, false));
    let mut columns = batch.columns().to_vec();
    columns.push(numbers);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

/// `batch` with its key column `key` as whole numbers if it holds floats (a JSON stream's
/// numbers are read as floats; a table is keyed by whole numbers or text).
fn whole_key(batch: &RecordBatch, key: &str) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let (index, field) = schema
        .column_with_name(key)
        .ok_or_else(|| format!("the rows have no column {key} to key them by"))?;
    let Some(floats) = batch.column(index).as_primitive_opt::<Float64Type>() else {
        return Ok(batch.clone());
    };
    let mut whole = Vec::with_capacity(floats.len());
    for at in 0..floats.len() {
        if floats.is_null(at) {
            whole.push(None);
            continue;
        }
        let value = floats.value(at);
        if value.fract() != 0.0 || value.abs() > MAX_WHOLE_FLOAT {
            return Err(format!("{key} {value} is not a whole number up to 2^53"));
        }
        // Lossless: a whole number within i64.
        whole.push(Some(value as i64));
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[index] = Field::new(field.name(), DataType::Int64, field.is_nullable());
    let mut columns = batch.columns().to_vec();
    columns[index] = Arc::new(Int64Array::from(whole));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
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
        let spec = StreamSpec::parse(
            "WSS://me:secret@Feed.Example.com:9443/v1/trades?token=t#key=id&arrow&subscribe=s",
        )
        .unwrap();
        assert_eq!(spec.title(), "Feed.Example.com:9443/v1/trades");
        assert_eq!(spec.name_hint(), "trades");
        assert_eq!(
            spec.origin(),
            "wss://feed.example.com:9443/v1/trades#key=id&arrow"
        );
        let bare = StreamSpec::parse("ws://me@host").unwrap();
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
        assert!(error("ws://h/p#keys=a").contains("unknown stream option keys=a"));
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

        assert!(too_old(0, KEPT_ROWS).is_none(), "as many as are kept");
        let gone = too_old(KEPT_ROWS - 1, KEPT_ROWS + 2).unwrap();
        assert_eq!(gone.values(), &[0, 1]);
        let gone = too_old(KEPT_ROWS + 2, KEPT_ROWS + 5).unwrap();
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
}
