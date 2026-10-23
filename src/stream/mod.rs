//! Live sources: WebSocket streams of JSON or Arrow IPC rows, opened from a URL whose part
//! after `#` (never sent to the server) says how to read them:
//! `ws://host/path#key=sensor&subscribe=TEXT&arrow&header=authorization:$TOKEN`, with a time
//! window `time=COLUMN&window=10m` if wanted. Also what remote sources share: their options,
//! their keying, their headers.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use densa_ws::{Stream, StreamEvent, StreamOptions};

use crate::time::{TimeOptions, TimeSpec};

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

/// How to read a stream: what its URL says. Its `Debug` output leaves header values out.
#[derive(Clone, PartialEq, Eq)]
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
    /// Headers of the request that opens the connection, as given (`$NAME` values are read
    /// from the environment when it opens).
    pub(crate) headers: Vec<(String, String)>,
    /// The column of the rows' times, and the window kept over it, if given.
    pub(crate) time: Option<TimeSpec>,
    /// The table name asked for.
    pub(crate) name: Option<String>,
}

impl std::fmt::Debug for StreamSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let headers: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("StreamSpec")
            .field("url", &self.url)
            .field("key", &self.key)
            .field("arrow", &self.arrow)
            .field("subscribe", &self.subscribe.len())
            .field("headers", &headers)
            .field("time", &self.time)
            .field("name", &self.name)
            .finish()
    }
}

impl StreamSpec {
    /// Whether `text` names a stream rather than a file.
    pub(crate) fn is_stream(text: &str) -> bool {
        let lower = text.trim_start().to_ascii_lowercase();
        lower.starts_with("ws://") || lower.starts_with("wss://")
    }

    /// Reads a stream's URL: `key=COLUMN`, `subscribe=TEXT` (any number of times), `arrow`,
    /// `header=NAME:VALUE` (any number of times), `name=TABLE` and a window
    /// (`time=COLUMN&window=10m`, `unit=s`), percent-encoded and separated by `&` after `#`.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let (url, options) = options(text)?;
        let mut spec = Self {
            url,
            key: None,
            arrow: false,
            subscribe: Vec::new(),
            headers: Vec::new(),
            time: None,
            name: None,
        };
        let mut time = TimeOptions::default();
        for (name, value) in options {
            if time.take(&name, &value)? {
                continue;
            }
            match (name.as_str(), value.is_empty()) {
                ("key", false) if spec.key.is_some() => return Err("key= is given twice".into()),
                ("key", false) => spec.key = Some(value),
                ("name", false) if spec.name.is_some() => return Err("name= is given twice".into()),
                ("name", false) => spec.name = Some(value),
                ("subscribe", false) => spec.subscribe.push(value),
                ("header", false) => spec.headers.push(header(&value)?),
                ("arrow", true) => spec.arrow = true,
                ("key" | "subscribe" | "header" | "name", true) => {
                    return Err(format!("{name} needs a value"))
                }
                _ => {
                    return Err(format!(
                        "unknown stream option {}: Studio knows key=COLUMN, subscribe=TEXT \
                         (with & written %26), arrow, header=NAME:VALUE, name=TABLE and {}",
                        option_text(&name, &value),
                        TimeOptions::KNOWN
                    ))
                }
            }
        }
        spec.time = time.finish(spec.key.as_deref())?;
        Ok(spec)
    }

    /// The host and path, without a user and password or a query: what the stream's tab
    /// says.
    pub(crate) fn title(&self) -> String {
        let (_, host, path) = parts(&self.url);
        match path.is_empty() {
            true => host.to_owned(),
            false => format!("{host}/{path}"),
        }
    }

    /// What the stream is known by, to find it open again and in a saved arrangement: its
    /// scheme and host in lower case, its path, and how its rows are keyed, read and kept;
    /// not a user and password, a query or a subscription, which may hold secrets.
    pub(crate) fn origin(&self) -> String {
        let (scheme, host, path) = parts(&self.url);
        let mut origin = format!(
            "{}://{}/{path}",
            scheme.to_ascii_lowercase(),
            host.to_ascii_lowercase()
        );
        let mut options = Vec::new();
        if let Some(key) = &self.key {
            options.push(format!("key={}", encode(key)));
        }
        if self.arrow {
            options.push("arrow".to_owned());
        }
        if let Some(time) = &self.time {
            options.extend(time.options());
        }
        if let Some(name) = &self.name {
            options.push(format!("name={}", encode(name)));
        }
        if !options.is_empty() {
            origin.push('#');
            origin.push_str(&options.join("&"));
        }
        origin
    }

    /// What its table is named after: the name asked for, the last part of the path, or the
    /// host.
    pub(crate) fn name_hint(&self) -> String {
        if let Some(name) = &self.name {
            return name.clone();
        }
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
        for (name, value) in &self.headers {
            options = options.header(name.clone(), header_value(value)?);
        }
        options.connect(wake).map_err(|e| e.to_string())
    }
}

/// `text` (trimmed) split at its first `#` into the URL and its options, decoded: `name` or
/// `name=VALUE`, separated by `&`.
pub(crate) fn options(text: &str) -> Result<(String, Vec<(String, String)>), String> {
    let text = text.trim();
    let (url, options) = text.split_once('#').unwrap_or((text, ""));
    if url.contains('@') {
        return Err(
            "Studio takes no user and password in a URL: give them as a header, \
             header=authorization:$NAME"
                .to_owned(),
        );
    }
    let mut decoded = Vec::new();
    for option in options.split('&').filter(|o| !o.is_empty()) {
        let (name, value) = option.split_once('=').unwrap_or((option, ""));
        // The value may be a secret: the message names the option alone.
        let value = decode(value).ok_or_else(|| format!("{name}= is not percent-encoded"))?;
        decoded.push((name.to_owned(), value));
    }
    Ok((url.to_owned(), decoded))
}

/// `value` written as an option's value: `%`, `&` and `#` (and spaces) escaped, so it reads
/// back as it is.
pub(crate) fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '%' => out.push_str("%25"),
            '&' => out.push_str("%26"),
            '#' => out.push_str("%23"),
            ' ' => out.push_str("%20"),
            c => out.push(c),
        }
    }
    out
}

/// How an option was written, for a message about it (a header's value left out).
pub(crate) fn option_text(name: &str, value: &str) -> String {
    match value.is_empty() {
        true => name.to_owned(),
        false => format!("{name}=..."),
    }
}

/// A URL's scheme, host (without a user and password) and path (without a query).
pub(crate) fn parts(url: &str) -> (&str, &str, &str) {
    let (scheme, rest) = url.split_once("://").unwrap_or(("", url));
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    // A user and password come before the host's `@`.
    let host = host.rsplit_once('@').map_or(host, |(_, host)| host);
    (scheme, host, path)
}

/// A `NAME:VALUE` header option, both parts there.
pub(crate) fn header(option: &str) -> Result<(String, String), String> {
    match option.split_once(':') {
        Some((name, value)) if !name.trim().is_empty() && !value.trim().is_empty() => {
            Ok((name.trim().to_owned(), value.trim().to_owned()))
        }
        _ => Err(
            "a header is header=NAME:VALUE (VALUE $NAME reads an environment variable)".to_owned(),
        ),
    }
}

/// A header's value with each `$NAME` or `${NAME}` in it read from that environment variable
/// (so a token need not be typed, shown or kept: `Bearer $TOKEN`); `$$` is a `$`.
pub(crate) fn header_value(given: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = given;
    while let Some(at) = rest.find('$') {
        out.push_str(&rest[..at]);
        rest = &rest[at + 1..];
        if let Some(after) = rest.strip_prefix('$') {
            out.push('$');
            rest = after;
            continue;
        }
        let (name, after) = match rest.strip_prefix('{') {
            Some(braced) => {
                let end = braced.find('}').ok_or("a ${NAME} without its }")?;
                (&braced[..end], &braced[end + 1..])
            }
            None => {
                let end = rest
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .unwrap_or(rest.len());
                rest.split_at(end)
            }
        };
        if name.is_empty() {
            return Err("a $ with no name after it (write $$ for a $)".to_owned());
        }
        out.push_str(&std::env::var(name).map_err(|_| format!("${name} is not set"))?);
        rest = after;
    }
    out.push_str(rest);
    Ok(out)
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

/// How a remote source's rows are keyed in its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Keying {
    /// By a column of the rows.
    Column(String),
    /// By a number added to each row, counting from 0; its name is settled by the first
    /// rows' columns (`row`, or `row_` and so on if they have one). With `kept`, only the
    /// newest rows that many are kept.
    Numbered {
        name: String,
        next: u64,
        settled: bool,
        kept: Option<u64>,
    },
}

impl Keying {
    /// By `key`, or numbered without one, keeping the newest `kept` rows if given.
    pub(crate) fn new(key: Option<String>, kept: Option<u64>) -> Self {
        match key {
            Some(key) => Self::Column(key),
            None => Self::Numbered {
                name: ROW.to_owned(),
                next: 0,
                settled: false,
                kept,
            },
        }
    }

    /// The key column of the table.
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Column(name) | Self::Numbered { name, .. } => name,
        }
    }

    /// Numbers from 0 again, and settles the number column's name again: the rows start
    /// over (a Flight result read again).
    pub(crate) fn restart(&mut self) {
        if let Self::Numbered {
            name,
            next,
            settled,
            ..
        } = self
        {
            ROW.clone_into(name);
            *next = 0;
            *settled = false;
        }
    }

    /// `batch` keyed as the table is, and the numbered rows now too old to keep.
    pub(crate) fn ready(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, Option<UInt64Array>), String> {
        match self {
            Self::Column(key) => whole_key(batch, key).map(|batch| (batch, None)),
            Self::Numbered {
                name,
                next,
                settled,
                kept,
            } => {
                if !*settled {
                    *settled = true;
                    while batch.schema().column_with_name(name).is_some() {
                        name.push('_');
                    }
                }
                let numbered = numbered(batch, name, *next)?;
                let first = *next;
                *next += batch.num_rows() as u64;
                Ok((numbered, kept.and_then(|kept| too_old(first, *next, kept))))
            }
        }
    }
}

/// A stream being read: its rows readied for a table, and how it stands.
pub(crate) struct Live {
    stream: Stream,
    keying: Keying,
    state: String,
    /// Messages skipped, and why the last one was.
    skipped: (u64, String),
    /// Rows taken in.
    rows: u64,
    /// Rows counted since `since`, and the rate last worked out.
    counted: (Instant, u64),
    rate: f64,
}

/// What came from a remote source.
pub(crate) enum Arrived {
    /// The rows' columns, keyed as the table is (no rows): a table made anew, the rows
    /// starting over.
    Columns(RecordBatch),
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
        Ok(Self {
            stream: spec.connect(wake)?,
            keying: Keying::new(spec.key.clone(), Some(KEPT_ROWS)),
            state: "connecting".to_owned(),
            skipped: (0, String::new()),
            rows: 0,
            counted: (Instant::now(), 0),
            rate: 0.0,
        })
    }

    /// The key column of the stream's table.
    pub(crate) fn key(&self) -> &str {
        self.keying.key()
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
                    self.skipped.1 = shortened(&reason);
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
        match self.keying.ready(&batch) {
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

/// `reason` cut to [`REASON_CHARS`] characters, with `…` if it was longer.
pub(crate) fn shortened(reason: &str) -> String {
    let mut chars = reason.chars();
    let mut short: String = chars.by_ref().take(REASON_CHARS).collect();
    if chars.next().is_some() {
        short.push('…');
    }
    short
}

/// The numbered rows to delete once `next` rows have come, `first` of them before this
/// batch: those that fell out of the newest `kept`.
fn too_old(first: u64, next: u64, kept: u64) -> Option<UInt64Array> {
    let from = first.saturating_sub(kept);
    let to = next.saturating_sub(kept);
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
mod tests;
