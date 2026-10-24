//! Arrow Flight sources: a Flight service's data, opened from a `grpc://` or `grpc+tls://`
//! URL whose part after `#` says what to ask for and how to keep it:
//! `grpc://host:port#sql=SELECT ...&key=id&header=authorization:$TOKEN&name=trips`, with a
//! time window `time=COLUMN&window=10m` if wanted.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::RecordBatch;
use densa_flight::{FlightEvent, FlightOptions, FlightStream};

use crate::stream::{
    encode, header, header_value, option_text, options, parts, shortened, Arrived, Keying,
};
use crate::time::{TimeOptions, TimeSpec};

/// A failed read that may pass is tried again after this, then twice as long, up to
/// [`RETRY_MOST`].
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MOST: Duration = Duration::from_secs(30);
/// Characters of a query a tab shows, at most.
const TITLE_QUERY_CHARS: usize = 40;

/// What a Flight source asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Request {
    /// A Flight SQL query.
    Sql(String),
    /// A dataset by its path (`sales/2026`).
    Path(Vec<String>),
    /// The data behind a ticket.
    Ticket(String),
    /// The tables a Flight SQL service has.
    Tables,
}

/// How to read a Flight source: what its URL says. Its `Debug` output leaves header values
/// out.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct FlightSpec {
    /// The URL without the part after `#`.
    pub(crate) url: String,
    pub(crate) request: Request,
    /// The column whose rows replace each other; none keeps every row, numbered.
    pub(crate) key: Option<String>,
    /// Headers of every call, as given (`$NAME` values are read from the environment when
    /// it opens).
    pub(crate) headers: Vec<(String, String)>,
    /// The table name asked for.
    pub(crate) name: Option<String>,
    /// The column of the rows' times, and the window kept over it, if given.
    pub(crate) time: Option<TimeSpec>,
}

impl fmt::Debug for FlightSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("FlightSpec")
            .field("url", &self.url)
            .field("request", &self.request)
            .field("key", &self.key)
            .field("headers", &headers)
            .field("name", &self.name)
            .field("time", &self.time)
            .finish()
    }
}

/// Sets `slot` to `value` once: an option given twice is a mistake.
fn once(slot: &mut Option<String>, option: &str, value: String) -> Result<(), String> {
    match slot.replace(value) {
        None => Ok(()),
        Some(_) => Err(format!("{option}= is given twice")),
    }
}

impl FlightSpec {
    /// Whether `text` names a Flight service.
    pub(crate) fn is_flight(text: &str) -> bool {
        let lower = text.trim_start().to_ascii_lowercase();
        ["grpc://", "grpc+tls://", "grpc+tcp://"]
            .iter()
            .any(|scheme| lower.starts_with(scheme))
    }

    /// Reads a Flight URL: one of `sql=QUERY`, `path=a/b`, `ticket=T` or `tables`, then
    /// `key=COLUMN`,
    /// `header=NAME:VALUE` (any number of times), `name=TABLE` and a window
    /// (`time=COLUMN&window=10m`, `unit=s`), percent-encoded and separated by `&` after `#`.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let (url, options) = options(text)?;
        let mut spec = Self {
            url,
            // Until an option says what to ask for.
            request: Request::Ticket(String::new()),
            key: None,
            headers: Vec::new(),
            name: None,
            time: None,
        };
        let mut asked = None;
        let mut time = TimeOptions::default();
        for (option, value) in options {
            if time.take(&option, &value)? {
                continue;
            }
            let request = match (option.as_str(), value.is_empty()) {
                ("tables", true) => Some(Request::Tables),
                (_, true) => return Err(format!("{option} needs a value")),
                _ => spec.apply(&option, value)?,
            };
            if let Some(request) = request {
                if asked.replace(request).is_some() {
                    return Err(
                        "ask for one of sql=, path=, ticket= or tables, not more".to_owned()
                    );
                }
            }
        }
        spec.request = asked.ok_or(
            "a Flight URL asks for something after #: sql=QUERY, path=A/B, ticket=T or tables",
        )?;
        if matches!(&spec.request, Request::Path(parts) if parts.is_empty()) {
            return Err("path= names a dataset: path=A/B".to_owned());
        }
        spec.time = time.finish(spec.key.as_deref())?;
        Ok(spec)
    }

    /// Takes in the option `option=value`; what to ask for, if it says.
    fn apply(&mut self, option: &str, value: String) -> Result<Option<Request>, String> {
        let request = match option {
            "sql" => Request::Sql(value),
            "path" => Request::Path(
                value
                    .split('/')
                    .filter(|p| !p.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ),
            "ticket" => Request::Ticket(value),
            "key" => return once(&mut self.key, option, value).map(|()| None),
            "name" => return once(&mut self.name, option, value).map(|()| None),
            "header" => {
                self.headers.push(header(&value)?);
                return Ok(None);
            }
            _ => {
                return Err(format!(
                    "unknown Flight option {}: Studio knows sql=QUERY, path=A/B, ticket=T, \
                     key=COLUMN, header=NAME:VALUE, name=TABLE, {}",
                    option_text(option, &value),
                    TimeOptions::KNOWN
                ))
            }
        };
        Ok(Some(request))
    }

    /// The host and what is asked: what the tab says.
    pub(crate) fn title(&self) -> String {
        let (_, host, _) = parts(&self.url);
        match &self.request {
            Request::Sql(query) => {
                let mut chars = query.split_whitespace().collect::<Vec<_>>().join(" ");
                if chars.chars().count() > TITLE_QUERY_CHARS {
                    chars = chars.chars().take(TITLE_QUERY_CHARS).collect();
                    chars.push('…');
                }
                format!("{host}: {chars}")
            }
            Request::Path(path) => format!("{host}/{}", path.join("/")),
            Request::Ticket(_) => format!("{host}: a ticket"),
            Request::Tables => format!("{host}: tables"),
        }
    }

    /// What its table is named after: the name asked for, the last part of the path, or
    /// `flight`.
    pub(crate) fn name_hint(&self) -> String {
        match (&self.name, &self.request) {
            (Some(name), _) => name.clone(),
            (None, Request::Path(path)) => path.last().cloned().unwrap_or_default(),
            (None, Request::Tables) => "tables".to_owned(),
            (None, _) => "flight".to_owned(),
        }
    }

    /// What the source is known by, to find it open again and in a saved arrangement: its
    /// scheme and host in lower case, what is asked (a ticket only by a hash of it), its key,
    /// name and window, written as options are; not headers, which may hold secrets.
    pub(crate) fn origin(&self) -> String {
        let (scheme, host, _) = parts(&self.url);
        let asked = match &self.request {
            Request::Sql(query) => format!("sql={}", encode(query)),
            Request::Path(path) => format!("path={}", encode(&path.join("/"))),
            Request::Ticket(ticket) => format!("ticket#{:016x}", fnv(ticket.as_bytes())),
            Request::Tables => "tables".to_owned(),
        };
        let mut origin = format!(
            "{}://{}#{asked}",
            scheme.to_ascii_lowercase(),
            host.to_ascii_lowercase()
        );
        if let Some(key) = &self.key {
            origin.push_str(&format!("&key={}", encode(key)));
        }
        if let Some(name) = &self.name {
            origin.push_str(&format!("&name={}", encode(name)));
        }
        for option in self.time.iter().flat_map(TimeSpec::options) {
            origin.push('&');
            origin.push_str(&option);
        }
        origin
    }

    /// Starts reading, calling `wake` after each event; a read that may pass is tried
    /// again. A list of tables comes with their schemas if `schemas`.
    fn connect(
        &self,
        wake: impl Fn() + Send + 'static,
        schemas: bool,
    ) -> Result<FlightStream, String> {
        let url = self.url.clone();
        let mut options = match &self.request {
            Request::Sql(query) => FlightOptions::sql(url, query.clone()),
            Request::Path(path) => FlightOptions::path(url, path.clone()),
            Request::Ticket(ticket) => FlightOptions::ticket(url, ticket.as_bytes().to_vec()),
            Request::Tables if schemas => FlightOptions::sql_tables_with_schemas(url),
            Request::Tables => FlightOptions::sql_tables(url),
        };
        for (name, value) in &self.headers {
            options = options.header(name.clone(), header_value(value)?);
        }
        options
            .retry(RETRY_FIRST, RETRY_MOST)
            .connect(wake)
            .map_err(|e| e.to_string())
    }
}

impl FlightSpec {
    /// The table named by `catalog`, `schema` and `table` (as a list of tables gives them;
    /// null parts left out) at the same service, with the same headers: all its rows, its
    /// SQL table named after it.
    pub(crate) fn table(
        &self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: &str,
    ) -> FlightSpec {
        // Each part quoted, a quote within doubled: names with spaces, dots or quotes stay
        // whole.
        // An empty catalog or schema is none, as servers without them say.
        let quoted: Vec<String> = [catalog, schema, Some(table)]
            .into_iter()
            .flatten()
            .filter(|part| !part.is_empty())
            .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
            .collect();
        FlightSpec {
            url: self.url.clone(),
            request: Request::Sql(format!("SELECT * FROM {}", quoted.join("."))),
            key: None,
            headers: self.headers.clone(),
            name: Some(table.to_owned()),
            time: None,
        }
    }

    /// Whether it lists a service's tables.
    pub(crate) fn lists_tables(&self) -> bool {
        self.request == Request::Tables
    }
}

/// The 64-bit FNV-1a hash of `bytes`: a ticket named without being written down.
fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Asks for a list of tables again, by their names alone.
type NamesOnly = Box<dyn FnOnce() -> Result<FlightStream, String>>;

/// A Flight source being read: its rows readied for a table, and how it stands.
pub(crate) struct Fetch {
    stream: FlightStream,
    keying: Keying,
    state: String,
    /// Whether nothing more will come: the result ended, or reading it failed for good.
    done: bool,
    /// Whether a read failed and is being tried again.
    retried: bool,
    /// Whether the result has started (its columns came).
    started: bool,
    /// For a list of tables asked for with their schemas: asking for the names alone, if
    /// that fails before anything comes.
    names_only: Option<NamesOnly>,
    /// Why a list of tables came without their columns, if it did.
    without_columns: Option<String>,
}

impl Fetch {
    /// Reads `spec`'s data, calling `wake` after each event.
    pub(crate) fn connect(
        spec: &FlightSpec,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, String> {
        // Shared with the names-only list of tables that may follow.
        let wake = Arc::new(Mutex::new(wake));
        let waker = Arc::clone(&wake);
        let wake_now = move || {
            if let Ok(wake) = waker.lock() {
                wake();
            }
        };
        let names_only = spec.lists_tables().then(|| {
            let spec = spec.clone();
            let again: NamesOnly = Box::new(move || {
                let wake_now = move || {
                    if let Ok(wake) = wake.lock() {
                        wake();
                    }
                };
                spec.connect(wake_now, false)
            });
            again
        });
        Ok(Self {
            stream: spec.connect(wake_now, true)?,
            // A Flight result is finite: every row of it is kept.
            keying: Keying::new(spec.key.clone(), None),
            state: "asking".to_owned(),
            done: false,
            retried: false,
            started: false,
            names_only,
            without_columns: None,
        })
    }

    /// Why a list of tables came without their columns: the service refused to give them.
    pub(crate) fn without_columns(&self) -> Option<&str> {
        self.without_columns.as_deref()
    }

    /// The key column of the table.
    pub(crate) fn key(&self) -> &str {
        self.keying.key()
    }

    /// Whether nothing more will come.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    /// Whether a read failed and is being tried again (a service not there, say): its rows
    /// are not worth waiting for.
    pub(crate) fn has_retried(&self) -> bool {
        self.retried
    }

    /// How it stands, for the status bar.
    pub(crate) fn status(&self) -> &str {
        &self.state
    }

    /// The next columns or rows that arrived, taking in the events before them; `None`
    /// when there are none for now.
    pub(crate) fn next(&mut self) -> Option<Arrived> {
        loop {
            match self.stream.try_next()? {
                FlightEvent::Schema(schema) => {
                    // The result starts (over): a table of its columns, numbered from 0.
                    self.started = true;
                    self.keying.restart();
                    "receiving".clone_into(&mut self.state);
                    let empty = RecordBatch::new_empty(schema);
                    return Some(match self.keying.ready(&empty) {
                        Ok((columns, _)) => Arrived::Columns(columns),
                        Err(reason) => Arrived::Refused { rows: 0, reason },
                    });
                }
                FlightEvent::Batch(batch) => {
                    let rows = batch.num_rows();
                    return Some(match self.keying.ready(&batch) {
                        Ok((batch, deleted)) => Arrived::Rows { batch, deleted },
                        Err(reason) => Arrived::Refused { rows, reason },
                    });
                }
                FlightEvent::Finished => {
                    self.done = true;
                    "done".clone_into(&mut self.state);
                }
                FlightEvent::Retrying { reason, retry_in } => {
                    self.retried = true;
                    let seconds = retry_in.as_secs_f64();
                    let reason = shortened(&reason);
                    self.state = format!("asking again in {seconds:.1} s ({reason})");
                }
                FlightEvent::Failed { reason } => {
                    // A service may refuse to give its tables' columns: ask for the names.
                    let names_only = self.names_only.take().filter(|_| !self.started);
                    if let Some(Ok(stream)) = names_only.map(|again| again()) {
                        self.stream = stream;
                        self.without_columns = Some(shortened(&reason));
                        "asking for the names alone".clone_into(&mut self.state);
                        continue;
                    }
                    self.done = true;
                    self.state = format!("failed: {}", shortened(&reason));
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_flight_url_says_what_to_ask_for_after_the_hash() {
        let spec = FlightSpec::parse(
            " grpc+tls://Data.Example.com:443#sql=SELECT%20*%20FROM%20trips&key=id&header=authorization:%24TOKEN&name=trips ",
        )
        .unwrap();
        assert_eq!(spec.url, "grpc+tls://Data.Example.com:443");
        assert_eq!(spec.request, Request::Sql("SELECT * FROM trips".to_owned()));
        assert_eq!(spec.key.as_deref(), Some("id"));
        assert_eq!(
            spec.headers,
            [("authorization".to_owned(), "$TOKEN".to_owned())]
        );
        assert_eq!(spec.name_hint(), "trips");
        assert_eq!(spec.title(), "Data.Example.com:443: SELECT * FROM trips");
        assert_eq!(
            spec.origin(),
            "grpc+tls://data.example.com:443#sql=SELECT%20*%20FROM%20trips&key=id&name=trips"
        );
        let printed = format!("{spec:?}");
        assert!(!printed.contains("TOKEN"), "{printed}");
        let refused = FlightSpec::parse("grpc://me:pw@h#path=a").unwrap_err();
        assert!(refused.contains("no user and password"), "{refused}");
        let twice = FlightSpec::parse("grpc://h#path=a&name=x&name=y").unwrap_err();
        assert_eq!(twice, "name= is given twice");
        let ambiguous = FlightSpec::parse("grpc://h#sql=a%26key=b").unwrap();
        assert_ne!(
            ambiguous.origin(),
            FlightSpec::parse("grpc://h#sql=a&key=b").unwrap().origin()
        );

        let path = FlightSpec::parse("grpc://h:1#path=/sales//2026/").unwrap();
        assert_eq!(
            path.request,
            Request::Path(vec!["sales".into(), "2026".into()])
        );
        assert_eq!(
            (path.title().as_str(), path.name_hint().as_str()),
            ("h:1/sales/2026", "2026")
        );
        assert_eq!(path.origin(), "grpc://h:1#path=sales/2026");

        let ticket = FlightSpec::parse("grpc://h:1#ticket=secret").unwrap();
        assert_eq!(
            (ticket.title().as_str(), ticket.name_hint().as_str()),
            ("h:1: a ticket", "flight")
        );
        assert!(!ticket.origin().contains("secret"), "{}", ticket.origin());
        let other = FlightSpec::parse("grpc://h:1#ticket=other").unwrap();
        assert_ne!(ticket.origin(), other.origin());
    }

    #[test]
    fn a_time_column_is_part_of_what_a_flight_source_is_known_by() {
        let spec = FlightSpec::parse("grpc://h#path=a&time=t&window=1h").unwrap();
        let window = spec.time.as_ref().unwrap().window;
        assert_eq!(window, Some(Duration::from_secs(3_600)));
        assert_eq!(spec.origin(), "grpc://h#path=a&time=t&window=1h");
        assert!(format!("{spec:?}").contains("time: Some"));
        let error = FlightSpec::parse("grpc://h#path=a&window=1h").unwrap_err();
        assert!(error.contains("time=COLUMN"), "{error}");
        let error = FlightSpec::parse("grpc://h#path=a&key=t&time=t").unwrap_err();
        assert!(error.contains("a key cannot be a time"), "{error}");
        let error = FlightSpec::parse("grpc://h#path=a&time=").unwrap_err();
        assert_eq!(error, "time needs a value");
        let error = FlightSpec::parse("grpc://h#path=a&later=1").unwrap_err();
        assert!(
            error.ends_with("name=TABLE, time=COLUMN, unit=s, window=10m and per=1m"),
            "{error}"
        );
    }

    #[test]
    fn a_flight_url_can_ask_for_the_list_of_tables() {
        let spec = FlightSpec::parse("grpc://Host:1#tables&header=authorization:$T").unwrap();
        assert!(spec.lists_tables());
        assert_eq!(spec.title(), "Host:1: tables");
        assert_eq!(spec.name_hint(), "tables");
        assert_eq!(spec.origin(), "grpc://host:1#tables");
        let both = FlightSpec::parse("grpc://h#tables&sql=SELECT 1").unwrap_err();
        assert!(both.contains("not more"), "{both}");
        let valued = FlightSpec::parse("grpc://h#tables=x").unwrap_err();
        assert!(
            valued.contains("unknown Flight option tables=..."),
            "{valued}"
        );
        let nothing = FlightSpec::parse("grpc://h#key=a").unwrap_err();
        assert!(nothing.ends_with("ticket=T or tables"), "{nothing}");
        assert!(!FlightSpec::parse("grpc://h#sql=x").unwrap().lists_tables());
    }

    #[test]
    fn a_listed_table_is_asked_for_by_its_quoted_name() {
        let listing = FlightSpec::parse("grpc://h#tables&header=authorization:$T").unwrap();
        let full = listing.table(Some("demo"), Some("public"), "requests");
        assert_eq!(
            full.request,
            Request::Sql("SELECT * FROM \"demo\".\"public\".\"requests\"".to_owned())
        );
        assert_eq!(full.name.as_deref(), Some("requests"));
        assert_eq!(full.headers, listing.headers, "the same token");
        assert_eq!(full.url, listing.url);
        let bare = listing.table(None, None, "odd \"name\".x");
        assert_eq!(
            bare.request,
            Request::Sql("SELECT * FROM \"odd \"\"name\"\".x\"".to_owned())
        );
        let unnamed = listing.table(Some(""), Some(""), "t");
        assert_eq!(
            unnamed.request,
            Request::Sql("SELECT * FROM \"t\"".to_owned())
        );
        let schema_only = listing.table(None, Some("s"), "t");
        assert_eq!(
            schema_only.request,
            Request::Sql("SELECT * FROM \"s\".\"t\"".to_owned())
        );
    }

    #[test]
    fn a_query_as_long_as_a_tab_shows_is_not_cut() {
        let spec = FlightSpec::parse(&format!("grpc://h#sql={}", "x".repeat(40))).unwrap();
        assert_eq!(spec.title(), format!("h: {}", "x".repeat(40)));
        let upper = FlightSpec::parse("GRPC://H:1#path=a").unwrap();
        assert_eq!(
            upper.origin(),
            "grpc://h:1#path=a",
            "scheme and host in lower case"
        );
    }

    #[test]
    fn a_long_query_is_cut_short_in_its_tab() {
        let query = format!("sql={}", "x".repeat(50));
        let spec = FlightSpec::parse(&format!("grpc://h#{query}")).unwrap();
        assert_eq!(spec.title(), format!("h: {}…", "x".repeat(40)));
        let spaced = FlightSpec::parse("grpc://h#sql=SELECT%0A%20%201").unwrap();
        assert_eq!(spaced.title(), "h: SELECT 1");
    }

    #[test]
    fn what_cannot_be_read_is_named() {
        let error = |text| FlightSpec::parse(text).unwrap_err();
        assert!(error("grpc://h").contains("asks for something"));
        assert!(error("grpc://h#key=id").contains("asks for something"));
        assert!(error("grpc://h#sql=a&path=b").contains("not more"));
        assert!(error("grpc://h#sql=").contains("sql needs a value"));
        assert!(error("grpc://h#path=/").contains("names a dataset"));
        assert!(error("grpc://h#sql=a&header=x").contains("header=NAME:VALUE"));
        assert!(error("grpc://h#sql=a&header=x:%20").contains("header=NAME:VALUE"));
        let unknown = error("grpc://h#sql=a&token=secret");
        assert!(
            unknown.contains("unknown Flight option token=..."),
            "{unknown}"
        );
        assert!(!unknown.contains("secret"), "a value is never echoed");
    }

    #[test]
    fn flight_urls_are_told_by_their_scheme() {
        assert!(FlightSpec::is_flight("grpc://h"));
        assert!(FlightSpec::is_flight("  GRPC+TLS://h"));
        assert!(FlightSpec::is_flight("grpc+tcp://h"));
        assert!(!FlightSpec::is_flight("https://h"));
        assert!(!FlightSpec::is_flight("ws://h"));
    }

    #[test]
    fn only_a_list_of_tables_is_asked_again_for_less() {
        // Nothing listens on port 9: the streams only start trying.
        let query = FlightSpec::parse("grpc://127.0.0.1:9#sql=SELECT%201").unwrap();
        assert!(Fetch::connect(&query, || {}).unwrap().names_only.is_none());
        let tables = FlightSpec::parse("grpc://127.0.0.1:9#tables").unwrap();
        assert!(Fetch::connect(&tables, || {}).unwrap().names_only.is_some());
    }

    #[test]
    fn a_header_value_from_the_environment_must_be_set() {
        let spec = FlightSpec::parse(
            "grpc://127.0.0.1:9#sql=SELECT%201&header=authorization:$DENSA_STUDIO_TEST_UNSET",
        )
        .unwrap();
        let error = spec.connect(|| {}, true).unwrap_err();
        assert_eq!(error, "$DENSA_STUDIO_TEST_UNSET is not set");
    }

    #[test]
    fn tickets_hash_apart() {
        assert_ne!(fnv(b"a"), fnv(b"b"));
        assert_eq!(fnv(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(
            fnv(b"a"),
            0xaf63_dc4c_8601_ec8c,
            "FNV-1a's own value for \"a\""
        );
    }
}
