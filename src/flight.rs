//! Arrow Flight sources: a Flight service's data, opened from a `grpc://` or `grpc+tls://`
//! URL whose part after `#` says what to ask for and how to keep it:
//! `grpc://host:port#sql=SELECT ...&key=id&header=authorization:$TOKEN&name=trips`, with a
//! time window `time=COLUMN&window=10m` if wanted.

use std::fmt;
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

    /// Reads a Flight URL: one of `sql=QUERY`, `path=a/b` or `ticket=T`, then `key=COLUMN`,
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
            if value.is_empty() {
                return Err(format!("{option} needs a value"));
            }
            if let Some(request) = spec.apply(&option, value)? {
                if asked.replace(request).is_some() {
                    return Err("ask for one of sql=, path= or ticket=, not more".to_owned());
                }
            }
        }
        spec.request = asked
            .ok_or("a Flight URL asks for something after #: sql=QUERY, path=A/B or ticket=T")?;
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
                     key=COLUMN, header=NAME:VALUE, name=TABLE and {}",
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
        }
    }

    /// What its table is named after: the name asked for, the last part of the path, or
    /// `flight`.
    pub(crate) fn name_hint(&self) -> String {
        match (&self.name, &self.request) {
            (Some(name), _) => name.clone(),
            (None, Request::Path(path)) => path.last().cloned().unwrap_or_default(),
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
    /// again.
    fn connect(&self, wake: impl Fn() + Send + 'static) -> Result<FlightStream, String> {
        let url = self.url.clone();
        let mut options = match &self.request {
            Request::Sql(query) => FlightOptions::sql(url, query.clone()),
            Request::Path(path) => FlightOptions::path(url, path.clone()),
            Request::Ticket(ticket) => FlightOptions::ticket(url, ticket.as_bytes().to_vec()),
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

/// The 64-bit FNV-1a hash of `bytes`: a ticket named without being written down.
fn fnv(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A Flight source being read: its rows readied for a table, and how it stands.
pub(crate) struct Fetch {
    stream: FlightStream,
    keying: Keying,
    state: String,
    /// Whether nothing more will come: the result ended, or reading it failed for good.
    done: bool,
    /// Whether a read failed and is being tried again.
    retried: bool,
}

impl Fetch {
    /// Reads `spec`'s data, calling `wake` after each event.
    pub(crate) fn connect(
        spec: &FlightSpec,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Self, String> {
        Ok(Self {
            stream: spec.connect(wake)?,
            // A Flight result is finite: every row of it is kept.
            keying: Keying::new(spec.key.clone(), None),
            state: "asking".to_owned(),
            done: false,
            retried: false,
        })
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
            error.ends_with("and time=COLUMN, unit=s and window=10m"),
            "{error}"
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
    fn a_header_value_from_the_environment_must_be_set() {
        let spec = FlightSpec::parse(
            "grpc://127.0.0.1:9#sql=SELECT%201&header=authorization:$DENSA_STUDIO_TEST_UNSET",
        )
        .unwrap();
        let error = spec.connect(|| {}).unwrap_err();
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
