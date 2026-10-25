//! Joins of sample streams to a file and to each other, taken in frame by frame.

use std::time::Duration;

use densa_paragraph::TextSystem;
use densa_widgets::{DockArea, DockAt};

use super::*;
use crate::flight::FlightSpec;
use crate::source::{Grid, PAGED_BYTES};
use crate::stream::StreamSpec;

/// Where each sample sensor's site is: a file of the sites and their regions (not the
/// airfield).
const SITES: &str = "site,region,elevation\nnorth,upland,420\nsouth,coast,12\n\
                     east,coast,30\nwest,upland,380\nharbor,coast,2\n";

struct Open {
    ui: Ui,
    dock: densa_ui::WidgetId,
    sources: Vec<Source>,
    folder: std::path::PathBuf,
}

impl Open {
    fn new(test: &str) -> Self {
        let mut ui = Ui::new(TextSystem::bundled().unwrap());
        let dock = ui
            .add(ui.root(), DockArea::new(), DockArea::style())
            .unwrap();
        let folder =
            std::env::temp_dir().join(format!("densa-studio-join-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).unwrap();
        Self {
            ui,
            dock,
            sources: Vec::new(),
            folder,
        }
    }

    fn sites(&mut self) {
        self.file("sites", SITES);
    }

    /// A CSV file of `text`, open as `name`.
    fn file(&mut self, name: &str, text: &str) {
        let path = self.folder.join(format!("{name}.csv"));
        std::fs::write(&path, text).unwrap();
        let source = Source::open_file(
            &mut self.ui,
            self.dock,
            &path,
            name.into(),
            DockAt::First,
            || {},
            PAGED_BYTES,
        )
        .unwrap();
        self.sources.push(source);
    }

    fn flight(&mut self, url: &str) {
        let spec = FlightSpec::parse(url).unwrap();
        let name = spec.name_hint();
        let source =
            Source::open_flight(&mut self.ui, self.dock, &spec, name, DockAt::First, || {})
                .unwrap();
        self.sources.push(source);
    }

    fn stream(&mut self, options: &str) {
        let url = format!("{}#{options}", samples::readings_server().unwrap());
        let spec = StreamSpec::parse(&url).unwrap();
        let name = spec.name_hint();
        let source =
            Source::open_stream(&mut self.ui, self.dock, &spec, name, DockAt::First, || {})
                .unwrap();
        self.sources.push(source);
    }

    /// Frames, as Studio takes them, until `done` says so (or 10 s).
    fn until(&mut self, done: impl Fn(&Self) -> bool) {
        let until = Instant::now() + Duration::from_secs(10);
        while !done(self) && Instant::now() < until {
            self.frame();
        }
    }

    /// One frame: each source takes its rows, then the joins theirs.
    fn frame(&mut self) {
        for source in &mut self.sources {
            source.take(&mut self.ui, Instant::now() + Duration::from_millis(20));
        }
        link(&mut self.ui, &mut self.sources);
        std::thread::sleep(Duration::from_millis(5));
    }

    fn table(&self, at: usize) -> &LiveTable {
        let grid = self.ui.widget::<Grid>(self.sources[at].grid).unwrap();
        grid.model().table()
    }

    fn columns(&self, at: usize) -> Vec<String> {
        let schema = self.table(at).schema().clone();
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    /// The rows of source `at` as (site, `column`) pairs, sorted; none before it is joined.
    fn pairs(&self, at: usize, column: &str) -> Vec<(String, Option<String>)> {
        self.pairs_by(at, "site", column)
    }

    /// The rows of source `at` as (`by`, `column`) pairs, sorted; none before it is joined.
    fn pairs_by(&self, at: usize, by: &str, column: &str) -> Vec<(String, Option<String>)> {
        let mut pairs = Vec::new();
        for batch in self.table(at).snapshot().unwrap() {
            let site = as_text(batch.column_by_name(by).unwrap()).unwrap();
            let Some(values) = batch.column_by_name(column) else {
                continue;
            };
            let values = as_text(values).unwrap();
            for row in 0..batch.num_rows() {
                let value = values.is_valid(row).then(|| values.value(row).to_owned());
                pairs.push((site.value(row).to_owned(), value));
            }
        }
        pairs.sort();
        pairs
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.folder);
    }
}

fn region_of(site: &str) -> Option<&'static str> {
    match site {
        "north" | "west" => Some("upland"),
        "south" | "east" | "harbor" => Some("coast"),
        _ => None,
    }
}

#[test]
fn a_stream_shows_the_columns_of_the_file_row_each_reading_names() {
    let mut open = Open::new("file");
    open.sites();
    open.stream("key=sensor&join=site:sites");
    open.until(|open| open.pairs(1, "region").len() >= 400);
    assert_eq!(
        open.columns(1),
        [
            "sensor",
            "site",
            "celsius",
            "humidity",
            "ok",
            "time",
            "region",
            "elevation"
        ],
        "the file's row numbers are left out"
    );
    let regions = open.pairs(1, "region");
    assert!(regions.len() >= 400, "{}", open.sources[1].status(&open.ui));
    for (site, region) in &regions {
        assert_eq!(region.as_deref(), region_of(site), "{site}");
    }
    assert!(
        regions.iter().any(|(site, _)| site == "airfield"),
        "a site the file lacks"
    );
    let status = open.sources[1].status(&open.ui);
    assert!(status.contains("(joined with sites on site)"), "{status}");
    assert!(open.sources[0].watched && !open.sources[1].watched);
    let (schema, _) = open.sources[1].snapshot(&open.ui).unwrap();
    assert_eq!(schema.fields().len(), 8, "SQL sees the joined rows");
}

#[test]
fn a_stream_opened_first_is_joined_once_the_file_opens() {
    let mut open = Open::new("later");
    open.stream("key=sensor&join=site:sites");
    open.until(|open| open.sources[0].rows(&open.ui) >= 100);
    let status = open.sources[0].status(&open.ui);
    assert!(
        status.contains("join waiting: no table named sites is open"),
        "{status}"
    );
    open.sites();
    // Joined at once, to the file's rows as far as they have been read: the rest are
    // joined as they come.
    let right = |open: &Open| {
        let regions = open.pairs(0, "region");
        regions.len() >= 100
            && regions
                .iter()
                .all(|(site, region)| region.as_deref() == region_of(site))
    };
    open.until(right);
    let regions = open.pairs(0, "region");
    assert!(regions.len() >= 100, "{}", open.sources[0].status(&open.ui));
    for (site, region) in &regions {
        assert_eq!(region.as_deref(), region_of(site), "{site}");
    }
}

#[test]
fn a_stream_joined_to_another_stream_follows_its_changes() {
    let mut open = Open::new("streams");
    // Each site's latest reading; its columns are named apart from the readings' own.
    open.stream("key=site&name=latest");
    open.stream("key=sensor&join=site:latest");
    open.until(|open| open.pairs(1, "latest_time").len() >= 400);
    let first = open.pairs(1, "latest_time");
    assert!(first.len() >= 400, "{}", open.sources[1].status(&open.ui));
    open.until(|open| open.pairs(1, "latest_time") != first);
    assert_ne!(
        open.pairs(1, "latest_time"),
        first,
        "the latest readings came"
    );
    assert_eq!(
        open.columns(1)[6..],
        [
            "latest_sensor",
            "latest_celsius",
            "latest_humidity",
            "latest_ok",
            "latest_time"
        ]
    );
}

#[test]
fn a_stream_cannot_be_joined_to_itself() {
    let mut open = Open::new("itself");
    open.stream("key=sensor&name=readings&join=site:readings");
    open.until(|open| open.sources[0].rows(&open.ui) >= 50);
    let status = open.sources[0].status(&open.ui);
    assert!(
        status.contains("join off: a table cannot be joined to itself"),
        "{status}"
    );
    assert_eq!(open.columns(0).len(), 6);
}

/// The sample requests' services and the teams that run them.
const SERVICES: &str = "service,team\ngateway,edge\nauth,identity\ncatalog,shop\n\
                        checkout,shop\nsearch,shop\nstorage,platform\n";

fn team_of(service: &str) -> Option<&'static str> {
    match service {
        "gateway" => Some("edge"),
        "auth" => Some("identity"),
        "catalog" | "checkout" | "search" => Some("shop"),
        "storage" => Some("platform"),
        _ => None,
    }
}

#[test]
fn a_flight_result_read_again_is_joined_again() {
    let mut open = Open::new("flight");
    open.file("services", SERVICES);
    let url = samples::flight_server_failing_once(4_000).unwrap();
    // Its second part fails once: the result is read again, into a table made anew.
    open.flight(&format!("{url}#path=requests&join=service:services"));
    let joined = |open: &Open| {
        let teams = open.pairs_by(1, "service", "team");
        open.sources[1].is_done()
            && teams.len() == 4_000
            && teams
                .iter()
                .all(|(service, team)| team.as_deref() == team_of(service))
    };
    open.until(joined);
    let status = open.sources[1].status(&open.ui);
    assert!(joined(&open), "{status}");
    assert!(
        !status.contains("refused") && !status.contains("skipped"),
        "{status}"
    );
}

#[test]
fn rows_of_the_other_table_changing_change_a_joined_source_that_is_done() {
    let mut open = Open::new("changed");
    open.stream("key=site&name=latest");
    let url = samples::flight_server_of(100).unwrap();
    open.flight(&format!("{url}#path=requests&join=service:latest.site"));
    open.until(|open| {
        open.sources[1].is_done() && open.columns(1).contains(&"latest_time".to_owned())
    });
    let status = open.sources[1].status(&open.ui);
    assert!(open.sources[1].is_done(), "{status}");
    // Its time is named apart from the requests' own; the sensor is not.
    let columns = open.columns(1);
    assert!(columns.contains(&"sensor".to_owned()), "{columns:?}");
    assert!(columns.contains(&"latest_time".to_owned()), "{status}");
    open.sources[1].changed = false;
    open.until(|open| open.sources[1].changed);
    assert!(
        open.sources[1].changed,
        "the latest readings came: SQL should see them"
    );
}

#[test]
fn rows_leaving_or_replaced_are_counted_for_the_sources_joined() {
    let mut open = Open::new("counted");
    open.stream("time=time&window=200ms&name=recent");
    open.stream("key=sensor&name=latest");
    let until = Instant::now() + Duration::from_secs(10);
    let (mut deleted, mut replaced) = (0, 0);
    while (deleted == 0 || replaced == 0) && Instant::now() < until {
        open.frame();
        deleted = open.sources[0].offer(&open.ui).deletions;
        replaced = open.sources[1].offer(&open.ui).updates;
    }
    assert!(deleted > 0, "rows left the window");
    assert!(replaced > 0, "sensors read again");
    assert_eq!(open.sources[1].offer(&open.ui).deletions, 0);
}
