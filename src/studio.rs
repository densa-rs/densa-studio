//! The application: a toolbar to open files, a docking workspace of sources and SQL, and a
//! status bar.

use std::error::Error;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use densa_datafusion::Engine;
use densa_ui::{Action, ActionKind, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{DockArea, DockAt, DockLayout, DockSide, Label, Panel, TextInput};
use densa_window::{App, FileDrop, Host, Waker};

use crate::config;
use crate::flight::FlightSpec;
use crate::query::Query;
use crate::source::{sql_name, table_name, Source};
use crate::stream::{parts, StreamSpec};

/// How often the status bar is written again while something loads.
const STATUS_EVERY: Duration = Duration::from_millis(200);
/// The longest a frame spends taking in rows: the rest wait for the next frame.
const FRAME_BUDGET: Duration = Duration::from_millis(6);
/// The part of it a query's result gets first, so it shows while files still load.
const QUERY_BUDGET: Duration = Duration::from_millis(2);
/// What the welcome tab says.
const WELCOME: &str = "Open a CSV or Parquet file, a ws:// stream or a grpc:// Flight \
                       service: type its path or URL in the box above and press Enter, drop \
                       files on the window, or start Studio with them. Each opens in a tab of \
                       its own that fills as rows come, and the SQL panel queries every one \
                       by its name. After a URL's #, key=COLUMN keeps the latest row of each \
                       key, time=COLUMN shows its times as times, join=COLUMN:TABLE adds the \
                       columns of the row it names in another open table, and tables lists \
                       a Flight SQL server's tables.";

/// Files named in the status bar while held over the window, at most.
const HOVER_NAMES: usize = 3;

/// `names` as the status bar lists them: the first few, then how many more.
fn listed(names: &[String]) -> String {
    match names.len() {
        0..=HOVER_NAMES => names.join(", "),
        n => format!(
            "{} and {} more",
            names[..HOVER_NAMES].join(", "),
            n - HOVER_NAMES
        ),
    }
}

/// What Studio starts with.
pub(crate) struct Start {
    /// Files and stream URLs.
    pub(crate) sources: Vec<String>,
    pub(crate) query: Option<String>,
    /// Run the query again each second while sources change.
    pub(crate) live: bool,
    pub(crate) light: bool,
    /// Ignore the arrangement saved last time.
    pub(crate) fresh: bool,
    pub(crate) close_at: Option<Instant>,
}

pub(crate) struct Studio {
    start: Start,
    engine: Engine,
    waker: Option<Waker>,
    ids: Option<Ids>,
    sources: Vec<Source>,
    query: Option<Query>,
    /// The welcome tab, while no file is open.
    welcome: Option<WidgetId>,
    /// Where the arrangement is saved; `None` once a saved one could not be read, so it is
    /// not overwritten.
    layout_file: Option<PathBuf>,
    /// The last thing to tell the user.
    note: String,
    /// Names of the files held over the window, said before anything else until they are
    /// dropped or leave.
    hovering: Vec<String>,
    /// What went wrong, until the user does something else: shown before any note.
    problems: Vec<String>,
    /// The first query runs once the files have loaded.
    query_pending: bool,
    /// The source taking rows in first next frame: they take turns.
    first_source: usize,
    status_at: Instant,
    /// When a thread will wake the window for a live query.
    wake_at: Option<Instant>,
}

#[derive(Clone, Copy)]
struct Ids {
    dock: WidgetId,
    open: WidgetId,
    status: WidgetId,
}

impl Studio {
    pub(crate) fn new(start: Start) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            query_pending: false,
            first_source: 0,
            start,
            engine: Engine::new()?,
            waker: None,
            ids: None,
            sources: Vec::new(),
            query: None,
            welcome: None,
            layout_file: config::layout_file(),
            note: String::new(),
            hovering: Vec::new(),
            problems: Vec::new(),
            status_at: Instant::now(),
            wake_at: None,
        })
    }

    /// Something that went wrong, shown until the user does something else (once, however
    /// often it happens).
    fn problem(&mut self, text: String) {
        if !self.problems.contains(&text) {
            self.problems.push(text);
        }
    }

    /// Opens `text`, a file's path or a stream's or Flight service's URL, as a new source in
    /// a tab beside the other sources; one already open has its tab shown instead.
    fn open(&mut self, ui: &mut Ui, text: &str) {
        match Opening::read(text) {
            Ok(what) => self.open_what(ui, what),
            Err(error) => self.problem(format!("{} not opened: {error}", shown(text))),
        }
    }

    /// Opens `what` as [`open`](Self::open) does.
    fn open_what(&mut self, ui: &mut Ui, what: Opening) {
        let Some(ids) = self.ids else {
            return;
        };
        // Without one (in tests), nothing is woken: frames come anyway.
        let waker = self.waker.clone();
        let origin = what.origin();
        if let Some(open) = self
            .sources
            .iter()
            .find(|s| Some(&s.origin) == origin.as_ref())
        {
            let _ = DockArea::show_panel(ui, ids.dock, open.panel);
            self.note = format!("Already open as {}", open.name);
            return;
        }
        let taken: Vec<String> = self.sources.iter().map(|s| s.name.clone()).collect();
        let at = match (self.sources.first(), self.welcome, &self.query) {
            (Some(first), _, _) => DockAt::With(first.panel),
            (None, Some(welcome), _) => DockAt::With(welcome),
            (None, None, Some(query)) => DockAt::Beside(query.panel, DockSide::Top),
            (None, None, None) => DockAt::First,
        };
        let wake = move || {
            if let Some(waker) = &waker {
                waker.wake();
            }
        };
        match what.open(ui, ids.dock, &taken, at, wake) {
            Ok(source) => {
                self.note = format!("Opened {} as {}", what.title(), source.name);
                let _ = DockArea::show_panel(ui, ids.dock, source.panel);
                self.sources.push(source);
                if let Some(welcome) = self.welcome.take() {
                    let _ = DockArea::remove_panel(ui, ids.dock, welcome);
                }
            }
            Err(error) => self.problem(format!("{} not opened: {error}", what.shown())),
        }
    }

    /// Arranges the dock as it was saved last time, unless starting fresh.
    fn restore(&mut self, ui: &mut Ui) {
        let Some(file) = self.layout_file.clone() else {
            return;
        };
        if self.start.fresh {
            return;
        }
        let restored = match std::fs::read_to_string(&file) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => Err(error.to_string()),
            Ok(text) => text
                .parse::<DockLayout>()
                .map_err(|e| e.to_string())
                .and_then(|layout| {
                    let dock = self.ids.ok_or("no dock")?.dock;
                    DockArea::restore_layout(ui, dock, &layout).map_err(|e| e.to_string())
                }),
        };
        if let Err(error) = restored {
            // Left as it is, for whoever wants to look at it.
            self.layout_file = None;
            self.problem(format!("The saved arrangement was not restored ({error})"));
        }
    }

    /// Saves the dock's arrangement for next time.
    fn save(&mut self, ui: &Ui) {
        let (Some(file), Some(ids)) = (&self.layout_file, self.ids) else {
            return;
        };
        let layout = ui.widget::<DockArea>(ids.dock).and_then(DockArea::layout);
        let saved = layout.map_or(Ok(()), |layout| config::save(file, &layout.to_string()));
        if let Err(error) = saved {
            self.problem(format!("The arrangement was not saved: {error}"));
        }
    }

    /// Runs the query in the SQL panel; `again` runs the last one again, updating its result
    /// in place.
    fn run_query(&mut self, ui: &mut Ui, again: bool) {
        let (Some(query), Some(waker)) = (self.query.as_mut(), self.waker.clone()) else {
            return;
        };
        let wake = move || {
            waker.wake();
        };
        query.run(ui, &self.engine, &mut self.sources, wake, again);
        query.show_status(ui);
    }

    /// Runs a live query again once it is due, or has the window woken then.
    fn live_query(&mut self, ui: &mut Ui) {
        let changed = self.sources.iter().any(|s| s.changed);
        let Some(at) = self.query.as_ref().and_then(|q| q.due_at(changed)) else {
            return;
        };
        if Instant::now() >= at {
            self.run_query(ui, true);
            return;
        }
        // No frames meanwhile: a thread wakes the window when it is due, once per wait.
        if self.wake_at != Some(at) {
            if let Some(waker) = self.waker.clone() {
                self.wake_at = Some(at);
                std::thread::spawn(move || {
                    std::thread::sleep(at.saturating_duration_since(Instant::now()));
                    waker.wake();
                });
            }
        }
    }

    fn show_status(&self, ui: &mut Ui) {
        let Some(ids) = self.ids else {
            return;
        };
        // Files still loading, and every stream: how they stand is news.
        let loading: Vec<String> = self
            .sources
            .iter()
            .filter(|s| !s.is_done())
            .map(|s| s.status(ui))
            .collect();
        let text = if !self.hovering.is_empty() {
            format!("Drop to open {}", listed(&self.hovering))
        } else if !self.problems.is_empty() {
            self.problems.join("   ")
        } else if !loading.is_empty() {
            loading.join("   ")
        } else if !self.note.is_empty() {
            self.note.clone()
        } else {
            let names: Vec<&str> = self.sources.iter().map(|s| s.name.as_str()).collect();
            match names.len() {
                0 => "No files open".to_owned(),
                _ => format!("Tables: {}", names.join(", ")),
            }
        };
        ui.update::<Label, _>(ids.status, |label, cx| label.set_text(cx, text));
    }
}

impl Studio {
    /// Fills the window: the toolbar, the dock with the start's sources and query, the
    /// status bar; then the arrangement saved last time, unless starting fresh.
    fn lay_out(&mut self, ui: &mut Ui) -> Result<(), Box<dyn Error>> {
        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        ui.add(
            toolbar,
            Label::new("densa studio").with_size(15.0),
            Style::row(),
        )?;
        let open = TextInput::new("").with_placeholder(
            "Open a CSV or Parquet file, a ws:// stream or a grpc:// Flight service, then Enter",
        );
        let open = ui.add(toolbar, open, Style::row().grow(1.0))?;
        let dock = ui.add(root, DockArea::new(), DockArea::style())?;
        let line = ui.add(root, Panel::new(), Style::row().padding(6.0).shrink(0.0))?;
        let status = ui.add(line, Label::new("").dim(), Style::row())?;
        self.ids = Some(Ids { dock, open, status });

        let welcome = DockArea::add_panel(ui, dock, Panel::new(), "Welcome", DockAt::First)?;
        let body = Style::column().grow(1.0).padding(16.0).gap(10.0);
        let body = ui.add(welcome, Panel::transparent(), body)?;
        ui.add(
            body,
            Label::new("densa studio").with_size(20.0),
            Style::row(),
        )?;
        ui.add(body, Label::new(WELCOME).wrapping().dim(), Style::row())?;
        self.welcome = Some(welcome);

        let query = self.start.query.clone().unwrap_or_default();
        let below = DockAt::Beside(welcome, DockSide::Bottom);
        let panel = Query::add(ui, dock, below, &query, self.start.live)?;
        let input = panel.input;
        self.query = Some(panel);
        for source in self.start.sources.clone() {
            self.open(ui, &source);
        }
        // Without a query of the user's, a look at the first file that opened.
        if self.start.query.is_none() {
            if let Some(first) = self.sources.first() {
                let query = format!("SELECT * FROM {} LIMIT 1000", first.name);
                ui.update::<TextInput, _>(input, |input, cx| input.set_text(cx, query));
            }
        }
        self.query_pending = self.start.query.is_some() || !self.sources.is_empty();
        self.restore(ui);
        ui.set_focus(Some(open));
        self.show_status(ui);
        Ok(())
    }

    /// Takes in a file dragged over the window or dropped on it.
    fn dropped(&mut self, ui: &mut Ui, drop: FileDrop) {
        match drop {
            FileDrop::Hovering(path) => {
                let name = path.file_name().unwrap_or(path.as_os_str());
                self.hovering.push(name.to_string_lossy().into_owned());
            }
            FileDrop::Left => self.hovering.clear(),
            FileDrop::Dropped(path) => {
                // The first file of a drop starts afresh (the files dropped with it come
                // after, the hover list empty by then): what went wrong before is old news.
                if !self.hovering.is_empty() {
                    self.hovering.clear();
                    self.problems.clear();
                }
                // A dropped file is opened as one typed in the box is.
                self.open_what(ui, Opening::File(path));
                self.save(ui);
            }
            _ => {}
        }
        self.show_status(ui);
    }
}

impl App for Studio {
    fn file_drop(&mut self, ui: &mut Ui, _: &mut Host<'_>, drop: FileDrop) {
        self.dropped(ui, drop);
    }

    fn build(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> Result<(), Box<dyn Error>> {
        if self.start.light {
            ui.set_theme(Theme::light());
        }
        self.waker = Some(host.waker());
        self.lay_out(ui)
    }

    fn action(&mut self, ui: &mut Ui, _: &mut Host<'_>, action: Action) {
        let Some(ids) = self.ids else {
            return;
        };
        if let Some(query) = &mut self.query {
            let waker = self.waker.clone();
            query.action(ui, &action, move || {
                if let Some(waker) = &waker {
                    waker.wake();
                }
            });
        }
        let submitted = action.kind == ActionKind::Submitted;
        if submitted {
            // The user moved on: what went wrong before is old news.
            self.problems.clear();
        }
        if submitted && action.source == ids.open {
            let text = ui
                .widget::<TextInput>(ids.open)
                .map(|input| input.text().trim().trim_matches('"').to_owned())
                .unwrap_or_default();
            if !text.is_empty() {
                self.open(ui, &text);
                let _ = ui.update::<TextInput, _>(ids.open, |input, cx| input.set_text(cx, ""));
                self.save(ui);
            }
        } else if submitted
            && self
                .query
                .as_ref()
                .is_some_and(|q| action.source == q.input)
        {
            self.query_pending = false;
            self.run_query(ui, false);
        } else if let Some(table) = self
            .sources
            .iter()
            .filter(|_| submitted)
            .find(|s| s.grid == action.source)
            .and_then(|s| s.selected_table(ui))
        {
            // A table chosen from a list of a service's tables: open it.
            self.open_what(ui, Opening::Flight(table));
            self.save(ui);
        } else if action.source == ids.dock
            && matches!(action.kind, ActionKind::Changed | ActionKind::Selected(_))
        {
            self.save(ui);
        }
        self.show_status(ui);
    }

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        let started = Instant::now();
        let mut more = false;
        // The query's result first, so it shows while files load; then the files, taking
        // turns at going first, each with a share of what is left.
        if let Some(query) = &mut self.query {
            more |= query.take(ui, started + QUERY_BUDGET);
        }
        let count = self.sources.len();
        let end = started + FRAME_BUDGET;
        for turn in 0..count {
            let index = (self.first_source + turn) % count;
            let now = Instant::now();
            let share =
                end.saturating_duration_since(now) / u32::try_from(count - turn).unwrap_or(1);
            more |= self.sources[index].take(ui, now + share);
        }
        if count > 0 {
            self.first_source = (self.first_source + 1) % count;
        }
        // Rows taken in above reach the sources joined to their tables.
        crate::join::link(ui, &mut self.sources);
        // Streams never finish, and a service not there may never answer: the first query
        // waits for files and Flight results being read.
        let loading = self.sources.iter().any(Source::blocks_query);
        if self.query_pending && !loading {
            self.query_pending = false;
            self.run_query(ui, false);
        }
        self.live_query(ui);
        let running = self.query.as_ref().is_some_and(Query::is_running);
        let streaming = self.sources.iter().any(Source::is_live);
        let busy = more || loading || running || streaming;
        if self.status_at.elapsed() >= STATUS_EVERY || !busy {
            self.status_at = Instant::now();
            self.show_status(ui);
            if let Some(query) = &self.query {
                query.show_status(ui);
            }
        }
        if self.start.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        // Loaders and queries wake the window as rows come; rows left over need another frame.
        more || self.start.close_at.is_some()
    }
}

/// What a source is opened from.
enum Opening {
    File(PathBuf),
    Stream(StreamSpec),
    Flight(FlightSpec),
}

impl Opening {
    /// What `text` names: a stream's or Flight service's URL, or else a file's path.
    fn read(text: &str) -> Result<Self, String> {
        if StreamSpec::is_stream(text) {
            return StreamSpec::parse(text).map(Self::Stream);
        }
        if FlightSpec::is_flight(text) {
            return FlightSpec::parse(text).map(Self::Flight);
        }
        Ok(Self::File(PathBuf::from(text)))
    }

    /// What it is known by when open; `None` for a file that is not there.
    fn origin(&self) -> Option<String> {
        match self {
            Self::File(path) => std::fs::canonicalize(path)
                .ok()
                .map(|p| p.display().to_string()),
            Self::Stream(spec) => Some(spec.origin()),
            Self::Flight(spec) => Some(spec.origin()),
        }
    }

    /// What a message about it says: a file's path as given, a remote source's title (its
    /// host and what is asked, never a header).
    fn shown(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Stream(spec) => spec.title(),
            Self::Flight(spec) => spec.title(),
        }
    }

    /// What it is called in a note.
    fn title(&self) -> String {
        match self {
            Self::File(path) => path
                .file_name()
                .unwrap_or(path.as_os_str())
                .to_string_lossy()
                .into_owned(),
            Self::Stream(spec) => spec.title(),
            Self::Flight(spec) => spec.title(),
        }
    }

    /// Opens it in a tab of `dock` placed `at`, named apart from `taken`.
    fn open(
        &self,
        ui: &mut Ui,
        dock: WidgetId,
        taken: &[String],
        at: DockAt,
        wake: impl Fn() + Send + 'static,
    ) -> Result<Source, Box<dyn Error>> {
        match self {
            Self::File(path) => {
                let name = table_name(path, taken);
                Source::open_file(ui, dock, path, name, at, wake)
            }
            Self::Stream(spec) => {
                let name = sql_name(&spec.name_hint(), taken);
                Source::open_stream(ui, dock, spec, name, at, wake)
            }
            Self::Flight(spec) => {
                let name = sql_name(&spec.name_hint(), taken);
                Source::open_flight(ui, dock, spec, name, at, wake)
            }
        }
    }
}

/// `text` as a message may show it: a URL by its scheme, host and path alone (what follows
/// `#` or `?`, and a user and password, may hold secrets), a path as it is.
fn shown(text: &str) -> String {
    if !StreamSpec::is_stream(text) && !FlightSpec::is_flight(text) {
        return text.to_owned();
    }
    let (scheme, host, path) = parts(text.trim());
    match path.is_empty() {
        true => format!("{scheme}://{host}"),
        false => format!("{scheme}://{host}/{path}"),
    }
}

#[cfg(test)]
mod tests {
    use densa_paragraph::TextSystem;

    use super::*;

    /// A Studio laid out with no window, keeping no arrangement.
    fn studio() -> (Ui, Studio) {
        let start = Start {
            sources: Vec::new(),
            query: None,
            live: false,
            light: false,
            fresh: true,
            close_at: None,
        };
        let mut studio = Studio::new(start).unwrap();
        studio.layout_file = None;
        let mut ui = Ui::new(TextSystem::bundled().unwrap());
        ui.resize(densa_draw::Size::new(1_000.0, 700.0));
        studio.lay_out(&mut ui).unwrap();
        (ui, studio)
    }

    #[test]
    fn many_files_held_are_counted_past_the_first_few() {
        let names = |n: usize| (0..n).map(|i| format!("f{i}")).collect::<Vec<_>>();
        assert_eq!(listed(&names(1)), "f0");
        assert_eq!(listed(&names(3)), "f0, f1, f2");
        assert_eq!(listed(&names(5)), "f0, f1, f2 and 2 more");
    }

    fn status(ui: &Ui, studio: &Studio) -> String {
        let status = studio.ids.unwrap().status;
        ui.widget::<Label>(status).unwrap().text().to_owned()
    }

    #[test]
    fn files_held_over_the_window_are_named_and_a_dropped_one_opens() {
        let folder = std::env::temp_dir().join(format!("densa-studio-drop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).unwrap();
        let (csv, text) = (folder.join("dropped.csv"), folder.join("notes.txt"));
        std::fs::write(
            &csv, "a,b
1,2
",
        )
        .unwrap();
        std::fs::write(&text, "hello").unwrap();
        let (mut ui, mut studio) = studio();

        studio.dropped(&mut ui, FileDrop::Hovering(csv.clone()));
        studio.dropped(&mut ui, FileDrop::Hovering(text.clone()));
        assert_eq!(status(&ui, &studio), "Drop to open dropped.csv, notes.txt");
        studio.dropped(&mut ui, FileDrop::Left);
        assert!(
            !status(&ui, &studio).starts_with("Drop"),
            "{}",
            status(&ui, &studio)
        );

        studio.dropped(&mut ui, FileDrop::Hovering(csv.clone()));
        studio.dropped(&mut ui, FileDrop::Dropped(csv));
        assert!(
            !status(&ui, &studio).starts_with("Drop"),
            "dropped: no longer held"
        );
        assert_eq!(studio.sources.len(), 1);
        assert_eq!(studio.sources[0].name, "dropped");
        assert!(studio.welcome.is_none(), "the welcome tab gives way");

        studio.dropped(&mut ui, FileDrop::Dropped(text.clone()));
        assert_eq!(studio.sources.len(), 1);
        let said = status(&ui, &studio);
        assert!(said.contains("notes.txt not opened"), "{said}");

        // Two files dropped at once: the first one's trouble stays when the second opens.
        let other = folder.join("other.csv");
        std::fs::write(&other, "c\n3\n").unwrap();
        studio.problems = vec!["from before the drag".to_owned()];
        for file in [&text, &other] {
            studio.dropped(&mut ui, FileDrop::Hovering(file.to_path_buf()));
        }
        studio.dropped(&mut ui, FileDrop::Dropped(text));
        studio.dropped(&mut ui, FileDrop::Dropped(other));
        assert_eq!(studio.sources.len(), 2);
        assert_eq!(studio.problems.len(), 1, "{:?}", studio.problems);
        assert!(studio.problems[0].contains("notes.txt not opened"));
        let _ = std::fs::remove_dir_all(&folder);
    }

    #[test]
    fn what_is_opened_is_told_by_its_scheme() {
        assert!(matches!(Opening::read("data/a.csv"), Ok(Opening::File(_))));
        assert!(matches!(Opening::read("ws://h/p"), Ok(Opening::Stream(_))));
        assert!(matches!(
            Opening::read("grpc://h#path=a"),
            Ok(Opening::Flight(_))
        ));
        assert!(
            Opening::read("grpc://h").is_err(),
            "a Flight URL must ask for something"
        );
        let file = Opening::read("missing/nowhere.csv").unwrap();
        assert_eq!(file.origin(), None, "a file that is not there");
        assert_eq!(file.title(), "nowhere.csv");
        let flight = Opening::read("grpc://h#path=a/b").unwrap();
        assert_eq!(flight.title(), "h/a/b");
    }

    #[test]
    fn messages_show_a_url_without_what_may_be_secret() {
        let shown = shown("grpc+tls://me:pw@h:443/x?token=t#sql=a&header=authorization:secret");
        assert_eq!(shown, "grpc+tls://h:443/x");
        assert_eq!(super::shown(" ws://h#key=a"), "ws://h");
        assert_eq!(super::shown("data/a b.csv"), "data/a b.csv");
    }
}
