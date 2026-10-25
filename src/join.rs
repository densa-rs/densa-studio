//! Lookup joins: a remote source's rows shown with the columns of the row of another open
//! table that each one names, `join=COLUMN:TABLE` after `#` (the table's column of the same
//! name) or `join=COLUMN:TABLE.KEY`, kept as either side changes. The joined rows are the
//! source's table: its grid, its window, its counts and SQL all see them.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_data::{Changes, DataError, LiveTable, LookupJoin};
use densa_ui::Ui;

use crate::source::Source;
use crate::stream::encode;

/// How often, at most, the joined rows are checked for rows that left the other table:
/// the check reads all of it.
const PRUNE_EVERY: Duration = Duration::from_secs(1);
/// What a join waits for first.
const OWN_ROWS: &str = "waiting for this source's rows";

/// What `join=` says: which column names rows of which table, by which of its columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JoinSpec {
    /// The source's column naming the other table's rows.
    pub(crate) column: String,
    /// The other table, by the name SQL knows it by.
    pub(crate) table: String,
    /// Its column the names are looked up in.
    pub(crate) key: String,
}

impl JoinSpec {
    /// The option, for messages about unknown ones.
    pub(crate) const KNOWN: &'static str = "join=COLUMN:TABLE";

    /// Reads `COLUMN:TABLE` or `COLUMN:TABLE.KEY` (table names have no dots).
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        let usage = || {
            "join= names a column and the open table its values name rows of: \
             join=COLUMN:TABLE, or join=COLUMN:TABLE.KEY when that table's column has \
             another name"
                .to_owned()
        };
        let (column, table) = value.split_once(':').ok_or_else(usage)?;
        let (table, key) = table.split_once('.').unwrap_or((table, column));
        if column.is_empty() || table.is_empty() || key.is_empty() {
            return Err(usage());
        }
        Ok(Self {
            column: column.to_owned(),
            table: table.to_owned(),
            key: key.to_owned(),
        })
    }

    /// The option giving this join, for a source's origin.
    pub(crate) fn option(&self) -> String {
        format!(
            "join={}:{}.{}",
            encode(&self.column),
            encode(&self.table),
            encode(&self.key)
        )
    }
}

/// What a source offers the sources joined to it, at each frame.
pub(crate) struct Offer {
    /// Its table's instance, once its table has its columns.
    pub(crate) instance: Option<u64>,
    pub(crate) schema: SchemaRef,
    /// The rows its table took in since the last frame.
    pub(crate) arrived: Vec<RecordBatch>,
    /// How many times rows left its table, and were replaced in it.
    pub(crate) deletions: u64,
    pub(crate) updates: u64,
    /// Its key column, and whether Studio numbers its rows in it (then left out of joins).
    pub(crate) key: String,
    pub(crate) numbered: bool,
    /// Whether it is joined itself.
    pub(crate) joined: bool,
    /// Whether it is a file read a page at a time: its rows are not held, so not joined.
    pub(crate) paged: bool,
}

/// A source's join to another open table.
pub(crate) struct Joined {
    pub(crate) spec: JoinSpec,
    state: State,
    /// What went wrong with the other table's last rows, if anything.
    trouble: Option<String>,
}

enum State {
    /// Not joined yet, and why.
    Waiting(String),
    /// Cannot be joined, and why; tried again when the other table (of this instance, if
    /// it was there) or the source's own starts over.
    Off {
        why: String,
        reference: Option<u64>,
    },
    On(Box<Linked>),
}

/// A join made.
struct Linked {
    join: LookupJoin,
    /// The source's own columns, first in the joined table.
    left: SchemaRef,
    /// The instance of the joined table.
    output: u64,
    /// The instance of the other table it was made from.
    reference: u64,
    /// The other table's columns the join takes, their names in the join, and where its
    /// key is among them.
    columns: Vec<usize>,
    schema: SchemaRef,
    key_at: usize,
    /// The other table's removals seen when it was last checked for rows that left, when
    /// that was, and how long until the next check.
    deletions: u64,
    updates: u64,
    pruned_at: Instant,
    prune_every: Duration,
}

/// What a [`Joined::step`] did to the source's table.
pub(crate) enum Stepped {
    Nothing,
    /// Rows of it changed.
    Changed,
    /// It is to be replaced by this one: the join made (anew), or the source's own columns
    /// again when a join made could not be made anew.
    Replaced(Box<LiveTable>),
}

impl Joined {
    pub(crate) fn new(spec: JoinSpec) -> Self {
        Self {
            spec,
            state: State::Waiting(OWN_ROWS.to_owned()),
            trouble: None,
        }
    }

    /// Starts again: the source's table is made anew (of its own columns).
    fn restart(&mut self) {
        self.state = State::Waiting(OWN_ROWS.to_owned());
        self.trouble = None;
    }

    /// Cannot be joined, for `why`, until the source's table starts over.
    pub(crate) fn refuse(&mut self, why: &str) {
        self.state = State::Off {
            why: why.to_owned(),
            reference: None,
        };
    }

    /// The other table's offer and instance, if it has a table to join (`offer` `None`: no
    /// table of that name is open); else says what the join waits for. A join made goes on
    /// with what it has meanwhile: the source's rows still go in through it.
    pub(crate) fn ready<'o>(&mut self, offer: Option<&'o Offer>) -> Option<(&'o Offer, u64)> {
        let name = &self.spec.table;
        let why = match offer {
            None => format!("no table named {name} is open"),
            Some(offer) if offer.paged => {
                // Said once, not made anew each frame.
                let refused = matches!(
                    self.state,
                    State::Off {
                        reference: None,
                        ..
                    }
                );
                if !refused && !matches!(self.state, State::On(_)) {
                    self.refuse(&format!(
                        "{name} is read a page at a time (too large to load), and Studio joins \
                         only tables it holds: join them in SQL instead"
                    ));
                }
                return None;
            }
            Some(offer) => match offer.instance {
                Some(instance) => return Some((offer, instance)),
                None => format!("{name} has no rows yet"),
            },
        };
        if !matches!(self.state, State::On(_)) {
            self.state = State::Waiting(why);
        }
        None
    }

    /// Whether [`step`](Self::step) has nothing to do with `offer` of the other table's
    /// `instance`: the join is made with that table, nothing arrived and no rows are to be
    /// looked over; or it cannot be made with it.
    pub(crate) fn is_idle(&self, offer: &Offer, instance: u64, all: bool) -> bool {
        match &self.state {
            State::On(linked) => linked.reference == instance && offer.arrived.is_empty() && !all,
            State::Off { reference, .. } => reference.is_none() || *reference == Some(instance),
            State::Waiting(_) => false,
        }
    }

    /// The join, once made with `table` its output: the source's rows go in through it.
    pub(crate) fn join_for(&mut self, table: &LiveTable) -> Option<&mut LookupJoin> {
        self.follow(table);
        match &mut self.state {
            State::On(linked) => Some(&mut linked.join),
            _ => None,
        }
    }

    /// Starts again if `table` is not the output of the join made: the source's table was
    /// made anew (a Flight result read again).
    fn follow(&mut self, table: &LiveTable) {
        if matches!(&self.state, State::On(linked) if linked.output != table.instance()) {
            self.restart();
        }
    }

    /// What the status bar says of it.
    pub(crate) fn describe(&self) -> String {
        let spec = &self.spec;
        let mut text = match &self.state {
            State::On(_) => format!("joined with {} on {}", spec.table, spec.column),
            State::Waiting(why) => format!("join waiting: {why}"),
            State::Off { why, .. } => format!("join off: {why}"),
        };
        if let Some(trouble) = &self.trouble {
            text.push_str(&format!(", {}'s rows refused: {trouble}", spec.table));
        }
        text
    }

    /// Whether [`step`](Self::step) needs all the other table's rows with `offer`: to make
    /// the join, to make it anew, or to look for rows that left.
    pub(crate) fn wants_all(&self, offer: &Offer) -> bool {
        let Some(instance) = offer.instance else {
            return false;
        };
        match &self.state {
            State::On(linked) if linked.reference == instance => {
                linked.prune_due(&self.spec, offer)
            }
            State::Off { reference, .. } => *reference != Some(instance),
            _ => !offer.joined,
        }
    }

    /// Takes in what the other table (of `instance`) offers, and `all` its rows if
    /// [`wants_all`](Self::wants_all) said so, for the source's `table`, keyed by `key`.
    pub(crate) fn step(
        &mut self,
        table: &mut LiveTable,
        key: &str,
        (offer, instance): (&Offer, u64),
        all: Option<Vec<RecordBatch>>,
    ) -> Stepped {
        self.follow(table);
        match &mut self.state {
            // Not idle: rows came, or are to be looked over.
            State::On(linked) if linked.reference == instance => {
                match linked.take(&self.spec, table, offer, all) {
                    // What went wrong stays said until rows are taken again.
                    Err(trouble) => self.trouble = Some(trouble),
                    Ok(()) if !offer.arrived.is_empty() => self.trouble = None,
                    Ok(()) => {}
                }
                return Stepped::Changed;
            }
            State::Off { reference, .. } if *reference == Some(instance) => {
                return Stepped::Nothing
            }
            _ => {}
        }
        if offer.joined {
            let why = format!(
                "{} is itself joined to another table, and Studio joins only tables that are not",
                self.spec.table
            );
            return self.off(table, key, why, instance);
        }
        let Some(all) = all else {
            return Stepped::Nothing;
        };
        // Made anew when the other table is (a Flight result read again, maybe with other
        // columns): from the source's own columns, kept apart in a join made.
        let (left, rows) = match own_rows(&self.state, table) {
            Ok(own) => own,
            Err(why) => return self.off(table, key, why, instance),
        };
        match Linked::new(&self.spec, left, rows, key, offer, instance, all) {
            Ok((linked, joined)) => {
                self.state = State::On(Box::new(linked));
                self.trouble = None;
                Stepped::Replaced(Box::new(joined))
            }
            Err(why) => self.off(table, key, why, instance),
        }
    }

    /// Cannot be joined with the other table of `instance`, for `why`: a join made gives the
    /// source back its own columns.
    fn off(&mut self, table: &LiveTable, key: &str, why: String, instance: u64) -> Stepped {
        let own = match &self.state {
            State::On(linked) => Some(plain(table, &linked.left, key)),
            _ => None,
        };
        let reference = Some(instance);
        match own {
            None => {
                self.state = State::Off { why, reference };
                Stepped::Nothing
            }
            Some(Ok(own)) => {
                self.state = State::Off { why, reference };
                Stepped::Replaced(Box::new(own))
            }
            // Kept joined to what it had: the source's rows still go in through it.
            Some(Err(error)) => {
                self.trouble = Some(format!("{why}; its own columns not restored: {error}"));
                Stepped::Nothing
            }
        }
    }
}

/// The source's own columns and rows in `table`: all of it, or the left columns of a join
/// made.
fn own_rows(state: &State, table: &LiveTable) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let rows = table.snapshot().map_err(|e| e.to_string())?;
    let State::On(linked) = state else {
        return Ok((table.schema().clone(), rows));
    };
    let left = Arc::clone(&linked.left);
    let width = left.fields().len();
    let rows = rows
        .into_iter()
        .map(|batch| RecordBatch::try_new(Arc::clone(&left), batch.columns()[..width].to_vec()))
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    Ok((left, rows))
}

/// A table of the source's own columns (`left`), keyed by `key`, with the rows of `table`,
/// a join made.
fn plain(table: &LiveTable, left: &SchemaRef, key: &str) -> Result<LiveTable, String> {
    let width = left.fields().len();
    let mut own = LiveTable::new(Arc::clone(left), key).map_err(|e| e.to_string())?;
    for batch in table.snapshot().map_err(|e| e.to_string())? {
        let batch = RecordBatch::try_new(Arc::clone(left), batch.columns()[..width].to_vec())
            .map_err(|e| e.to_string())?;
        own.upsert(batch).map_err(|e| e.to_string())?;
    }
    Ok(own)
}

impl Linked {
    /// The join of `rows` of the source's `left` columns (keyed by `key`) to `all` the other
    /// table's, and the joined table; or why they cannot be joined.
    fn new(
        spec: &JoinSpec,
        left: SchemaRef,
        rows: Vec<RecordBatch>,
        key: &str,
        offer: &Offer,
        reference: u64,
        all: Vec<RecordBatch>,
    ) -> Result<(Self, LiveTable), String> {
        if left.column_with_name(&spec.column).is_none() {
            return Err(format!("this source's rows have no column {}", spec.column));
        }
        if offer.schema.column_with_name(&spec.key).is_none() {
            return Err(format!(
                "{} has no column {}: join=COLUMN:{}.KEY names its column",
                spec.table, spec.key, spec.table
            ));
        }
        let (columns, schema) = right_columns(spec, &left, offer);
        let key_at = schema.index_of(&spec.key).map_err(|e| e.to_string())?;
        let join = LookupJoin::new(
            Arc::clone(&left),
            key,
            &spec.column,
            Arc::clone(&schema),
            &spec.key,
        )
        .map_err(|e| e.to_string())?;
        let mut joined = join.table().map_err(|e| e.to_string())?;
        let mut linked = Self {
            join,
            output: joined.instance(),
            left,
            reference,
            columns,
            schema,
            key_at,
            deletions: offer.deletions,
            updates: offer.updates,
            pruned_at: Instant::now(),
            prune_every: PRUNE_EVERY,
        };
        // The other table's rows first, so the source's are joined once, as they go in.
        for batch in all {
            linked.upsert_right(&mut joined, &batch)?;
        }
        for batch in rows {
            linked
                .join
                .upsert_left(&mut joined, batch)
                .map_err(|e| e.to_string())?;
        }
        Ok((linked, joined))
    }

    /// Takes in the rows the other table took, then, given `all` its rows, lets go of those
    /// that left it. What went wrong last, if anything.
    fn take(
        &mut self,
        spec: &JoinSpec,
        table: &mut LiveTable,
        offer: &Offer,
        all: Option<Vec<RecordBatch>>,
    ) -> Result<(), String> {
        let mut taken = Ok(());
        for batch in &offer.arrived {
            if let Err(error) = self.upsert_right(table, batch) {
                taken = Err(error);
            }
        }
        if let Some(all) = all {
            let started = Instant::now();
            self.deletions = offer.deletions;
            self.updates = offer.updates;
            if let Err(error) = self.prune(spec, table, &all) {
                taken = Err(error);
            }
            // A large table is read less often: at most a quarter of the time goes to it.
            self.pruned_at = Instant::now();
            self.prune_every = PRUNE_EVERY.max(started.elapsed() * 4);
        }
        taken
    }

    /// Whether rows may have left the other table since it was last checked, and a check
    /// is due: rows were deleted from it, or replaced (one keyed by another column may have
    /// named another row).
    fn prune_due(&self, spec: &JoinSpec, offer: &Offer) -> bool {
        let removed = offer.deletions != self.deletions
            || (offer.key != spec.key && offer.updates != self.updates);
        removed && self.pruned_at.elapsed() >= self.prune_every
    }

    /// Joins the other table's `batch` (in that table's columns) into `table`; its rows
    /// without a key name nothing and are left out.
    fn upsert_right(
        &mut self,
        table: &mut LiveTable,
        batch: &RecordBatch,
    ) -> Result<Changes, String> {
        let columns = self
            .columns
            .iter()
            .map(|&i| batch.columns().get(i).map(Arc::clone))
            .collect::<Option<Vec<_>>>()
            .ok_or("fewer columns than the table has")?;
        let mut batch =
            RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|e| e.to_string())?;
        let keys = batch.column(self.key_at);
        if keys.null_count() > 0 {
            let keyed: BooleanArray = (0..keys.len()).map(|i| Some(keys.is_valid(i))).collect();
            batch = arrow_select::filter::filter_record_batch(&batch, &keyed)
                .map_err(|e| e.to_string())?;
        }
        self.join
            .upsert_right(table, batch)
            .map_err(|e| e.to_string())
    }

    /// Lets go of the other table's rows whose key is in none of `all` its rows now.
    fn prune(
        &mut self,
        spec: &JoinSpec,
        table: &mut LiveTable,
        all: &[RecordBatch],
    ) -> Result<(), String> {
        let mut present = HashSet::new();
        for batch in all {
            let column = batch
                .column_by_name(&spec.key)
                .ok_or_else(|| format!("{} has no column {}", spec.table, spec.key))?;
            let keys = as_text(column)?;
            present.extend(keys.iter().flatten().map(str::to_owned));
        }
        let mut gone: Vec<ArrayRef> = Vec::new();
        for batch in self.join.right().snapshot().map_err(|e| e.to_string())? {
            let keys = batch.column(self.key_at);
            let text = as_text(keys)?;
            let left: BooleanArray = text
                .iter()
                .map(|key| Some(key.is_some_and(|key| !present.contains(key))))
                .collect();
            if left.true_count() > 0 {
                gone.push(arrow_select::filter::filter(keys, &left).map_err(|e| e.to_string())?);
            }
        }
        for keys in gone {
            self.join
                .delete_right(table, keys.as_ref())
                .map_err(|e: DataError| e.to_string())?;
        }
        Ok(())
    }
}

/// The other table's columns the join takes (all but rows' numbers Studio gave them,
/// unless they are looked up in), named apart from the source's own: a name both have
/// gets the table's name in front (`sites_name`).
fn right_columns(spec: &JoinSpec, left: &Schema, offer: &Offer) -> (Vec<usize>, SchemaRef) {
    let mut taken: Vec<String> = left.fields().iter().map(|f| f.name().clone()).collect();
    let mut columns = Vec::new();
    let mut fields = Vec::new();
    for (i, field) in offer.schema.fields().iter().enumerate() {
        let name = field.name();
        if offer.numbered && *name == offer.key && *name != spec.key {
            continue;
        }
        let mut renamed = name.clone();
        // The key is left out of the joined columns: its value is in the source's own.
        if *name != spec.key && taken.contains(&renamed) {
            renamed = format!("{}_{name}", spec.table);
            while taken.contains(&renamed) || offer.schema.column_with_name(&renamed).is_some() {
                renamed.push('_');
            }
        }
        taken.push(renamed.clone());
        columns.push(i);
        fields.push(
            Field::new(renamed, field.data_type().clone(), true)
                .with_metadata(field.metadata().clone()),
        );
    }
    (columns, Arc::new(Schema::new(fields)))
}

/// `keys` as text, to compare keys of any kind.
fn as_text(keys: &ArrayRef) -> Result<StringArray, String> {
    let text = arrow_cast::cast(keys, &DataType::Utf8).map_err(|e| e.to_string())?;
    Ok(text
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("keys not read as text")?
        .clone())
}

/// Links each source joined to another open table with it, for this frame: the rows each
/// other table took in since the last frame are joined, and tables joined anew replace
/// the sources' own.
pub(crate) fn link(ui: &mut Ui, sources: &mut [Source]) {
    let wanted: Vec<Option<usize>> = sources
        .iter()
        .map(|source| {
            let table = &source.joined.as_ref()?.spec.table;
            sources.iter().position(|other| other.name == *table)
        })
        .collect();
    let offers: Vec<Option<Offer>> = (0..sources.len())
        .map(|r| {
            let watched = wanted.contains(&Some(r));
            sources[r].watched = watched;
            // An offer takes the rows kept; a source nobody joins keeps none.
            let offer = sources[r].offer(ui);
            watched.then_some(offer)
        })
        .collect();
    for (i, reference) in wanted.iter().enumerate() {
        if sources[i].joined.is_none() {
            continue;
        }
        let offer = reference.and_then(|r| offers[r].as_ref());
        let all = match (reference, offer, &sources[i].joined) {
            (Some(r), Some(offer), Some(joined)) if joined.wants_all(offer) => {
                sources[*r].snapshot(ui).ok().map(|(_, batches)| batches)
            }
            _ => None,
        };
        let itself = *reference == Some(i);
        sources[i].link(ui, offer, all, itself);
    }
}

#[cfg(test)]
mod tests;
