//! Joins of a remote source's rows to another open table, kept as either side changes:
//! lookups, `join=COLUMN:TABLE` after `#` (the table's column of the same name) or
//! `join=COLUMN:TABLE.KEY`, show each row with the columns of the row it names; pairs,
//! `pairs=COLUMN:TABLE` or `pairs=COLUMN:TABLE.OTHER`, show each row once with every row of
//! the table holding the same value (once with empty columns if none does), in a table keyed
//! by a `pair` column. The joined rows are the source's table: its grid, its window, its
//! counts and SQL all see them.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_data::{Changes, DataError, LiveTable, LookupJoin, PairJoin};
use densa_ui::Ui;

use crate::source::Source;
use crate::stream::encode;
use crate::time::TimeSpec;

/// How often, at most, the joined rows are checked for rows that left the other table:
/// the check reads all of it.
const PRUNE_EVERY: Duration = Duration::from_secs(1);
/// What a join waits for first.
const OWN_ROWS: &str = "waiting for this source's rows";

/// What `join=` or `pairs=` says: which column is matched with which table, by which of its
/// columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JoinSpec {
    /// The source's column naming (or holding the values of) the other table's rows.
    pub(crate) column: String,
    /// The other table, by the name SQL knows it by.
    pub(crate) table: String,
    /// Its column `column`'s values are matched with: looked up in, or paired by.
    pub(crate) key: String,
    /// Whether each row is paired with every row holding its value (`pairs=`), not with the
    /// one row it names (`join=`).
    pub(crate) pairs: bool,
}

impl JoinSpec {
    /// The options, for messages about unknown ones.
    pub(crate) const KNOWN: &'static str = "join=COLUMN:TABLE, pairs=COLUMN:TABLE";
    /// What a source given two joins is told.
    pub(crate) const TWICE: &'static str =
        "join= or pairs= is given twice: a source is joined to one table, by one of them";

    /// Reads `join=`'s `COLUMN:TABLE` or `COLUMN:TABLE.KEY` (table names have no dots).
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        Self::read(value, false).ok_or_else(|| {
            "join= names a column and the open table its values name rows of: \
             join=COLUMN:TABLE, or join=COLUMN:TABLE.KEY when that table's column has \
             another name"
                .to_owned()
        })
    }

    /// Reads `pairs=`'s `COLUMN:TABLE` or `COLUMN:TABLE.OTHER`.
    pub(crate) fn pairs(value: &str) -> Result<Self, String> {
        Self::read(value, true).ok_or_else(|| {
            "pairs= names a column and the open table whose rows holding the same value each \
             row is shown with: pairs=COLUMN:TABLE, or pairs=COLUMN:TABLE.OTHER when that \
             table's column has another name"
                .to_owned()
        })
    }

    fn read(value: &str, pairs: bool) -> Option<Self> {
        let (column, table) = value.split_once(':')?;
        let (table, key) = table.split_once('.').unwrap_or((table, column));
        if column.is_empty() || table.is_empty() || key.is_empty() {
            return None;
        }
        Some(Self {
            column: column.to_owned(),
            table: table.to_owned(),
            key: key.to_owned(),
            pairs,
        })
    }

    /// The option's name: `join` or `pairs`.
    fn name(&self) -> &'static str {
        if self.pairs {
            "pairs"
        } else {
            "join"
        }
    }

    /// The option giving this join, for a source's origin.
    pub(crate) fn option(&self) -> String {
        format!(
            "{}={}:{}.{}",
            self.name(),
            encode(&self.column),
            encode(&self.table),
            encode(&self.key)
        )
    }
}

/// Refuses `window=` with `pairs=`: a pair join holds every row of the source, and a window
/// on the joined rows lets go of pairs, not of the rows behind them, which would be held for
/// good (and their pairs made again whenever the other table changes).
pub(crate) fn check_window(join: Option<&JoinSpec>, time: Option<&TimeSpec>) -> Result<(), String> {
    let pairs = join.is_some_and(|join| join.pairs);
    let window = time.is_some_and(|time| time.window.is_some());
    if pairs && window {
        return Err(
            "window= cannot go with pairs= yet: the source's rows a window let go of \
                    would stay held for their pairs; use join= (one row each), or leave out \
                    window="
                .to_owned(),
        );
    }
    Ok(())
}

/// Deletes the source's rows keyed `keys` (its key's values) from its `table`: through the
/// join made with it, if one is (pairs are keyed by pair, not by the source's key).
pub(crate) fn delete_rows(
    joined: Option<&mut Joined>,
    table: &mut LiveTable,
    keys: &dyn Array,
) -> Result<Changes, DataError> {
    match joined.and_then(|joined| joined.join_for(table)) {
        Some(join) => join.delete_left(table, keys),
        None => table.delete(keys),
    }
}

/// A join made: a lookup or pairs.
pub(crate) enum Join {
    // Boxed: the two are hundreds of bytes, of different sizes.
    Lookup(Box<LookupJoin>),
    Pairs(Box<PairJoin>),
}

impl Join {
    /// Joins the source's `rows` into `out`.
    pub(crate) fn upsert_left(
        &mut self,
        out: &mut LiveTable,
        rows: RecordBatch,
    ) -> Result<Changes, DataError> {
        match self {
            Self::Lookup(join) => join.upsert_left(out, rows),
            Self::Pairs(join) => join.upsert_left(out, rows),
        }
    }

    /// Deletes the source's rows keyed `keys` from `out`: a lookup's output is keyed as the
    /// source is, so they go from it; pairs go through the join (their output is keyed by
    /// pair).
    pub(crate) fn delete_left(
        &mut self,
        out: &mut LiveTable,
        keys: &dyn Array,
    ) -> Result<Changes, DataError> {
        match self {
            Self::Lookup(_) => out.delete(keys),
            Self::Pairs(join) => join.delete_left(out, keys),
        }
    }

    fn upsert_right(
        &mut self,
        out: &mut LiveTable,
        rows: RecordBatch,
    ) -> Result<Changes, DataError> {
        match self {
            Self::Lookup(join) => join.upsert_right(out, rows),
            Self::Pairs(join) => join.upsert_right(out, rows),
        }
    }

    fn delete_right(
        &mut self,
        out: &mut LiveTable,
        keys: &dyn Array,
    ) -> Result<Changes, DataError> {
        match self {
            Self::Lookup(join) => join.delete_right(out, keys),
            Self::Pairs(join) => join.delete_right(out, keys),
        }
    }

    /// The other table's rows, as the join holds them.
    fn right(&self) -> &LiveTable {
        match self {
            Self::Lookup(join) => join.right(),
            Self::Pairs(join) => join.right(),
        }
    }

    /// The join of `left` (the source's columns, keyed by `key`) to `right` (the other
    /// table's, its rows keyed in the join by `right_key`) that `spec` asks for, and an
    /// output table for it.
    fn make(
        spec: &JoinSpec,
        left: &SchemaRef,
        key: &str,
        right: &SchemaRef,
        right_key: &str,
    ) -> Result<(Self, LiveTable), DataError> {
        let (left, right) = (Arc::clone(left), Arc::clone(right));
        if spec.pairs {
            let join = PairJoin::new(left, key, &spec.column, right, right_key, &spec.key)?;
            let output = join.table()?;
            Ok((Self::Pairs(Box::new(join)), output))
        } else {
            let join = LookupJoin::new(left, key, &spec.column, right, &spec.key)?;
            let output = join.table()?;
            Ok((Self::Lookup(Box::new(join)), output))
        }
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
    join: Join,
    /// The source's own columns, first in the joined table.
    left: SchemaRef,
    /// The instance of the joined table.
    output: u64,
    /// The instance of the other table it was made from.
    reference: u64,
    /// The other table's columns the join takes, their names in the join, and where its
    /// rows' keys in the join are among them.
    columns: Vec<usize>,
    schema: SchemaRef,
    key_at: usize,
    /// The other table's column keying its rows in the join: the one looked up in, or, for
    /// pairs, its own key.
    right_key: String,
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
    pub(crate) fn join_for(&mut self, table: &LiveTable) -> Option<&mut Join> {
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
            State::On(_) if spec.pairs => {
                format!("paired with rows of {} on {}", spec.table, spec.column)
            }
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
            State::On(linked) if linked.reference == instance => linked.prune_due(offer),
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
            State::On(linked) => Some(plain(linked, table, key)),
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

/// The source's own columns and rows in `table`: all of it, or the source's rows of a join
/// made.
fn own_rows(state: &State, table: &LiveTable) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let State::On(linked) = state else {
        let rows = table.snapshot().map_err(|e| e.to_string())?;
        return Ok((table.schema().clone(), rows));
    };
    Ok((Arc::clone(&linked.left), linked.own(table)?))
}

/// A table of the source's own columns, keyed by `key`, with its rows in `table`, the
/// output of the join made `linked`.
fn plain(linked: &Linked, table: &LiveTable, key: &str) -> Result<LiveTable, String> {
    let mut own = LiveTable::new(Arc::clone(&linked.left), key).map_err(|e| e.to_string())?;
    for batch in linked.own(table)? {
        own.upsert(batch).map_err(|e| e.to_string())?;
    }
    Ok(own)
}

impl Linked {
    /// The source's rows, in its own columns: a pair join holds them apart (its output holds
    /// each once per pair); a lookup's output holds each once, its columns first.
    fn own(&self, table: &LiveTable) -> Result<Vec<RecordBatch>, String> {
        let rows = match &self.join {
            Join::Pairs(join) => join.left().snapshot(),
            Join::Lookup(_) => table.snapshot(),
        }
        .map_err(|e| e.to_string())?;
        let width = self.left.fields().len();
        rows.into_iter()
            .map(|batch| {
                RecordBatch::try_new(Arc::clone(&self.left), batch.columns()[..width].to_vec())
            })
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())
    }

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
            let (option, other) = if spec.pairs {
                ("pairs", "OTHER")
            } else {
                ("join", "KEY")
            };
            return Err(format!(
                "{} has no column {}: {option}=COLUMN:{}.{other} names its column",
                spec.table, spec.key, spec.table
            ));
        }
        // Pairs hold each of the other table's rows by its own key; a lookup, by the value
        // looked up.
        let right_key = if spec.pairs { &offer.key } else { &spec.key };
        let (columns, schema) = right_columns(spec, &left, offer);
        let key_at = columns
            .iter()
            .position(|&i| offer.schema.field(i).name() == right_key)
            .ok_or_else(|| format!("{} has no column {right_key}", spec.table))?;
        let named = schema.field(key_at).name();
        let (join, mut joined) =
            Join::make(spec, &left, key, &schema, named).map_err(|e| e.to_string())?;
        let mut linked = Self {
            join,
            output: joined.instance(),
            left,
            reference,
            columns,
            schema,
            key_at,
            right_key: right_key.clone(),
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
    /// is due: rows were deleted from it, or replaced (in a lookup by another column than
    /// its key, a row replaced may have named another; pairs hold rows by its key, so a row
    /// replaced arrives in place of the old).
    fn prune_due(&self, offer: &Offer) -> bool {
        let removed = offer.deletions != self.deletions
            || (offer.key != self.right_key && offer.updates != self.updates);
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
                .column_by_name(&self.right_key)
                .ok_or_else(|| format!("{} has no column {}", spec.table, self.right_key))?;
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

/// The other table's columns the join takes, named apart from the source's own (a name
/// both have gets the table's name in front: `sites_name`). The row numbers Studio gave
/// the other table are left out of a lookup unless they are looked up in; pairs keep them,
/// as they tell its rows apart.
fn right_columns(spec: &JoinSpec, left: &Schema, offer: &Offer) -> (Vec<usize>, SchemaRef) {
    let mut taken: Vec<String> = left.fields().iter().map(|f| f.name().clone()).collect();
    let mut columns = Vec::new();
    let mut fields = Vec::new();
    for (i, field) in offer.schema.fields().iter().enumerate() {
        let name = field.name();
        if offer.numbered && *name == offer.key && *name != spec.key && !spec.pairs {
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
