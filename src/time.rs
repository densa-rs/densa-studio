//! Time columns of remote sources: `#time=COLUMN` says which column holds the rows' times, and
//! Studio turns it into UTC timestamps as rows arrive (numbers count milliseconds unless
//! `unit=s`, `us` or `ns` says otherwise, as JSON feeds mostly send them; text is read as
//! ISO 8601, or as such a number). `window=10m` then keeps only the rows within ten minutes of
//! the newest.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::{Array, ArrayRef, RecordBatch, TimestampMillisecondArray};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use densa_data::{LiveTable, Window};

/// What a number column of times counts, unless `unit=` says.
const DEFAULT_UNIT: Unit = Unit::Millis;
/// The longest window: longer ones are surely mistyped.
const MOST: Duration = Duration::from_secs(366 * 86_400);
/// The time zone timestamps are given in: epoch numbers and ISO 8601 text name instants. An
/// offset, not the name `UTC`: Arrow reads names only with its chrono-tz feature.
const UTC: &str = "+00:00";

/// What a number column of times counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl Unit {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "s" => Self::Seconds,
            "ms" => Self::Millis,
            "us" => Self::Micros,
            "ns" => Self::Nanos,
            _ => return None,
        })
    }

    fn text(self) -> &'static str {
        match self {
            Self::Seconds => "s",
            Self::Millis => "ms",
            Self::Micros => "us",
            Self::Nanos => "ns",
        }
    }

    fn arrow(self) -> TimeUnit {
        match self {
            Self::Seconds => TimeUnit::Second,
            Self::Millis => TimeUnit::Millisecond,
            Self::Micros => TimeUnit::Microsecond,
            Self::Nanos => TimeUnit::Nanosecond,
        }
    }
}

/// A source's time column, made timestamps as rows arrive, and the window kept over it, if
/// any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeSpec {
    pub(crate) column: String,
    /// What a number column counts, if `unit=` said.
    unit: Option<Unit>,
    /// How far back from the newest time rows are kept; every row without one.
    pub(crate) window: Option<Duration>,
}

/// The time options of a source's URL, as they are read.
#[derive(Debug, Default)]
pub(crate) struct TimeOptions {
    column: Option<String>,
    span: Option<Duration>,
    unit: Option<Unit>,
}

impl TimeOptions {
    /// The options Studio knows here, for messages about unknown ones.
    pub(crate) const KNOWN: &'static str = "time=COLUMN, unit=s and window=10m";

    /// Takes in `name=value` if it is a time option; whether it was.
    pub(crate) fn take(&mut self, name: &str, value: &str) -> Result<bool, String> {
        let twice = || format!("{name}= is given twice");
        match name {
            "time" if value.is_empty() => return Err("time needs a value".into()),
            "time" if self.column.is_some() => return Err(twice()),
            "time" => self.column = Some(value.to_owned()),
            "window" if self.span.is_some() => return Err(twice()),
            "window" => self.span = Some(span(value)?),
            "unit" if self.unit.is_some() => return Err(twice()),
            "unit" => {
                self.unit = Some(Unit::parse(value).ok_or("unit= is s, ms, us or ns")?);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// The time column the options name, if any; `key` is the source's key column, which
    /// cannot be its time column too (keys are numbers or text, not timestamps).
    pub(crate) fn finish(self, key: Option<&str>) -> Result<Option<TimeSpec>, String> {
        if self.column.is_some() && self.column.as_deref() == key {
            return Err("time= and key= name the same column: a key cannot be a time".into());
        }
        let Some(column) = self.column else {
            return match (self.span, self.unit) {
                (Some(_), _) => Err("window= needs the column of times: time=COLUMN".into()),
                (None, Some(_)) => Err("unit= goes with the column of times: time=COLUMN".into()),
                (None, None) => Ok(None),
            };
        };
        Ok(Some(TimeSpec {
            column,
            unit: self.unit,
            window: self.span,
        }))
    }
}

impl TimeSpec {
    /// The options that give this time column, for a source's origin.
    pub(crate) fn options(&self) -> Vec<String> {
        let mut options = vec![format!("time={}", crate::stream::encode(&self.column))];
        if let Some(span) = self.window {
            options.push(format!("window={}", written(span)));
        }
        if let Some(unit) = self.unit {
            options.push(format!("unit={}", unit.text()));
        }
        options
    }

    /// What the status bar says of its window, if it has one.
    pub(crate) fn describe(&self) -> Option<String> {
        let span = self.window?;
        Some(format!("last {} by {}", written(span), self.column))
    }

    /// `batch` with its time column made UTC timestamps: numbers counting the unit, text read
    /// as ISO 8601 or as such a number, to the millisecond (other text becomes null). Also what
    /// went wrong, if anything did: the rows come all the same. A column's type gives its
    /// timestamps' type, so batches of one source agree.
    pub(crate) fn convert(&self, batch: RecordBatch) -> (RecordBatch, Option<String>) {
        let schema = batch.schema();
        let Some((index, field)) = schema.column_with_name(&self.column) else {
            let missing = format!("the time column {} is not in the rows", self.column);
            return (batch, Some(missing));
        };
        let column = batch.column(index);
        let to = match (field.data_type(), self.unit) {
            (DataType::Timestamp(..) | DataType::Date32 | DataType::Date64, None) => {
                return (batch, None)
            }
            (data_type, Some(_)) if !data_type.is_numeric() && !is_text(data_type) => {
                let wrong = format!(
                    "unit= is for a column of numbers; {} holds {data_type}",
                    self.column
                );
                return (batch, Some(wrong));
            }
            (data_type, unit) if data_type.is_numeric() => {
                timestamp(unit.unwrap_or(DEFAULT_UNIT).arrow())
            }
            (data_type, _) if is_text(data_type) => timestamp(TimeUnit::Millisecond),
            (data_type, _) => {
                let wrong = format!("{} holds {data_type}, not times", self.column);
                return (batch, Some(wrong));
            }
        };
        let unit = self.unit.unwrap_or(DEFAULT_UNIT);
        let converted = match to_times(column, &to, unit) {
            Ok(times) => times,
            Err(error) => return (batch, Some(error)),
        };
        let unread = converted.null_count() - column.null_count();
        let problem = (unread > 0).then(|| match field.data_type().is_numeric() {
            true => format!("{unread} times in {} out of range", self.column),
            false => format!(
                "{unread} times in {} neither ISO 8601 nor numbers",
                self.column
            ),
        });
        let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields[index] = Field::new(field.name(), to, true).with_metadata(field.metadata().clone());
        let converted_schema = Schema::new_with_metadata(fields, schema.metadata().clone());
        let mut columns = batch.columns().to_vec();
        columns[index] = converted;
        match RecordBatch::try_new(Arc::new(converted_schema), columns) {
            Ok(converted) => (converted, problem),
            Err(error) => (batch, Some(error.to_string())),
        }
    }

    /// Keeps `table` to the window, if there is one and the time column can take it.
    pub(crate) fn apply(&self, table: &mut LiveTable) -> Result<(), String> {
        let Some(span) = self.window else {
            return Ok(());
        };
        let schema = table.schema().clone();
        let field = schema
            .field_with_name(&self.column)
            .map_err(|_| format!("the window's column {} is not in the rows", self.column))?;
        if !matches!(
            field.data_type(),
            DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
        ) {
            return Err(format!(
                "a window needs times; {} holds {}",
                self.column,
                field.data_type()
            ));
        }
        table
            .set_window(Some(Window::duration(&self.column, span)))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

fn timestamp(unit: TimeUnit) -> DataType {
    DataType::Timestamp(unit, Some(UTC.into()))
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// `column` as `to` (a timestamp type). Numbers count `unit` (floats cut to whole units);
/// text is read as ISO 8601, or else as a number counting `unit` (a JSON feed whose first
/// time was null sends its numbers as text).
fn to_times(column: &ArrayRef, to: &DataType, unit: Unit) -> Result<ArrayRef, String> {
    let error = |e: arrow_schema::ArrowError| e.to_string();
    if is_text(column.data_type()) {
        let iso = cast(column, to).map_err(error)?;
        let numbers = cast(column, &DataType::Float64).map_err(error)?;
        let numbers = to_times(&numbers, &timestamp(unit.arrow()), unit)?;
        let numbers = cast(&numbers, to).map_err(error)?;
        let (iso, numbers) = (
            iso.as_primitive::<TimestampMillisecondType>(),
            numbers.as_primitive::<TimestampMillisecondType>(),
        );
        let either: TimestampMillisecondArray = iso
            .iter()
            .zip(numbers.iter())
            .map(|(i, n)| i.or(n))
            .collect();
        return Ok(Arc::new(either.with_timezone(UTC)));
    }
    cast(column, to).map_err(error)
}

/// A span written as a number and a unit: `500ms`, `30s`, `10m`, `1.5h`, `7d`.
fn span(text: &str) -> Result<Duration, String> {
    let wrong = || format!("window={text}: write a span such as 30s, 10m, 2h or 1d");
    let digits = text
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .ok_or_else(wrong)?;
    let (number, unit) = text.split_at(digits);
    let number: f64 = number.parse().map_err(|_| wrong())?;
    let seconds = match unit {
        "ms" => number / 1e3,
        "s" => number,
        "m" | "min" => number * 60.0,
        "h" => number * 3_600.0,
        "d" => number * 86_400.0,
        _ => return Err(wrong()),
    };
    if seconds > MOST.as_secs_f64() {
        return Err(format!("window={text} is longer than a year"));
    }
    match Duration::try_from_secs_f64(seconds) {
        Ok(span) if span.is_zero() => Err(format!("window={text} keeps nothing")),
        Ok(span) => Ok(span),
        Err(_) => Err(wrong()),
    }
}

/// `span` written in the largest unit that holds it whole (`90m`, not `1.5h`), so one span
/// is always written one way.
fn written(span: Duration) -> String {
    const UNITS: [(&str, u128); 5] = [
        ("d", 86_400_000_000_000),
        ("h", 3_600_000_000_000),
        ("m", 60_000_000_000),
        ("s", 1_000_000_000),
        ("ms", 1_000_000),
    ];
    let nanos = span.as_nanos();
    match UNITS.iter().find(|(_, size)| nanos.is_multiple_of(*size)) {
        Some((unit, size)) => format!("{}{unit}", nanos / size),
        None => format!("{}ms", nanos as f64 / 1e6),
    }
}

#[cfg(test)]
mod tests;
