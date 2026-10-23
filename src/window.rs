//! Time windows over remote sources: `#time=COLUMN&window=10m` keeps the rows whose time is
//! within ten minutes of the newest, and `unit=s` says what a number column counts (it
//! counts milliseconds otherwise, as JSON feeds mostly do).

use std::time::Duration;

use arrow_schema::DataType;
use densa_data::{LiveTable, Window};

/// What a number column of times counts, unless `unit=` says.
const DEFAULT_UNIT: Unit = Unit::Millis;
/// The longest window: longer ones are surely mistyped.
const MOST: Duration = Duration::from_secs(366 * 86_400);

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

    fn nanos(self) -> f64 {
        match self {
            Self::Seconds => 1e9,
            Self::Millis => 1e6,
            Self::Micros => 1e3,
            Self::Nanos => 1.0,
        }
    }

    fn text(self) -> &'static str {
        match self {
            Self::Seconds => "s",
            Self::Millis => "ms",
            Self::Micros => "us",
            Self::Nanos => "ns",
        }
    }
}

/// Keeps a source's rows whose `column` is within `span` of the newest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeWindow {
    pub(crate) column: String,
    pub(crate) span: Duration,
    /// What a number column counts, if `unit=` said.
    unit: Option<Unit>,
}

/// The window options of a source's URL, as they are read.
#[derive(Debug, Default)]
pub(crate) struct WindowOptions {
    column: Option<String>,
    span: Option<Duration>,
    unit: Option<Unit>,
}

impl WindowOptions {
    /// The options Studio knows here, for messages about unknown ones.
    pub(crate) const KNOWN: &'static str = "time=COLUMN, window=10m and unit=s";

    /// Takes in `name=value` if it is a window option; whether it was.
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

    /// The window the options describe, if any.
    pub(crate) fn finish(self) -> Result<Option<TimeWindow>, String> {
        match (self.column, self.span) {
            (Some(column), Some(span)) => Ok(Some(TimeWindow {
                column,
                span,
                unit: self.unit,
            })),
            (None, Some(_)) => Err("window= needs the column of times: time=COLUMN".into()),
            (Some(_), None) => Err("time= goes with a window: window=10m".into()),
            (None, None) if self.unit.is_some() => {
                Err("unit= goes with a window: time=COLUMN&window=10m".into())
            }
            (None, None) => Ok(None),
        }
    }
}

impl TimeWindow {
    /// The options that give this window, for a source's origin.
    pub(crate) fn options(&self) -> Vec<String> {
        let mut options = vec![
            format!("time={}", crate::stream::encode(&self.column)),
            format!("window={}", written(self.span)),
        ];
        if let Some(unit) = self.unit {
            options.push(format!("unit={}", unit.text()));
        }
        options
    }

    /// What the status bar says of it.
    pub(crate) fn describe(&self) -> String {
        format!("last {} by {}", written(self.span), self.column)
    }

    /// Keeps `table` to the window, if its time column can take it.
    pub(crate) fn apply(&self, table: &mut LiveTable) -> Result<(), String> {
        let schema = table.schema().clone();
        let field = schema
            .field_with_name(&self.column)
            .map_err(|_| format!("the window's column {} is not in the rows", self.column))?;
        let window = match field.data_type() {
            DataType::Timestamp(..)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
                if self.unit.is_none() =>
            {
                Window::duration(&self.column, self.span)
            }
            data_type if data_type.is_numeric() => {
                let unit = self.unit.unwrap_or(DEFAULT_UNIT);
                // From whole nanoseconds, so a span of whole units is whole: 1.001 s is
                // 1,001 ms, not 1,000.9999999999999.
                Window::span(&self.column, self.span.as_nanos() as f64 / unit.nanos())
            }
            _ if self.unit.is_some() => {
                return Err(format!(
                    "unit= is for a column of numbers; {} holds {}",
                    self.column,
                    field.data_type()
                ))
            }
            data_type => {
                return Err(format!(
                    "a window needs times or numbers; {} holds {data_type}",
                    self.column
                ))
            }
        };
        table
            .set_window(Some(window))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
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
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampSecondArray,
    };

    use super::*;

    fn window(options: &[(&str, &str)]) -> Result<Option<TimeWindow>, String> {
        let mut read = WindowOptions::default();
        for (name, value) in options {
            assert!(read.take(name, value)?, "{name} is a window option");
        }
        read.finish()
    }

    fn table(time: ArrayRef) -> LiveTable {
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("time", time),
        ])
        .unwrap();
        let mut table = LiveTable::new(batch.schema(), "id").unwrap();
        table.upsert(batch).unwrap();
        table
    }

    #[test]
    fn spans_are_a_number_and_a_unit() {
        let secs = |text| span(text).map(|d| d.as_secs_f64());
        assert_eq!(secs("500ms"), Ok(0.5));
        assert_eq!(secs("30s"), Ok(30.0));
        assert_eq!(secs("10m"), Ok(600.0));
        assert_eq!(secs("10min"), Ok(600.0));
        assert_eq!(secs("1.5h"), Ok(5_400.0));
        assert_eq!(secs("2d"), Ok(172_800.0));
        assert_eq!(secs("366d"), Ok(MOST.as_secs_f64()));
        let huge = format!("{}d", "9".repeat(400));
        assert!(span(&huge).unwrap_err().contains("longer than a year"));
        for wrong in ["10", "m", "10x", "1.2.3s", "-5m", "", "10 m"] {
            assert!(span(wrong).unwrap_err().contains("such as"), "{wrong}");
        }
        assert!(span("0s").unwrap_err().contains("keeps nothing"));
        assert!(span("367d").unwrap_err().contains("longer than a year"));
    }

    #[test]
    fn the_options_go_together() {
        assert_eq!(window(&[]), Ok(None));
        let given = window(&[("time", "t"), ("window", "10m"), ("unit", "s")])
            .unwrap()
            .unwrap();
        assert_eq!(given.span, Duration::from_secs(600));
        assert_eq!(given.options(), ["time=t", "window=10m", "unit=s"]);
        assert_eq!(given.describe(), "last 10m by t");
        let same = window(&[("time", "t"), ("window", "600s")])
            .unwrap()
            .unwrap();
        assert_eq!(
            same.options(),
            ["time=t", "window=10m"],
            "one span, one way"
        );
        let spaced = window(&[("time", "a b"), ("window", "1h")])
            .unwrap()
            .unwrap();
        assert_eq!(spaced.options(), ["time=a%20b", "window=1h"]);

        assert!(window(&[("window", "1m")])
            .unwrap_err()
            .contains("time=COLUMN"));
        assert!(window(&[("time", "t")]).unwrap_err().contains("window=10m"));
        assert!(window(&[("unit", "s")])
            .unwrap_err()
            .contains("goes with a window"));
        assert!(window(&[("unit", "h")])
            .unwrap_err()
            .contains("s, ms, us or ns"));
        assert_eq!(window(&[("time", "")]), Err("time needs a value".into()));
        for twice in ["time", "unit"] {
            let value = if twice == "unit" { "s" } else { "t" };
            let error = window(&[(twice, value), (twice, value)]).unwrap_err();
            assert_eq!(error, format!("{twice}= is given twice"));
        }
        let error = window(&[("window", "1m"), ("window", "1m")]).unwrap_err();
        assert_eq!(error, "window= is given twice");
        assert_eq!(WindowOptions::default().take("key", "id"), Ok(false));
    }

    #[test]
    fn spans_are_written_in_their_largest_whole_unit() {
        let text = |s| written(span(s).unwrap());
        assert_eq!(text("1.5h"), "90m");
        assert_eq!(text("48h"), "2d");
        assert_eq!(text("0.25s"), "250ms");
        assert_eq!(text("1001ms"), "1001ms");
        assert_eq!(text("0.5ms"), "0.5ms");
        assert_eq!(text("90s"), "90s");
        for given in ["1.5h", "0.5ms", "1d", "1001ms"] {
            assert_eq!(span(&text(given)), span(given), "{given} reads back");
        }
    }

    #[test]
    fn whole_units_stay_whole() {
        let ms = |ids: &[i64], v: &[i64]| {
            let batch = RecordBatch::try_from_iter([
                ("id", Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef),
                ("time", Arc::new(Int64Array::from(v.to_vec())) as ArrayRef),
            ])
            .unwrap();
            let mut table = LiveTable::new(batch.schema(), "id").unwrap();
            table.upsert(batch).unwrap();
            table
        };
        let mut kept = ms(&[1, 2], &[0, 1_001]);
        let window = window(&[("time", "time"), ("window", "1.001s")])
            .unwrap()
            .unwrap();
        window.apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 2, "1,001 ms old: exactly the span");
    }

    #[test]
    fn a_number_column_counts_milliseconds_unless_told() {
        let minute = window(&[("time", "time"), ("window", "1m")])
            .unwrap()
            .unwrap();
        let seconds = window(&[("time", "time"), ("window", "1m"), ("unit", "s")])
            .unwrap()
            .unwrap();
        let ninety = || Arc::new(Float64Array::from(vec![0.0, 90.0]));

        let mut kept = table(ninety());
        minute.apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 2, "90 ms apart");
        let mut kept = table(Arc::new(Float64Array::from(vec![0.0, 90_000.0])));
        minute.apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 1, "90,000 ms apart: the first goes");
        let mut kept = table(ninety());
        seconds.apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 1, "90 s apart: the first goes");
        let micros = Arc::new(Float64Array::from(vec![0.0, 60_000_000.0, 60_000_001.0]));
        let ids = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([("id", ids), ("time", micros as ArrayRef)]);
        let batch = batch.unwrap();
        let mut kept = LiveTable::new(batch.schema(), "id").unwrap();
        kept.upsert(batch).unwrap();
        let unit = window(&[("time", "time"), ("window", "1m"), ("unit", "us")]);
        unit.unwrap().unwrap().apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 2, "a minute and a microsecond: the first goes");
        let nanos = window(&[("time", "time"), ("window", "1m"), ("unit", "ns")]);
        assert_eq!(nanos.unwrap().unwrap().options()[2], "unit=ns");
    }

    #[test]
    fn a_time_column_counts_its_own_units() {
        let mut kept = table(Arc::new(TimestampSecondArray::from(vec![0, 90])));
        let minute = window(&[("time", "time"), ("window", "1m")])
            .unwrap()
            .unwrap();
        minute.apply(&mut kept).unwrap();
        assert_eq!(kept.len(), 1);
        let told = window(&[("time", "time"), ("window", "1m"), ("unit", "s")]);
        let error = told.unwrap().unwrap().apply(&mut kept).unwrap_err();
        assert!(
            error.contains("unit= is for a column of numbers"),
            "{error}"
        );
    }

    #[test]
    fn a_column_that_cannot_hold_times_is_named() {
        let mut text = table(Arc::new(StringArray::from(vec!["a", "b"])));
        let minute = window(&[("time", "time"), ("window", "1m")])
            .unwrap()
            .unwrap();
        let error = minute.apply(&mut text).unwrap_err();
        assert_eq!(error, "a window needs times or numbers; time holds Utf8");
        let elsewhere = window(&[("time", "when"), ("window", "1m")])
            .unwrap()
            .unwrap();
        let error = elsewhere.apply(&mut text).unwrap_err();
        assert_eq!(error, "the window's column when is not in the rows");
        assert!(text.window().is_none());
    }
}
