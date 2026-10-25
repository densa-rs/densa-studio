//! The names SQL knows tables by, and numbers written for people.

use std::path::Path;

/// Words SQL reserves: a table named after one is given a `_` after it, so queries can name
/// it without quotes.
const RESERVED: &[&str] = &[
    "all",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "cast",
    "check",
    "column",
    "create",
    "cross",
    "current",
    "default",
    "delete",
    "desc",
    "distinct",
    "drop",
    "else",
    "end",
    "except",
    "exists",
    "false",
    "fetch",
    "filter",
    "for",
    "foreign",
    "from",
    "full",
    "group",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "interval",
    "into",
    "is",
    "join",
    "key",
    "lateral",
    "left",
    "like",
    "limit",
    "natural",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "over",
    "partition",
    "primary",
    "range",
    "references",
    "right",
    "row",
    "rows",
    "select",
    "set",
    "some",
    "table",
    "then",
    "to",
    "true",
    "union",
    "unique",
    "update",
    "user",
    "using",
    "values",
    "when",
    "where",
    "window",
    "with",
];

/// A SQL table name for `path`: its file name without the extension, lower case, with
/// anything but letters, digits and `_` made `_`, a `_` after a word SQL reserves, and a
/// number added if `taken` has it.
pub(crate) fn table_name(path: &Path, taken: &[String]) -> String {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    sql_name(&stem, taken)
}

/// A SQL table name from `text`: lower case, with anything but letters, digits and `_` made
/// `_`, a `_` after a word SQL reserves, and a number added if `taken` has it.
pub(crate) fn sql_name(text: &str, taken: &[String]) -> String {
    let mut base: String = text
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    if base.is_empty() {
        base = "data".to_owned();
    } else if base.starts_with(|c: char| c.is_ascii_digit()) {
        base.insert_str(0, "t_");
    } else if RESERVED.contains(&base.as_str()) {
        base.push('_');
    }
    let mut name = base.clone();
    let mut n = 2;
    while taken.contains(&name) {
        name = format!("{base}_{n}");
        n += 1;
    }
    name
}

/// `n` with a comma between groups of three digits.
pub(crate) fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut text = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            text.push(',');
        }
        text.push(digit);
    }
    text
}

/// A source's tab title: the name SQL knows its table by, then `title` (its file, or its
/// URL's host and what it asks for), so the name stays in view when tabs shrink from the
/// right; `title` alone when it starts with the name already (`trips.parquet`).
pub(crate) fn tab_title(name: &str, title: &str) -> String {
    let says_it = title
        .strip_prefix(name)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'));
    match says_it {
        true => title.to_owned(),
        false => format!("{name} · {title}"),
    }
}
