//! The command line.

use std::path::PathBuf;

/// What the command line asks for.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Args {
    /// Files to open, in order.
    pub(crate) files: Vec<PathBuf>,
    /// Write the sample requests to a Parquet file and open it.
    pub(crate) sample: bool,
    pub(crate) light: bool,
    /// Start with the default arrangement, not the one saved last time.
    pub(crate) fresh: bool,
    pub(crate) query: Option<String>,
    pub(crate) seconds: Option<u64>,
}

pub(crate) const USAGE: &str = "usage: densa-studio [FILE...] [--sample] [--query SQL] \
                                [--light] [--fresh] [--seconds N]";

/// Reads `args` (without the program's name).
pub(crate) fn parse(args: &[String]) -> Result<Args, String> {
    let mut parsed = Args::default();
    let mut at = 0;
    let value = |at: usize, flag: &str| {
        args.get(at + 1)
            .filter(|v| !v.starts_with("--"))
            .cloned()
            .ok_or_else(|| format!("{flag} needs a value"))
    };
    while at < args.len() {
        match args[at].as_str() {
            "--sample" => parsed.sample = true,
            "--light" => parsed.light = true,
            "--fresh" => parsed.fresh = true,
            "--query" => {
                parsed.query = Some(value(at, "--query")?);
                at += 1;
            }
            "--seconds" => {
                let text = value(at, "--seconds")?;
                parsed.seconds = Some(text.parse().map_err(|_| format!("--seconds {text}?"))?);
                at += 1;
            }
            flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
            path => parsed.files.push(PathBuf::from(path)),
        }
        at += 1;
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(text: &[&str]) -> Result<Args, String> {
        parse(&text.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
    }

    #[test]
    fn files_and_flags_in_any_order() {
        let parsed = args(&["a.parquet", "--light", "b.csv", "--seconds", "5", "--fresh"]).unwrap();
        assert_eq!(
            parsed.files,
            [PathBuf::from("a.parquet"), PathBuf::from("b.csv")]
        );
        assert!(parsed.light && parsed.fresh && !parsed.sample);
        assert_eq!(parsed.seconds, Some(5));
    }

    #[test]
    fn a_query_is_one_argument() {
        let parsed = args(&["--query", "SELECT 1", "--sample"]).unwrap();
        assert_eq!(parsed.query.as_deref(), Some("SELECT 1"));
        assert!(parsed.sample);
    }

    #[test]
    fn mistakes_are_named() {
        assert_eq!(args(&["--nope"]), Err("unknown option --nope".to_owned()));
        assert_eq!(
            args(&["--seconds"]),
            Err("--seconds needs a value".to_owned())
        );
        assert_eq!(
            args(&["--seconds", "--light"]),
            Err("--seconds needs a value".to_owned())
        );
        assert_eq!(args(&["--seconds", "x"]), Err("--seconds x?".to_owned()));
    }
}
