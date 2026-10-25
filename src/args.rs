//! The command line.

/// What the command line asks for.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Args {
    /// Files and stream URLs to open, in order.
    pub(crate) sources: Vec<String>,
    /// Write the sample requests to a Parquet file and open it, and a local stream of
    /// sensor readings.
    pub(crate) sample: bool,
    /// Run the query again each second while sources change.
    pub(crate) live: bool,
    pub(crate) light: bool,
    /// Start with the default arrangement, not the one saved last time.
    pub(crate) fresh: bool,
    pub(crate) query: Option<String>,
    pub(crate) seconds: Option<u64>,
}

pub(crate) const USAGE: &str = "usage: densa-studio [FILE, ws:// or grpc:// URL...] [--sample] \
                                [--query SQL] [--live] [--light] [--fresh] [--seconds N]";

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
            "--live" => parsed.live = true,
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
            source => parsed.sources.push(source.to_owned()),
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
        let parsed = args(&[
            "a.parquet",
            "--light",
            "ws://h/p#key=id",
            "--seconds",
            "5",
            "--fresh",
            "--live",
        ])
        .unwrap();
        assert_eq!(parsed.sources, ["a.parquet", "ws://h/p#key=id"]);
        assert!(parsed.light && parsed.fresh && parsed.live && !parsed.sample);
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
