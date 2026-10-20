# densa studio

Open Parquet and CSV files and explore them at once: each file fills a grid of its own while
it loads, SQL runs over all of them, and the result is charted as it comes. An open-source application built on
[densa](https://github.com/densa-rs/densa), the Rust toolkit for applications that show a lot
of live data, and on its [connectors](https://github.com/densa-rs/densa-connectors).

<p align="center">
  <img alt="densa studio: a million web requests in a grid, and below it a SQL query grouping them by service and status, with its result" src="https://raw.githubusercontent.com/densa-rs/densa-studio/main/docs/studio.png" width="760">
</p>

> **Status: 0.1, experimental.** Studio is installed from this repository; it uses densa and
> its connectors from their main branches (pinned in `Cargo.lock`) until their next
> releases.

## Install and run

You need Rust 1.94 or later and a GPU with Vulkan, Metal, DirectX 12 or OpenGL.

```sh
cargo install --locked --git https://github.com/densa-rs/densa-studio
densa-studio trips.parquet zones.csv
densa-studio --sample        # a million sample web requests, to look around (written to
                             # your cache folder the first time)
```

Or from a clone: `cargo run --release -- trips.parquet`.

## What it does

- **Opens files as they load.** CSV and Parquet files, from the command line or typed into
  the box at the top, open in tabs of their own; rows appear while the rest is read. Click a
  column title to sort, drag column edges to resize, select rows with the mouse or keyboard
  and copy them as tab-separated text (Ctrl+C) for a spreadsheet.
- **SQL over everything open.** Each file is a table named after it, in lower case with
  anything but letters and digits made `_` (`Trips 2024.parquet` is `trips_2024`; a name SQL
  reserves gets a `_` after it, so `select.csv` is `select_`); the status bar shows each
  name. A `row` column numbers its rows (`row_` if the file has a `row` column). The SQL
  panel runs the query you type with Apache DataFusion and shows the result as it is
  computed; a query sees the files as far as they have loaded. Queries only read.
- **A chart of the result.** The Chart tab beside the SQL panel puts the result's first
  column across (a number, date or time) and gives each number column after it a pane of
  its own, up to four, named in its corner:
  `SELECT date_trunc('minute', time) AS minute, count(*), avg(ms) FROM requests GROUP BY 1`
  charts requests and their mean time per minute. It fills as the result comes, in order of
  the first column, and shows all of it until you zoom (wheel) or pan (drag); a double click
  follows the newest again. Selecting a row in the result marks it on the chart. A result it
  cannot chart (text first) says why above it.
- **A workspace that stays as you left it.** Tabs and panels can be dragged to other groups
  or split beside them, and the arrangement is restored next time (`--fresh` starts from the
  default one). It is saved in your configuration folder: `%APPDATA%\densa-studio` on
  Windows, `~/Library/Application Support/densa-studio` on macOS, `~/.config/densa-studio`
  elsewhere.

Options: `--query SQL` runs a query once the files have loaded, `--light` for the light
theme, `--seconds N` closes after N seconds.

## Next

Live sources (WebSocket streams, Arrow Flight) queried as they change; saving query results;
opening files by dropping them on the window; bar charts of text categories and picking the
charted columns without SQL.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in densa studio by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
