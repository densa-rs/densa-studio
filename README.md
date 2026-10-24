# densa studio

Open Parquet and CSV files, live WebSocket streams and Arrow Flight services and explore them
at once: each fills a grid of its own as rows come, SQL runs over all of them (again each second, if you like, as
they change), and the result is charted as it comes. An open-source application built on
[densa](https://github.com/densa-rs/densa), the Rust toolkit for applications that show a lot
of live data, and on its [connectors](https://github.com/densa-rs/densa-connectors).

<p align="center">
  <img alt="densa studio: a live stream of 500 sensors in a grid; below it a live SQL query counting the sensors at each temperature, its result, and a chart of it, all updating each second" src="https://raw.githubusercontent.com/densa-rs/densa-studio/main/docs/studio.png" width="760">
</p>

> **Status: 0.1, experimental.** Studio is installed from this repository. It uses densa 0.2
> from crates.io, and densa's connectors from their main branch (pinned in `Cargo.lock`)
> until they are released.

## Install and run

You need Rust 1.94 or later and a GPU with Vulkan, Metal, DirectX 12 or OpenGL.

```sh
cargo install --locked --git https://github.com/densa-rs/densa-studio
densa-studio trips.parquet zones.csv
densa-studio --sample        # a million sample web requests (written to your cache
                             # folder the first time) and a local stream of 500 sensors:
                             # `readings` (each sensor's latest) and `recent` (the last minute),
                             # their `time` column as timestamps, and a local Flight SQL
                             # service's list of tables
densa-studio "wss://example.com/trades#key=id"
densa-studio "grpc+tls://flight.example.com:443#sql=SELECT * FROM trips&header=authorization:Bearer $TOKEN"
```

Or from a clone: `cargo run --release -- trips.parquet`.

## What it does

- **Opens files as they load.** CSV and Parquet files, from the command line, typed into
  the box at the top or dropped on the window, open in tabs of their own; rows appear while the rest is read. Click a
  column title to sort, drag column edges to resize, select rows with the mouse or keyboard
  and copy them as tab-separated text (Ctrl+C) for a spreadsheet.
- **SQL over everything open.** Each file is a table named after it, in lower case with
  anything but letters and digits made `_` (`Trips 2024.parquet` is `trips_2024`; a name SQL
  reserves gets a `_` after it, so `select.csv` is `select_`); the status bar shows each
  name. A `row` column numbers its rows (`row_` if the file has a `row` column). The SQL
  panel runs the query you type with Apache DataFusion and shows the result as it is
  computed; a query sees the files as far as they have loaded. Queries only read.
- **A chart of the result.** The Chart tab beside the SQL panel puts the result's first
  column across (a number, date, timestamp or text) and gives each number column after it a pane
  of its own, up to four, named above it:
  `SELECT date_trunc('minute', time) AS minute, count(*), avg(ms) FROM requests GROUP BY 1`
  charts requests and their mean time per minute (no `ORDER BY` needed). It fills as the
  result comes, in order of the first column, and shows all of it until you zoom (wheel) or
  pan (drag), and again once you zoom out to all of it; a few points are drawn as dots.
  Selecting a row in the result marks it on the chart. A first column of text (or
  true/false) makes a bar chart instead: a bar for each row, in the result's order, named
  under it, so `SELECT service, count(*), avg(ms) FROM requests GROUP BY 1 ORDER BY 2 DESC`
  draws requests and mean time per service. A result it cannot chart says why above it; rows
  without a value in the first column are left out of a line chart.
- **Live streams.** A `ws://` or `wss://` URL, in the box or on the command line, opens a
  WebSocket stream in a tab that keeps up as rows come. JSON messages are read with the
  columns of the first one (numbers as floats, so a fraction that comes later is never cut
  off); after the URL, options go after `#`, where the server never sees them:
  `#key=COLUMN` keeps the latest row of each key (without one, every row is kept, numbered
  in a `row` column, the newest million), `#subscribe=TEXT` sends a message each time the
  connection opens (percent-encoded: `%7B` for `{`, `%23` for `#`), `#arrow` reads Arrow
  IPC instead of JSON, and `#name=TABLE` names its table for SQL, joined with `&`. The status bar shows each stream's connection, rows
  per second and messages skipped; a lost connection is opened again.
- **Times as times.** Streams and Flight sources take `time=COLUMN` after `#`: that column
  becomes UTC timestamps as rows arrive, so the grid shows `2026-10-24 01:23:45.119Z`, SQL
  works on it directly (`date_trunc('minute', time)`) and charts put it on a time axis.
  Numbers are read as milliseconds since 1970, as JSON feeds mostly send them, unless
  `unit=s` (or `ms`, `us`, `ns`) says otherwise; text is read as ISO 8601
  (`2026-10-24T01:23:45Z`, to the millisecond). A column that is not there, or values that
  cannot be read, are named in the status bar; the rows come all the same.
- **Time windows.** With `window=10m` beside `time=COLUMN`, only the rows within ten minutes
  of the newest are kept, older ones leaving as new ones come (`500ms`, `30s`, `10m`, `2h`
  and `1d` are spans too). The window follows the data's own times, not the clock. The
  status bar names it: `(last 10m by time)`.
- **Arrow Flight.** A `grpc://` or `grpc+tls://` URL opens a Flight service's data in a tab
  that fills as it arrives. After `#`, say what to ask for: `sql=QUERY` (a Flight SQL
  server), `path=A/B` (a dataset by its path) or `ticket=T`. `key=COLUMN` keeps the latest
  row of each key (without one, rows are numbered), and `name=TABLE` names its table for
  SQL. A service that cannot be reached is asked again, with waits that grow; the status bar
  says so, and the first query does not wait for it.
- **Browsing a Flight SQL server.** `#tables` lists the server's tables (catalog, schema,
  name and type) with their columns (`id int64, city text, at timestamp`); Enter or a
  double click on one opens all its rows in a tab of its own, with the same headers.
- **Headers, without writing secrets down.** Streams and Flight sources take
  `header=NAME:VALUE` after `#`, any number of times; `$NAME` (or `${NAME}`) in a value is
  read from that environment variable when the source opens, so
  `header=authorization:Bearer $TOKEN` sends a token that is never typed, shown, or kept in
  the saved arrangement (`$$` is a `$`). A user and password in a URL are refused: they go
  in a header. Write `&`, `#` and `%` in option values as `%26`, `%23` and `%25`.
- **Saving a result.** Type a file name in the box beside the query and press Enter: the
  result is written as the grid shows it (in its sort, without the `#` row numbers), as
  Parquet or CSV by the name's extension, while you go on working. A name without a folder
  goes in the folder Studio was started from; the status bar says where the file went. A
  file that is already there is never written over.
- **Live SQL.** The Live button beside the query (or `--live`) runs the last query again each
  second while its sources change: its result is updated in place (the selected row and the
  sort stay) and the chart moves with it.
- **A workspace that stays as you left it.** Tabs and panels can be dragged to other groups
  or split beside them, and the arrangement is restored next time (`--fresh` starts from the
  default one). It is saved in your configuration folder: `%APPDATA%\densa-studio` on
  Windows, `~/Library/Application Support/densa-studio` on macOS, `~/.config/densa-studio`
  elsewhere.

Options: `--query SQL` runs a query once the files have loaded, `--live` runs it again as
sources change, `--light` for the light theme, `--seconds N` closes after N seconds.

## Next

A listed table's columns before opening it; bar charts of text categories and picking the
charted columns without SQL.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in densa studio by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
