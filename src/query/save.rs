//! Saving a query's result: the rows as the grid shows them (in its sort), the query's own
//! columns (not the row numbers), to a Parquet or CSV file named by its extension. The rows
//! are gathered on the interface's thread (their order, and the batches holding them, shared
//! not copied); the file is written on a thread of its own.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema};
use arrow_select::interleave::interleave;
use densa_data::{LiveTable, View};
use parquet::arrow::ArrowWriter;

use crate::config;

/// Rows written per batch: a long result is written in parts, never copied whole.
const ROWS_PER_BATCH: usize = 65_536;

/// What a file is written as, by its extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Format {
    Parquet,
    Csv,
}

impl Format {
    /// The format `path`'s extension names.
    pub(crate) fn of(path: &Path) -> Result<Self, String> {
        let extension = path
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase());
        match extension.as_deref() {
            Some("parquet") => Ok(Self::Parquet),
            Some("csv") => Ok(Self::Csv),
            _ => Err("name a .parquet or .csv file".to_owned()),
        }
    }
}

/// Rows to write, gathered from a result: ready to be written on another thread.
pub(crate) struct Job {
    batches: Vec<RecordBatch>,
    /// Each row's batch and row within it, in the order to write.
    rows: Vec<(usize, usize)>,
    columns: Vec<usize>,
    schema: Arc<Schema>,
    format: Format,
    /// Where the file goes, made absolute (a relative name is under the current folder).
    pub(crate) path: PathBuf,
}

impl Job {
    /// `view`'s rows of `table`, in the view's order, in `columns` (the query's own, by
    /// name), to be written to `path`, which must not exist yet.
    pub(crate) fn new(
        table: &LiveTable,
        view: View<'_>,
        columns: &Schema,
        path: &Path,
    ) -> Result<Self, String> {
        let format = Format::of(path)?;
        let path = std::path::absolute(path).map_err(|e| e.to_string())?;
        if path.exists() {
            return Err(format!("{} exists: choose another name", path.display()));
        }
        let indices: Vec<usize> = columns
            .fields()
            .iter()
            .map(|field| {
                let (index, _) = table
                    .schema()
                    .column_with_name(field.name())
                    .ok_or_else(|| format!("the result has no column {}", field.name()))?;
                Ok(index)
            })
            .collect::<Result<_, String>>()?;
        // As the table has them: every one nullable.
        let fields: Vec<Field> = indices
            .iter()
            .map(|&i| table.schema().field(i).clone())
            .collect();
        let rows = (0..view.len())
            .filter_map(|i| view.row(i))
            .map(|at| (at.batch, at.row))
            .collect();
        Ok(Self {
            batches: table.batches().to_vec(),
            rows,
            columns: indices,
            schema: Arc::new(Schema::new(fields)),
            format,
            path,
        })
    }

    /// How many rows it writes.
    pub(crate) fn rows(&self) -> usize {
        self.rows.len()
    }

    /// Writes the file, through a file beside it so a failure leaves nothing half written.
    /// Returns how many rows it wrote.
    pub(crate) fn run(self) -> Result<usize, String> {
        let partial = config::partial(&self.path);
        let written = self.write(&partial).and_then(|rows| {
            std::fs::rename(&partial, &self.path)
                .map(|()| rows)
                .map_err(|e| e.to_string())
        });
        if written.is_err() {
            let _ = std::fs::remove_file(&partial);
        }
        written
    }

    fn write(&self, path: &Path) -> Result<usize, String> {
        let error = |e: &dyn std::fmt::Display| e.to_string();
        let file = File::create(path).map_err(|e| error(&e))?;
        let mut writer = match self.format {
            Format::Parquet => Writer::Parquet(
                ArrowWriter::try_new(file, Arc::clone(&self.schema), None)
                    .map_err(|e| error(&e))?,
            ),
            Format::Csv => Writer::Csv(
                arrow_csv::WriterBuilder::new()
                    .with_header(true)
                    .build(file),
            ),
        };
        let mut written = 0;
        let mut put = |batch: RecordBatch| -> Result<(), String> {
            match &mut writer {
                Writer::Parquet(parquet) => parquet.write(&batch).map_err(|e| error(&e))?,
                Writer::Csv(csv) => csv.write(&batch).map_err(|e| error(&e))?,
            }
            written += batch.num_rows();
            Ok(())
        };
        if self.rows.is_empty() {
            // A CSV file gets its header from the first batch, even an empty one.
            put(RecordBatch::new_empty(Arc::clone(&self.schema)))?;
        }
        for rows in self.rows.chunks(ROWS_PER_BATCH) {
            put(self.gather(rows).map_err(|e| error(&e))?)?;
        }
        if let Writer::Parquet(parquet) = writer {
            parquet.close().map_err(|e| error(&e))?;
        }
        Ok(written)
    }

    /// `rows` of the stored batches as one batch of the columns to write.
    fn gather(&self, rows: &[(usize, usize)]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let arrays = self
            .columns
            .iter()
            .map(|&column| {
                let parts: Vec<&dyn Array> = self
                    .batches
                    .iter()
                    .map(|b| b.column(column).as_ref())
                    .collect();
                interleave(&parts, rows)
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
    }
}

/// Writes `view`'s rows of `table` as [`Job`] does, on this thread.
#[cfg(test)]
pub(crate) fn save(
    table: &LiveTable,
    view: View<'_>,
    columns: &Schema,
    path: &Path,
) -> Result<usize, String> {
    Job::new(table, view, columns, path)?.run()
}

enum Writer {
    Parquet(ArrowWriter<File>),
    Csv(arrow_csv::Writer<File>),
}

#[cfg(test)]
mod tests;
