use std::io::Write;
use std::error::Error;
use std::collections::HashSet;
use std::fs::File;

use polars::prelude::*;
use tracing::{debug, info};

use crate::query::ast::SelectQuery;
use crate::hashjoin::strategy::JoinStrategy;

pub struct PolarsJoin;

impl JoinStrategy for PolarsJoin {
    fn execute_to_writer(
        &self,
        query: &SelectQuery,
        writer: &mut dyn Write,
    ) -> Result<(), Box<dyn Error>> {
        let _span = tracing::debug_span!("polars_join").entered();

        debug!("Loading {} with Polars", query.file1);
        let file1 = File::open(&query.file1)?;
        let df1 = CsvReader::new(file1).finish()?;

        debug!("Loading {} with Polars", query.file2);
        let file2 = File::open(&query.file2)?;
        let df2 = CsvReader::new(file2).finish()?;

        info!(rows1 = df1.height(), cols1 = df1.width(), "First DataFrame loaded");
        info!(rows2 = df2.height(), cols2 = df2.width(), "Second DataFrame loaded");

        let join_key = df1
            .get_column_names()
            .first()
            .ok_or("First CSV has no columns")?
            .to_string();

        let df_empty = DataFrame::default();

        let joined_df = match query.join_type {
            crate::hashjoin::JoinType::Inner => df1.inner_join(&df2, [join_key.as_str()], [join_key.as_str()])?,
            crate::hashjoin::JoinType::Left => df1.left_join(&df2, [join_key.as_str()], [join_key.as_str()])?,
            crate::hashjoin::JoinType::Right => df2.left_join(&df1, [join_key.as_str()], [join_key.as_str()])?,
            crate::hashjoin::JoinType::Full => df_empty,
        };

        let all_col_names: Vec<&str> = joined_df
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();

        let selected_columns: Vec<String> = if query.columns.len() == 1 && query.columns[0] == "*" {
            all_col_names.iter().map(|&s| s.to_string()).collect()
        } else {
            let all_names: HashSet<&str> = all_col_names.iter().copied().collect();
            for col in &query.columns {
                if !all_names.contains(col.as_str()) {
                    return Err(format!("Unknown column: '{}'", col).into());
                }
            }
            query.columns.iter().map(|s| s.to_string()).collect()
        };

        let mut projected_df = if selected_columns.len() != all_col_names.len() {
            joined_df.select(selected_columns)?
        } else {
            joined_df
        };

        let mut csv_writer = CsvWriter::new(writer);
        csv_writer.include_header(true).finish(&mut projected_df)?;

        info!(output_rows = projected_df.height(), "Polars join completed");
        Ok(())
    }
}