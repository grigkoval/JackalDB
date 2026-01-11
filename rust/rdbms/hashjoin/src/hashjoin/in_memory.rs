use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::time::Instant;

use csv::{Writer};

use tracing::{debug, info};

use crate::query::ast::SelectQuery;
use crate::hashjoin::strategy::JoinStrategy;
use crate::storage::csv_reader::read_csv_to_map;

fn build_column_index(headers1: &[String], headers2: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (i, col) in headers1.iter().enumerate() {
        map.entry(col.clone()).or_insert(i);
    }
    let offset = headers1.len();
    for (i, col) in headers2.iter().enumerate() {
        if i == 0 {
            continue;
        }
        map.entry(col.clone()).or_insert(offset + i - 1);
    }
    map
}

pub struct InMemoryJoin;

impl JoinStrategy for InMemoryJoin {
    fn execute_to_writer(
        &self,
        query: &SelectQuery,
        writer: &mut dyn Write,
    ) -> Result<(), Box<dyn Error>> {
        let _span = tracing::debug_span!("in_memory_join").entered();

        debug!("Reading first CSV file: {}", query.file1);
        let read1_start = Instant::now();
        let (headers1, map1) = read_csv_to_map(&query.file1)?;
        let read1_duration = read1_start.elapsed();
        info!(
            file = %query.file1,
            rows = %map1.len(),
            duration_ms = %read1_duration.as_millis(),
            "First file loaded"
        );

        debug!("Reading second CSV file: {}", query.file2);
        let read2_start = Instant::now();
        let (headers2, map2) = read_csv_to_map(&query.file2)?;
        let read2_duration = read2_start.elapsed();
        info!(
            file = %query.file2,
            rows = %map2.len(),
            duration_ms = %read2_duration.as_millis(),
            "Second file loaded"
        );

        let full_headers: Vec<String> = {
            let mut h = headers1.clone();
            h.extend_from_slice(&headers2[1..]);
            h
        };

        let selected_columns = if query.columns.len() == 1 && query.columns[0] == "*" {
            full_headers.clone()
        } else {
            let col_index = build_column_index(&headers1, &headers2);
            for col in &query.columns {
                if !col_index.contains_key(col) {
                    return Err(format!("Unknown column: '{}'", col).into());
                }
            }
            query.columns.clone()
        };

        let col_index_final = build_column_index(&headers1, &headers2);
        let mut wtr = Writer::from_writer(writer);
        wtr.write_record(&selected_columns)?;

        let all_keys: Vec<String> = match query.join_type {
            crate::hashjoin::JoinType::Inner => {
                map1.keys().filter(|k| map2.contains_key(*k)).cloned().collect()
            }
            crate::hashjoin::JoinType::Left => map1.keys().cloned().collect(),
            crate::hashjoin::JoinType::Right => map2.keys().cloned().collect(),
            crate::hashjoin::JoinType::Full => {
                let mut keys: HashSet<String> = map1.keys().cloned().collect();
                keys.extend(map2.keys().cloned());
                keys.into_iter().collect()
            }
        };

        let join_start = Instant::now();
        let mut output_row_count = 0;

        for key in all_keys {
            let row1 = map1.get(&key);
            let row2 = map2.get(&key);

            let full_row: Vec<String> = match query.join_type {
                crate::hashjoin::JoinType::Inner => {
                    if let (Some(r1), Some(r2)) = (row1, row2) {
                        let mut out = r1.clone();
                        out.extend_from_slice(&r2[1..]);
                        out
                    } else {
                        continue;
                    }
                }
                crate::hashjoin::JoinType::Left => {
                    if let Some(r1) = row1 {
                        let mut out = r1.clone();
                        if let Some(r2) = row2 {
                            out.extend_from_slice(&r2[1..]);
                        } else {
                            out.extend(vec!["".to_string(); headers2.len() - 1]);
                        }
                        out
                    } else {
                        continue;
                    }
                }
                crate::hashjoin::JoinType::Right => {
                    if let Some(r2) = row2 {
                        let mut out = if let Some(r1) = row1 {
                            r1.clone()
                        } else {
                            vec!["".to_string(); headers1.len()]
                        };
                        out.extend_from_slice(&r2[1..]);
                        out
                    } else {
                        continue;
                    }
                }
                crate::hashjoin::JoinType::Full => {
                    let mut out = if let Some(r1) = row1 {
                        r1.clone()
                    } else {
                        vec!["".to_string(); headers1.len()]
                    };
                    if let Some(r2) = row2 {
                        out.extend_from_slice(&r2[1..]);
                    } else {
                        out.extend(vec!["".to_string(); headers2.len() - 1]);
                    }
                    out
                }
            };

            let projected_row: Vec<String> = selected_columns
                .iter()
                .map(|col| {
                    col_index_final
                        .get(col)
                        .and_then(|&idx| full_row.get(idx).cloned())
                        .unwrap_or_default()
                })
                .collect();

            wtr.write_record(&projected_row)?;
            output_row_count += 1;
        }

        wtr.flush()?;
        let join_duration = join_start.elapsed();
        info!(
            output_rows = %output_row_count,
            duration_ms = %join_duration.as_millis(),
            "Join processing completed"
        );

        Ok(())
    }
}