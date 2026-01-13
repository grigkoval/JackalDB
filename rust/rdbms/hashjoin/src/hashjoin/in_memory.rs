use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::io::Write;
use std::time::Instant;

use csv::{Writer, StringRecord};
use tracing::{debug, info};

use crate::query::ast::SelectQuery;
use crate::hashjoin::strategy::JoinStrategy;
use crate::storage::csv_reader::read_csv_to_map;

// Вспомогательная структура для эффективного доступа к данным
struct JoinedRow<'a> {
    left: Option<&'a StringRecord>,
    right: Option<&'a StringRecord>,
    left_headers_len: usize,
}

impl<'a> JoinedRow<'a> {
    fn get(&self, index: usize) -> &str {
        if index < self.left_headers_len {
            self.left
                .and_then(|r| r.get(index))
                .unwrap_or("")
        } else {
            // index >= left_headers_len → колонка из правого файла (без ключа)
            let right_index = index - self.left_headers_len + 1; // +1 потому что пропускаем ключ
            self.right
                .and_then(|r| r.get(right_index))
                .unwrap_or("")
        }
    }
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

        // Собираем полный список заголовков один раз
        let full_headers: Vec<String> = {
            let mut h = headers1.clone();
            h.extend_from_slice(&headers2[1..]);
            h
        };

        // Обработка SELECT *
        let selected_columns = if query.columns.len() == 1 && query.columns[0] == "*" {
            full_headers.clone()
        } else {
            query.columns.clone()
        };

        // Строим карту индексов один раз
        let col_index = {
            let mut map = HashMap::new();
            for (i, col) in headers1.iter().enumerate() {
                map.entry(col.clone()).or_insert(i);
            }
            let offset = headers1.len();
            for (i, col) in headers2.iter().enumerate().skip(1) {
                map.entry(col.clone()).or_insert(offset + i - 1);
            }
            map
        };

        // Проверяем существование колонок (если не *)
        if !(query.columns.len() == 1 && query.columns[0] == "*") {
            for col in &selected_columns {
                if !col_index.contains_key(col) {
                    return Err(format!("Unknown column: '{}'", col).into());
                }
            }
        }

        let mut wtr = Writer::from_writer(writer);
        wtr.write_record(&selected_columns)?;

        // Подготовка ключей
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
        let left_len = headers1.len();

        for key in all_keys {
            let row1 = map1.get(&key);
            let row2 = map2.get(&key);

            // Пропуск неподходящих строк для INNER/LEFT/RIGHT
            match query.join_type {
                crate::hashjoin::JoinType::Inner if row1.is_none() || row2.is_none() => continue,
                crate::hashjoin::JoinType::Left if row1.is_none() => continue,
                crate::hashjoin::JoinType::Right if row2.is_none() => continue,
                _ => {}
            }

            let joined = JoinedRow {
                left: row1,
                right: row2,
                left_headers_len: left_len,
            };

            // Формируем строку без клонирования значений — только ссылки
            let projected_row: Vec<&str> = selected_columns
                .iter()
                .map(|col| {
                    col_index
                        .get(col)
                        .map(|&idx| joined.get(idx))
                        .unwrap_or("")
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