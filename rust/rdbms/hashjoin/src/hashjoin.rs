use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;

use csv::{Reader, StringRecord, Writer};

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

// Получает полный список колонок для SELECT *
pub fn get_full_column_list(file1: &str, file2: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let file1_handle = File::open(file1)?;
    let mut rdr1 = Reader::from_reader(file1_handle);
    let headers1: Vec<String> = rdr1.headers()?.iter().map(|s: &str| s.to_string()).collect();

    let file2_handle = File::open(file2)?;
    let mut rdr2 = Reader::from_reader(file2_handle);
    let headers2: Vec<String> = rdr2.headers()?.iter().map(|s: &str| s.to_string()).collect();

    let mut full = headers1;
    full.extend_from_slice(&headers2[1..]); // пропускаем первую колонку второго файла
    Ok(full)
}

fn parse_join_type(s: &str) -> Result<JoinType, String> {
    match s.to_uppercase().as_str() {
        "INNER" => Ok(JoinType::Inner),
        "LEFT" | "LEFT OUTER" => Ok(JoinType::Left),
        "RIGHT" | "RIGHT OUTER" => Ok(JoinType::Right),
        "FULL" | "FULL OUTER" => Ok(JoinType::Full),
        _ => Err(format!("Неизвестный тип соединения: {}", s)),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    InMemory,
    OnlySmallest,
    StreamProcessing,
    MergeJoin,
    DiskBased,
}

impl JoinStrategy {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "in_memory" => Some(JoinStrategy::InMemory),
            "only_smallest" => Some(JoinStrategy::OnlySmallest),
            "stream_processing" => Some(JoinStrategy::StreamProcessing),
            "merge_join" => Some(JoinStrategy::MergeJoin),
            "disk_based" => Some(JoinStrategy::DiskBased),
            _ => None,
        }
    }
}

fn read_csv_to_map<P: AsRef<Path>>(
    path: P,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), Box<dyn Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(file);

    let headers: Vec<String> = rdr.headers()?.iter().map(|s: &str| s.to_string()).collect();
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    for result in rdr.records() {
        let record: StringRecord = result?;
        let key = record.get(0).unwrap_or("").to_string();
        let values: Vec<String> = record.iter().map(|s: &str| s.to_string()).collect();
        map.insert(key, values);
    }

    Ok((headers, map))
}

// === ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: карта имён колонок → индекс в полной строке ===
fn build_column_index(headers1: &[String], headers2: &[String]) -> HashMap<String, usize> {
    let mut map = HashMap::new();
    for (i, col) in headers1.iter().enumerate() {
        map.entry(col.clone()).or_insert(i);
    }
    let offset = headers1.len();
    for (i, col) in headers2.iter().enumerate() {
        if i == 0 { continue; } // пропускаем ключ из второго файла
        map.entry(col.clone()).or_insert(offset + i - 1);
    }
    map
}

// === ОСНОВНАЯ ФУНКЦИЯ С ПОДДЕРЖКОЙ ПРОЕКЦИИ ===
pub fn run_hashjoin_with_projection(
    file1: &str,
    file2: &str,
    join_type: &str,
    output_path: &str,
    strategy: JoinStrategy,
    selected_columns: &[String],
) -> Result<(), Box<dyn Error>> {
    // Проекция пока поддерживается только в in_memory
    if strategy != JoinStrategy::InMemory {
        return Err("Проекция колонок (select ...) поддерживается только со стратегией 'in_memory'".into());
    }

    let jt = parse_join_type(join_type)?;
    let (headers1, map1) = read_csv_to_map(file1)?;
    let (headers2, map2) = read_csv_to_map(file2)?;

    let full_headers: Vec<String> = {
        let mut h = headers1.clone();
        h.extend_from_slice(&headers2[1..]);
        h
    };

    let col_index = build_column_index(&headers1, &headers2);

    // Проверка существования колонок
    for col in selected_columns {
        if !col_index.contains_key(col) {
            return Err(format!("Неизвестная колонка: '{}'", col).into());
        }
    }

    let mut wtr = Writer::from_path(output_path)?;
    wtr.write_record(selected_columns)?;

    let all_keys: Vec<String> = match jt {
        JoinType::Inner => map1.keys().filter(|k| map2.contains_key(*k)).cloned().collect(),
        JoinType::Left => map1.keys().cloned().collect(),
        JoinType::Right => map2.keys().cloned().collect(),
        JoinType::Full => {
            let mut keys: HashSet<String> = map1.keys().cloned().collect();
            keys.extend(map2.keys().cloned());
            keys.into_iter().collect()
        }
    };

    for key in all_keys {
        let row1 = map1.get(&key);
        let row2 = map2.get(&key);

        let full_row: Vec<String> = match jt {
            JoinType::Inner => {
                if let (Some(r1), Some(r2)) = (row1, row2) {
                    let mut out = r1.clone();
                    out.extend_from_slice(&r2[1..]);
                    out
                } else {
                    continue;
                }
            }
            JoinType::Left => {
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
            JoinType::Right => {
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
            JoinType::Full => {
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
                col_index
                    .get(col)
                    .and_then(|&idx| full_row.get(idx).cloned())
                    .unwrap_or_default()
            })
            .collect();

        wtr.write_record(&projected_row)?;
    }

    wtr.flush()?;
    Ok(())
}