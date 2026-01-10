use regex::Regex;
use std::error::Error;
use crate::hashjoin::{JoinStrategy, run_hashjoin_with_projection, get_full_column_list};

pub fn execute(command: &str) -> Result<(), Box<dyn Error>> {
    let cmd = command.trim();

    // Регулярное выражение: захватываем всё после SELECT до FROM
    let re = Regex::new(
        r"(?i)^\s*select\s+(.+?)\s+from\s+([^,\s]+(?:\s*,\s*[^,\s]+)*)\s+hashjoin\s+(\w+)(?:\s+(\w+))?\s*$"
    )?;

    let caps = re.captures(cmd).ok_or("Неподдерживаемая команда")?;

    let columns_expr = caps.get(1).unwrap().as_str().trim();
    let files_str = caps.get(2).unwrap().as_str();
    let join_type = caps.get(3).unwrap().as_str().trim();
    let strategy_str = caps.get(4).map(|m| m.as_str());

    let strategy = if let Some(s) = strategy_str {
        JoinStrategy::from_str(s).ok_or_else(|| format!("Неизвестная стратегия: {}", s))?
    } else {
        JoinStrategy::OnlySmallest
    };

    let files: Vec<&str> = files_str
        .split(',')
        .map(|s| s.trim())
        .collect();

    if files.len() != 2 {
        return Err("Требуется ровно два CSV-файла".into());
    }

    let file1 = files[0];
    let file2 = files[1];

    // Обработка SELECT *
    let selected_columns: Vec<String> = if columns_expr == "*" {
        get_full_column_list(file1, file2)?
    } else {
        columns_expr
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    };

    if selected_columns.is_empty() {
        return Err("Список колонок не может быть пустым".into());
    }

    println!("Проекция: {:?}", selected_columns);
    println!("Файлы: {}, {}", file1, file2);
    println!("Тип соединения: {}, Стратегия: {:?}", join_type, strategy);

    run_hashjoin_with_projection(
        file1,
        file2,
        join_type,
        "result.csv",
        strategy,
        &selected_columns,
    )?;

    println!("Результат сохранён в result.csv");
    Ok(())
}
