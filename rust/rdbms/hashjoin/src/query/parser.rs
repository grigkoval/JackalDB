use regex::Regex;
use std::error::Error;

use crate::query::ast::{SelectQuery, OutputTarget};
use crate::hashjoin::strategy::parse_join_type;

pub fn parse_command(command: &str) -> Result<SelectQuery, Box<dyn Error>> {
    let cmd = command.trim();

    // Регулярка: захватывает всё после ">"
    let re = Regex::new(
        r"(?i)^\s*select\s+(.+?)\s+from\s+([^,]+(?:\s*,\s*[^,]+)*)\s+hashjoin\s+(\w+)(?:\s+(\w+))?(?:\s*>\s*(.*))?\s*$"
    )?;

    let caps = re.captures(cmd).ok_or("Неподдерживаемая команда")?;

    let columns_expr = caps.get(1).unwrap().as_str().trim();
    let files_str = caps.get(2).unwrap().as_str();
    let join_type_str = caps.get(3).unwrap().as_str().trim();
    let strategy_name = caps.get(4)
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "only_smallest".to_string());
    let output_expr = caps.get(5).map(|m| m.as_str().trim());

    let files: Vec<&str> = files_str
        .split(',')
        .map(|s| s.trim())
        .collect();

    if files.len() != 2 {
        return Err("Требуется ровно два CSV-файла".into());
    }

    let columns = if columns_expr == "*" {
        vec!["*".to_string()]
    } else {
        columns_expr
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };

    if columns.is_empty() {
        return Err("Список колонок не может быть пустым".into());
    }

    let join_type = parse_join_type(join_type_str)?;
    let output = match output_expr {
        None => OutputTarget::File("result.csv".to_string()),
        Some("") => OutputTarget::Stdout,
        Some(path) => {
            let p = path.to_string();
            if p.ends_with('/') || p.ends_with('\\') {
                OutputTarget::File(format!("{}result.csv", p))
            } else {
                OutputTarget::File(p)
            }
        }
    };

    Ok(SelectQuery {
        columns,
        file1: files[0].to_string(),
        file2: files[1].to_string(),
        join_type,
        strategy_name,
        output,
    })
}