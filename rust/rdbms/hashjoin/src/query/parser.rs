use regex::Regex;
use std::error::Error;
use tracing::{info, debug};

use crate::query::ast::{SelectQuery, OutputTarget};
use crate::hashjoin::strategy::parse_join_type;

pub fn parse_command(command: &str) -> Result<SelectQuery, Box<dyn Error>> {
    let _span = tracing::info_span!("parse_command").entered();
    debug!(command = %command, "Parsing command");

    let cmd = command.trim();

    // Надёжная регулярка с именованными группами
    let re = Regex::new(
        r"(?ix)
        ^\s*
        select \s+ (?P<columns>.+?)
        \s+ from \s+ (?P<files>[^,]+(?:\s*,\s*[^,]+)*)
        \s+ hashjoin \s+ (?P<join_type>\w+)
        (?: \s+ (?P<strategy>\w+) )?
        (?: \s* > \s* (?P<output>.*) )?
        \s*$
        "
    )?;

    let caps = re.captures(cmd).ok_or("Unsupported command")?;

    let columns_expr = caps.name("columns").unwrap().as_str().trim();
    let files_str = caps.name("files").unwrap().as_str();
    let join_type_str = caps.name("join_type").unwrap().as_str().trim();
    let strategy_name = caps.name("strategy")
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "only_smallest".to_string());
    let output_expr = caps.name("output").map(|m| m.as_str().trim());

    let files: Vec<&str> = files_str
        .split(',')
        .map(|s| s.trim())
        .collect();

    if files.len() != 2 {
        return Err("Exactly two CSV files are required".into());
    }

    let selected_columns = if columns_expr == "*" {
        vec!["*".to_string()]
    } else {
        columns_expr
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>()
    };

    if selected_columns.is_empty() {
        return Err("Column list cannot be empty".into());
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

    info!(
        columns = ?selected_columns,
        files = ?files,
        join_type = %join_type_str,
        strategy = %strategy_name,
        output = ?output,
        "Query parsed successfully"
    );

    Ok(SelectQuery {
        columns: selected_columns,
        file1: files[0].to_string(),
        file2: files[1].to_string(),
        join_type,
        strategy_name,
        output,
    })
}