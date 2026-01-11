use std::error::Error;
use std::fs::File;
use std::io::Write;

use crate::query::ast::SelectQuery;

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

pub fn parse_join_type(s: &str) -> Result<JoinType, String> {
    match s.to_uppercase().as_str() {
        "INNER" => Ok(JoinType::Inner),
        "LEFT" | "LEFT OUTER" => Ok(JoinType::Left),
        "RIGHT" | "RIGHT OUTER" => Ok(JoinType::Right),
        "FULL" | "FULL OUTER" => Ok(JoinType::Full),
        _ => Err(format!("Неизвестный тип соединения: {}", s)),
    }
}

pub trait JoinStrategy {
    fn execute_to_writer(
        &self,
        query: &SelectQuery,
        writer: &mut dyn Write,
    ) -> Result<(), Box<dyn Error>>;

    fn execute_to_file(
        &self,
        query: &SelectQuery,
        path: &str,
    ) -> Result<(), Box<dyn Error>> {
        let mut file = File::create(path)?;
        self.execute_to_writer(query, &mut file)
    }
}