use crate::hashjoin::JoinType;

#[derive(Debug, Clone)]
pub struct SelectQuery {
    pub columns: Vec<String>,
    pub file1: String,
    pub file2: String,
    pub join_type: JoinType,
    pub strategy_name: String,
    pub output: OutputTarget,
}

#[derive(Debug, Clone)]
pub enum OutputTarget {
    Stdout,
    File(String),
}