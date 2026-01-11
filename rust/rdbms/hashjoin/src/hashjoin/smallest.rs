use std::error::Error;
use std::io::Write;
use crate::query::ast::SelectQuery;
use crate::hashjoin::strategy::JoinStrategy;
use crate::hashjoin::JoinType;

pub struct OnlySmallestJoin;

impl JoinStrategy for OnlySmallestJoin {
    fn execute_to_writer(
        &self,
        query: &SelectQuery,
        _writer: &mut dyn Write,
    ) -> Result<(), Box<dyn Error>> {
        if query.join_type != JoinType::Inner {
            return Err("Стратегия 'only_smallest' поддерживает только 'inner' join".into());
        }
        if query.columns.iter().any(|c| c == "*") {
            return Err("Стратегия 'only_smallest' не поддерживает 'SELECT *'".into());
        }
        Err("Стратегия 'only_smallest' пока не реализована — используйте 'in_memory'".into())
    }
}