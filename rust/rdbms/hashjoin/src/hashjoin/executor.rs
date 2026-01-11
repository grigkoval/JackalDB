use std::error::Error;
use std::io::{self, Write};

use crate::query::ast::{SelectQuery, OutputTarget};
use crate::hashjoin::strategy::JoinStrategy;
use crate::hashjoin::in_memory::InMemoryJoin;
use crate::hashjoin::smallest::OnlySmallestJoin;

pub fn execute_query(query: &SelectQuery) -> Result<(), Box<dyn Error>> {
    let strategy: Box<dyn JoinStrategy> = match query.strategy_name.to_lowercase().as_str() {
        "in_memory" => Box::new(InMemoryJoin),
        "only_smallest" | "stream_processing" => Box::new(OnlySmallestJoin),
        "merge_join" | "disk_based" => {
            return Err("Эта стратегия пока не реализована".into());
        }
        _ => return Err(format!("Неизвестная стратегия: {}", query.strategy_name).into()),
    };

    match &query.output {
        OutputTarget::Stdout => {
            let mut buffer = Vec::new();
            strategy.execute_to_writer(query, &mut buffer)?;
            io::stdout().write_all(&buffer)?;
            Ok(())
        }
        OutputTarget::File(path) => {
            strategy.execute_to_file(query, path)?;
            Ok(())
        }
    }
}