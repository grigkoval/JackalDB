use std::error::Error;
use std::io::{self, Write};
use std::time::Instant;

use tracing::{info, error, debug};

use crate::query::ast::{SelectQuery, OutputTarget};
use crate::hashjoin::strategy::JoinStrategy;
use crate::hashjoin::in_memory::InMemoryJoin;
use crate::hashjoin::smallest::OnlySmallestJoin;

pub fn execute_query(query: &SelectQuery) -> Result<(), Box<dyn Error>> {
    let start_time = Instant::now();
    let _span = tracing::info_span!("execute_query").entered();

    info!("Initializing execution strategy");
    let strategy: Box<dyn JoinStrategy> = match query.strategy_name.to_lowercase().as_str() {
        "in_memory" => Box::new(InMemoryJoin),
        "only_smallest" | "stream_processing" => Box::new(OnlySmallestJoin),
        "merge_join" | "disk_based" => {
            let msg = "This strategy is not implemented yet";
            error!(strategy = %query.strategy_name, "{}", msg);
            return Err(msg.into());
        }
        _ => {
            let msg = format!("Unknown strategy: {}", query.strategy_name);
            error!("{}", msg);
            return Err(msg.into());
        }
    };

    let result = match &query.output {
        OutputTarget::Stdout => {
            debug!("Output target: stdout");
            let mut buffer = Vec::new();
            let join_start = Instant::now();
            let join_result = strategy.execute_to_writer(query, &mut buffer);
            let join_duration = join_start.elapsed();
            info!(duration_ms = %join_duration.as_millis(), "Join completed");

            if join_result.is_ok() {
                io::stdout().write_all(&buffer)?;
                info!("Result written to stdout");
            }
            join_result
        }
        OutputTarget::File(path) => {
            debug!(path = %path, "Output target: file");
            let write_start = Instant::now();
            let write_result = strategy.execute_to_file(query, path);
            let write_duration = write_start.elapsed();
            if write_result.is_ok() {
                info!(path = %path, duration_ms = %write_duration.as_millis(), "Result saved to file");
            }
            write_result
        }
    };

    let total_duration = start_time.elapsed();
    if result.is_ok() {
        info!(total_duration_ms = %total_duration.as_millis(), "Query execution finished successfully");
    } else {
        error!(total_duration_ms = %total_duration.as_millis(), "Query execution failed");
    }

    result
}