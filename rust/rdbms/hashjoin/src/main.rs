mod query;
mod hashjoin;
mod storage;
mod polars; 

use query::parser::parse_command;
use hashjoin::executor::execute_query;
use tracing_subscriber;

fn main() {
    // Инициализация логгера: вывод в stderr с уровнем INFO и выше
    tracing_subscriber::fmt()
        .with_env_filter("hashjoin=info")
        .with_target(false)
        .init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} \"<query>\"", args[0]);
        eprintln!("Examples:");
        eprintln!("  {} \"select * from test/a.csv, test/b.csv hashjoin inner in_memory > \"", args[0]);
        std::process::exit(1);
    }

    let command = &args[1];

    use tracing::info;
    info!(command = %command, "Starting query execution");

    match parse_command(command) {
        Ok(query) => {
            if let Err(e) = execute_query(&query) {
                eprintln!("Execution error: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
            std::process::exit(1);
        }
    }

    info!("Query completed successfully");
}