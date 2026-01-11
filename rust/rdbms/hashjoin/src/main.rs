mod query;
mod hashjoin;
mod storage;

use query::parser::parse_command;
use hashjoin::executor::execute_query;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Использование: {} \"<запрос>\"", args[0]);
        eprintln!("Примеры:");
        eprintln!("  {} \"select * from test/a.csv, test/b.csv hashjoin inner in_memory > \"", args[0]);
        eprintln!("  {} \"select id, name from /tmp/x.csv, y.csv hashjoin left in_memory > out.csv\"", args[0]);
        std::process::exit(1);
    }

    let command = &args[1];

    match parse_command(command) {
        Ok(query) => {
            if let Err(e) = execute_query(&query) {
                eprintln!("Ошибка выполнения: {}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Ошибка парсинга: {}", e);
            std::process::exit(1);
        }
    }
}