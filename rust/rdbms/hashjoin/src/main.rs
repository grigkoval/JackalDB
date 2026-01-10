mod operator;
mod hashjoin;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Использование: {} \"<команда>\"", args[0]);
        eprintln!("Пример: {} \"select from a.csv, b.csv hashjoin inner in_memory\"", args[0]);
        std::process::exit(1);
    }

    let command = &args[1];

    if let Err(e) = operator::execute(command) {
        eprintln!("Ошибка: {}", e);
        std::process::exit(1);
    }
}
