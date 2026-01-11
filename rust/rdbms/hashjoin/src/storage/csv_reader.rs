use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::path::Path;

use csv::{Reader, StringRecord};

pub fn read_csv_to_map<P: AsRef<Path>>(
    path: P,
) -> Result<(Vec<String>, HashMap<String, Vec<String>>), Box<dyn Error>> {
    let file = File::open(path)?;
    let mut rdr = Reader::from_reader(file);
    let headers: Vec<String> = rdr.headers()?.iter().map(|s: &str| s.to_string()).collect();
    let mut map = HashMap::new();
    for result in rdr.records() {
        let record: StringRecord = result?;
        let key = record.get(0).unwrap_or("").to_string();
        let values: Vec<String> = record.iter().map(|s: &str| s.to_string()).collect();
        map.insert(key, values);
    }
    Ok((headers, map))
}