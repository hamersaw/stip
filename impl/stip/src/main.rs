#[macro_use]
extern crate clap;
use clap::App;

mod cluster;
mod data;
mod task;

use std::error::Error;

fn main() {
    let yaml = load_yaml!("clap.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    // parse subcommands
    match matches.subcommand() {
        ("cluster", Some(cluster_matches)) =>
            cluster::process(&matches, &cluster_matches),
        ("data", Some(data_matches)) =>
            data::process(&matches, &data_matches),
        ("task", Some(task_matches)) =>
            task::process(&matches, &task_matches),
        (cmd, _) => println!("unknown subcommand '{}'", cmd),
    }
}

fn string_opt(value: Option<&str>) -> Option<String> {
    match value {
        Some(value) => Some(value.to_string()),
        None => None,
    }
}

fn float_opt(value: Option<&str>)
        -> Result<Option<f32>, Box<dyn Error>> {
    match value {
        Some(value) => Ok(Some(value.parse::<f32>()?)),
        None => Ok(None),
    }
}

fn u64_opt(value: Option<&str>) -> Result<Option<u64>, Box<dyn Error>> {
    match value {
        Some(value) => Ok(Some(value.parse::<u64>()?)),
        None => Ok(None),
    }
}
