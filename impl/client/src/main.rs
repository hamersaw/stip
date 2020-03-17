#[macro_use]
extern crate clap;
use clap::App;

mod dataset;

fn main() {
    let yaml = load_yaml!("clap.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    // parse subcommands
    match matches.subcommand() {
        ("dataset", Some(dataset_matches)) =>
            dataset::process(&matches, &dataset_matches),
        (cmd, _) => println!("unknown subcommand '{}'", cmd),
    }
}
