#[macro_use]
extern crate clap;
use clap::App;

mod cluster;

fn main() {
    let yaml = load_yaml!("clap.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    // parse subcommands
    match matches.subcommand() {
        ("cluster", Some(cluster_matches)) =>
            cluster::process(&matches, &cluster_matches),
        (cmd, _) => println!("unknown subcommand '{}'", cmd),
    }
}
