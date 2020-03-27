use clap::ArgMatches;
use protobuf::{LoadRequest, DataManagementClient};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, data_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match data_matches.subcommand() {
        ("load", Some(load_matches)) => {
            load(&matches, &data_matches, &load_matches)
        },
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn load(matches: &ArgMatches, _: &ArgMatches,
        load_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // listialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    // TODO - fill out request
    let request = Request::new(LoadRequest {
        channels: Vec::new(), 
        directory: String::from(""),
        file: load_matches.value_of("FILE").unwrap().to_string(),
        satellite: String::from(""),
    });

    // retrieve reply
    let reply = client.load(request).await?;
    println!("REPLY={:?}", reply);

    Ok(())
}
