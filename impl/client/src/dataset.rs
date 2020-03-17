use clap::ArgMatches;
use protobuf::{InitDatasetRequest, DataManagementClient};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, dataset_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match dataset_matches.subcommand() {
        ("init", Some(init_matches)) => {
            init(&matches, &dataset_matches, &init_matches)
        },
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn init(matches: &ArgMatches, _: &ArgMatches,
        init_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(InitDatasetRequest {
        id: init_matches.value_of("ID").unwrap().into(),
    });

    // retrieve reply
    let reply = client.init_dataset(request).await?;
    println!("REPLY={:?}", reply);

    Ok(())
}
