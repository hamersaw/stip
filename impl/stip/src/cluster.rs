use clap::ArgMatches;
use protobuf::{NodeListRequest, NodeShowRequest, ClusterManagementClient};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, cluster_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match cluster_matches.subcommand() {
        ("list", Some(list_matches)) => {
            list(&matches, &cluster_matches, &list_matches)
        },
        ("show", Some(show_matches)) => {
            show(&matches, &cluster_matches, &show_matches)
        },
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn list(matches: &ArgMatches, _: &ArgMatches,
        _list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ClusterManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(NodeListRequest {});

    // retrieve reply
    let reply = client.node_list(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<8}{:<20}{:<20}", "id", "rpc_addr", "xfer_addr");
    println!("------------------------------------------------");
    for node in reply.nodes.iter() {
        println!("{:<8}{:<20}{:<20}", node.id,
            node.rpc_addr, node.xfer_addr);
    }

    Ok(())
}

#[tokio::main]
async fn show(matches: &ArgMatches, _: &ArgMatches,
        show_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ClusterManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let node_id = show_matches.value_of("ID").unwrap().parse::<u32>()?;
    let request = Request::new(NodeShowRequest {
        id: node_id,
    });

    // retrieve reply
    let reply = client.node_show(request).await?;
    let reply = reply.get_ref();

    // print information
    match &reply.node {
        Some(node) => {
            println!("node_id: {}", node.id);
            println!("rpc_addr: {}", node.rpc_addr);
            println!("xfer_addr: {}", node.xfer_addr);
        },
        None => println!("node with id '{}' does not exist", node_id),
    }

    Ok(())
}
