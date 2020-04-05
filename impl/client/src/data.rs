use clap::ArgMatches;
use protobuf::{FillAllRequest, ImageFormat, SearchAllRequest, LoadFormat, LoadRequest, DataManagementClient};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, data_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match data_matches.subcommand() {
        ("load", Some(load_matches)) => {
            load(&matches, &data_matches, &load_matches)
        },
        ("search", Some(search_matches)) => {
            search(&matches, &data_matches, &search_matches)
        },
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn fill(matches: &ArgMatches, _: &ArgMatches,
        fill_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(FillAllRequest {
        geohash: fill_matches.value_of("geohash").unwrap().to_string(),
        platform: fill_matches.value_of("platform").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.fill_all(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, fill_reply) in reply.nodes.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, fill_reply.task_id);
    }

    Ok(())
}

#[tokio::main]
async fn load(matches: &ArgMatches, _: &ArgMatches,
        load_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // parse formats
    let image_format = match load_matches.value_of("IMAGE_FORMAT") {
        Some("jpeg") => ImageFormat::Jpeg as i32,
        Some("tiff") => ImageFormat::Tiff as i32,
        _ => unimplemented!(),
    };

    let load_format = match load_matches.value_of("LOAD_FORMAT") {
        Some("landsat") => LoadFormat::Landsat as i32,
        Some("sentinel") => LoadFormat::Sentinel as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let request = Request::new(LoadRequest {
        directory: load_matches.value_of("DIRECTORY").unwrap().to_string(),
        file: load_matches.value_of("FILE").unwrap().to_string(),
        image_format: image_format,
        load_format: load_format,
        precision: load_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        thread_count: load_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    });

    // retrieve reply
    let reply = client.load(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("task starting with id '{}'", reply.task_id);

    Ok(())
}

#[tokio::main]
async fn search(matches: &ArgMatches, _: &ArgMatches,
        search_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(SearchAllRequest {
        geohash: search_matches.value_of("geohash").unwrap().to_string(),
        platform: search_matches.value_of("platform").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.search_all(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<12}{:<80}{:<16}{:<12}{:<8}", "node_id",
        "path", "platform", "geohash", "coverage");
    println!("--------------------------------------------------------------------------------------------------------------------------------");
    for (node_id, search_reply) in reply.nodes.iter() {
        for image in search_reply.images.iter() {
            println!("{:<12}{:<80}{:<16}{:<12}{:<8}", node_id,
                image.path, image.platform,
                image.geohash, image.coverage);
        }
    }

    Ok(())
}
