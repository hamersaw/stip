use clap::ArgMatches;
use protobuf::{ImageFormat, SearchRequest, LoadFormat, LoadRequest, DataManagementClient};
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
    let request = Request::new(SearchRequest {
        geohash: search_matches.value_of("GEOHASH").unwrap().to_string(),
        platform: search_matches.value_of("PLATFORM").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.search(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<80}{:<16}{:<12}{:<8}", "path", "platform", "geohash", "coverage");
    println!("--------------------------------------------------------------------------------------------------------------------");
    for image in reply.images.iter() {
        println!("{:<80}{:<16}{:<12}{:<8}", image.path, image.platform, image.geohash, image.coverage);
    }

    Ok(())
}
