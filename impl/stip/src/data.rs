use clap::ArgMatches;
use protobuf::{BroadcastRequest, BroadcastType, DataManagementClient, FillRequest, ListRequest, LoadFormat, LoadRequest, SearchRequest, SplitRequest};
use tonic::Request;

use std::{error, io};
use std::collections::BTreeMap;

pub fn process(matches: &ArgMatches, data_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match data_matches.subcommand() {
        ("fill", Some(fill_matches)) => {
            fill(&matches, &data_matches, &fill_matches)
        },
        ("list", Some(list_matches)) => {
            list(&matches, &data_matches, &list_matches)
        },
        ("load", Some(load_matches)) => {
            load(&matches, &data_matches, &load_matches)
        },
        ("search", Some(search_matches)) => {
            search(&matches, &data_matches, &search_matches)
        },
        ("split", Some(split_matches)) => {
            split(&matches, &data_matches, &split_matches)
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

    // initialize FillRequest
    let fill_request = FillRequest {
        band: fill_matches.value_of("band").unwrap().to_string(),
        geohash: fill_matches.value_of("geohash").unwrap().to_string(),
        platform: fill_matches.value_of("platform").unwrap().to_string(),
        thread_count: fill_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
        window_seconds: fill_matches.value_of("window_seconds")
            .unwrap().parse::<i64>()?,
    };
 
    // initialize request
    let request = Request::new(BroadcastRequest {
        message_type: BroadcastType::Fill as i32,
        fill_request: Some(fill_request),
        list_request: None,
        search_request: None,
        split_request: None,
        task_list_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, fill_reply) in reply.fill_replies.iter() {
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

    // parse load format
    let load_format = match load_matches.value_of("LOAD_FORMAT") {
        Some("landsat") => LoadFormat::Landsat as i32,
        Some("sentinel") => LoadFormat::Sentinel as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let request = Request::new(LoadRequest {
        directory: load_matches.value_of("DIRECTORY").unwrap().to_string(),
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
async fn list(matches: &ArgMatches, _: &ArgMatches,
        list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize ListRequest
    let list_request = ListRequest {
        band: list_matches.value_of("band").unwrap().to_string(),
        dataset: list_matches.value_of("dataset").unwrap().to_string(),
        geohash: list_matches.value_of("geohash").unwrap().to_string(),
        platform: list_matches.value_of("platform").unwrap().to_string(),
    };

    // initialize request
    let request = Request::new(BroadcastRequest {
        message_type: BroadcastType::List as i32,
        fill_request: None,
        list_request: Some(list_request),
        search_request: None,
        split_request: None,
        task_list_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<12}{:<80}{:<16}{:<10}{:<6}{:<12}{:<16}{:<16}",
        "node_id", "path", "platform", "geohash", "band",
        "dataset", "pixel_coverage", "cloud_coverage");
    println!("------------------------------------------------------------------------------------------------------------------------------------------------------------");
    for (node_id, list_reply) in reply.list_replies.iter() {
        for image in list_reply.images.iter() {
            println!("{:<12}{:<80}{:<16}{:<10}{:<6}{:<12}{:<16}{:<16}", 
                node_id, image.path, image.platform,
                image.geohash, image.band, image.dataset,
                image.pixel_coverage, image.cloud_coverage);
        }
    }

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

    // initialize SearchRequest
    let search_request = SearchRequest {
        band: search_matches.value_of("band").unwrap().to_string(),
        dataset: search_matches.value_of("dataset").unwrap().to_string(),
        geohash: search_matches.value_of("geohash").unwrap().to_string(),
        platform: search_matches.value_of("platform").unwrap().to_string(),
    };

    // initialize request
    let request = Request::new(BroadcastRequest {
        message_type: BroadcastType::Search as i32,
        fill_request: None,
        list_request: None,
        search_request: Some(search_request),
        split_request: None,
        task_list_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // compile agglomerate view of data
    let mut platform_map = BTreeMap::new();
    for (_, search_reply) in reply.search_replies.iter() {
        for extent in search_reply.extents.iter() {
            let geohash_map = platform_map.entry(
                extent.platform.clone()).or_insert(BTreeMap::new());

            let band_map = geohash_map.entry(
                extent.geohash.clone()).or_insert(BTreeMap::new());

            let dataset_map = band_map.entry(extent.band.clone())
                .or_insert(BTreeMap::new());

            let count_map = dataset_map.entry(
                extent.dataset.clone()).or_insert(BTreeMap::new());

            let count = count_map.entry(extent.precision)
                .or_insert(0);
            *count += extent.count;
        }
    }

    // print summarized data
    println!("{:<16}{:<10}{:<6}{:<12}{:<12}{:<12}", "platform",
        "geohash", "band", "dataset", "precision", "count");
    println!("--------------------------------------------------------------------");
    for (platform, geohash_map) in platform_map.iter() {
        for (geohash, band_map) in geohash_map.iter() {
            for (band, dataset_map) in band_map.iter() {
                for (dataset, count_map) in dataset_map.iter() {
                    for (precision, count) in count_map.iter() {
                        println!("{:<16}{:<10}{:<6}{:<12}{:<12}{:<12}",
                            platform, geohash, band, dataset,
                            precision, count);
                    }
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn split(matches: &ArgMatches, _: &ArgMatches,
        split_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize SplitRequest
    let split_request = SplitRequest {
        band: split_matches.value_of("band").unwrap().to_string(),
        geohash: split_matches.value_of("geohash").unwrap().to_string(),
        platform: split_matches.value_of("platform").unwrap().to_string(),
        precision: split_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        thread_count: split_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    };

    // initialize request
    let request = Request::new(BroadcastRequest {
        message_type: BroadcastType::Split as i32,
        fill_request: None,
        list_request: None,
        search_request: None,
        split_request: Some(split_request),
        task_list_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, split_reply) in reply.split_replies.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, split_reply.task_id);
    }

    Ok(())
}
