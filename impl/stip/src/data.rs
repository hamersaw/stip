use clap::ArgMatches;
use protobuf::{ClusterManagementClient, DataBroadcastRequest, DataBroadcastType, DataFillRequest, DataListRequest, Extent, Filter, LoadFormat, DataLoadRequest, DataManagementClient, DataSearchRequest, DataSplitRequest, NodeListRequest};
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

    // TODO - fix fill
    /*// initialize DataFillRequest
    let fill_request = DataFillRequest {
        band: crate::string_opt(fill_matches.value_of("band")),
        end_timestamp: crate::i64_opt(
            fill_matches.value_of("end_timestamp"))?,
        geohash: crate::string_opt(fill_matches.value_of("geohash")),
        platform: crate::string_opt(fill_matches.value_of("platform")),
        recurse: fill_matches.is_present("recurse"),
        start_timestamp: crate::i64_opt(
            fill_matches.value_of("start_timestamp"))?,
        task_id: crate::u64_opt(fill_matches.value_of("task_id"))?,
        thread_count: fill_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
        window_seconds: fill_matches.value_of("window_seconds")
            .unwrap().parse::<i64>()?,
    };
 
    // initialize request
    let request = Request::new(DataBroadcastRequest {
        message_type: DataBroadcastType::Fill as i32,
        fill_request: Some(fill_request),
        split_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, fill_reply) in reply.fill_replies.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, fill_reply.task_id);
    }*/

    Ok(())
}

#[tokio::main]
async fn list(matches: &ArgMatches, _: &ArgMatches,
        list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize ClusterManagement grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ClusterManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeListRequest
    let node_list_request = Request::new(NodeListRequest {});

    // retrieve NodeListReply
    let node_list_reply = client.node_list(node_list_request).await?;
    let node_list_reply = node_list_reply.get_ref();

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            list_matches.value_of("end_timestamp"))?,
        geohash: crate::string_opt(list_matches.value_of("geohash")),
        max_cloud_coverage: crate::f64_opt(
            list_matches.value_of("max_cloud_coverage"))?,
        min_pixel_coverage: crate::f64_opt(
            list_matches.value_of("min_pixel_coverage"))?,
        platform: crate::string_opt(list_matches.value_of("platform")),
        recurse: list_matches.is_present("recurse"),
        source: crate::string_opt(list_matches.value_of("source")),
        start_timestamp: crate::i64_opt(
            list_matches.value_of("start_timestamp"))?,
    };

    // initialize DataListRequest
    let request = DataListRequest {
        filter: filter,
    };

    // iterate over each available node
    println!("{:<8}{:<12}{:<10}{:<8}{:<12}{:<16}{:<16}{:<80}",
        "node", "platform", "geohash", "source", "timestamp",
        "pixel_coverage", "cloud_coverage", "path");
    println!("------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    for node in node_list_reply.nodes.iter() {
        // initialize DataManagement grpc client
        let mut client = DataManagementClient::connect(
            format!("http://{}", node.rpc_addr)).await?;

        // iterate over image stream
        let mut stream = client.list(Request::new(request.clone()))
            .await?.into_inner();
        while let Some(image) = stream.message().await? {
            for file in image.files.iter() {
                println!("{:<8}{:<12}{:<10}{:<8}{:<12}{:<16.5}{:<16.5}{:<80}",
                    node.id, image.platform, image.geohash,
                    image.source, image.timestamp, file.pixel_coverage,
                    image.cloud_coverage.unwrap_or(-1.0), file.path);
            }
        }
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
        Some("modis") => LoadFormat::Modis as i32,
        Some("naip") => LoadFormat::Naip as i32,
        Some("sentinel") => LoadFormat::Sentinel as i32,
        _ => unimplemented!(),
    };

    // initialize DataLoadRequest
    let request = Request::new(DataLoadRequest {
        glob: load_matches.value_of("GLOB").unwrap().to_string(),
        load_format: load_format,
        precision: load_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        task_id: crate::u64_opt(load_matches.value_of("task_id"))?,
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
    // initialize ClusterManagement grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ClusterManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeListRequest
    let node_list_request = Request::new(NodeListRequest {});

    // retrieve NodeListReply
    let node_list_reply = client.node_list(node_list_request).await?;
    let node_list_reply = node_list_reply.get_ref();

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            search_matches.value_of("end_timestamp"))?,
        geohash: crate::string_opt(search_matches.value_of("geohash")),
        max_cloud_coverage: crate::f64_opt(
            search_matches.value_of("max_cloud_coverage"))?,
        min_pixel_coverage: crate::f64_opt(
            search_matches.value_of("min_pixel_coverage"))?,
        platform: crate::string_opt(search_matches.value_of("platform")),
        recurse: search_matches.is_present("recurse"),
        source: crate::string_opt(search_matches.value_of("source")),
        start_timestamp: crate::i64_opt(
            search_matches.value_of("start_timestamp"))?,
    };

    // initialize DataSearchRequest
    let request = DataSearchRequest {
        filter: filter,
    };

    // TODO - maintains streams vector
    let mut clients = Vec::new();
    for node in node_list_reply.nodes.iter() {
        // initialize DataManagement grpc client
        let client = DataManagementClient::connect(
            format!("http://{}", node.rpc_addr)).await?;

        clients.push(client);
    }

    let mut replies = Vec::new();
    for client in clients.iter_mut() {
        // iterate over image stream
        let reply = client.search(Request::new(request.clone()));
        replies.push(reply);
    }

    let mut streams: Vec<tonic::codec::Streaming<Extent>> = Vec::new();
    for reply in replies {
        let stream = reply.await?.into_inner();
        streams.push(stream);
    }

    let mut stream_index = streams.len();
    let mut platform_map = BTreeMap::new();
    while streams.len() != 0 {
        stream_index = (stream_index + 1) % streams.len();
        if let Some(extent) = streams[stream_index].message().await? {
            let geohash_map = platform_map.entry(
                extent.platform.clone()).or_insert(BTreeMap::new());

            let source_map = geohash_map.entry(
                extent.geohash.clone()).or_insert(BTreeMap::new());

            let count_map = source_map.entry(
                extent.source.clone()).or_insert(BTreeMap::new());

            let count = count_map.entry(extent.precision)
                .or_insert(0);
            *count += extent.count;
        } else {
            let _ = streams.remove(stream_index);
        }
    }

    // print summarized data
    println!("{:<16}{:<10}{:<12}{:<12}{:<12}", "platform",
        "geohash", "source", "precision", "count");
    println!("--------------------------------------------------------------");
    for (platform, geohash_map) in platform_map.iter() {
        for (geohash, source_map) in geohash_map.iter() {
            for (source, count_map) in source_map.iter() {
                for (precision, count) in count_map.iter() {
                    println!("{:<16}{:<10}{:<12}{:<12}{:<12}",
                        platform, geohash, source, precision, count);
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

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            split_matches.value_of("end_timestamp"))?,
        geohash: crate::string_opt(split_matches.value_of("geohash")),
        max_cloud_coverage: None,
        min_pixel_coverage: None,
        platform: crate::string_opt(split_matches.value_of("platform")),
        recurse: split_matches.is_present("recurse"),
        source: None,
        start_timestamp: crate::i64_opt(
            split_matches.value_of("start_timestamp"))?,
    };

    // initialize DataSplitRequest
    let split_request = DataSplitRequest {
        filter: filter,
        geohash_bound: crate::string_opt(
            split_matches.value_of("geohash_bound")),
        precision: split_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        task_id: crate::u64_opt(split_matches.value_of("task_id"))?,
        thread_count: split_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    };

    // initialize request
    let request = Request::new(DataBroadcastRequest {
        message_type: DataBroadcastType::Split as i32,
        fill_request: None,
        split_request: Some(split_request),
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
