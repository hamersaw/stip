use clap::ArgMatches;
use protobuf::{NodeManagementClient, ImageBroadcastRequest, ImageBroadcastType, ImageCoalesceRequest, ImageFillRequest, ImageListRequest, Extent, Filter, ImageFormat, ImageStoreRequest, ImageManagementClient, ImageSearchRequest, ImageSplitRequest, NodeListRequest};
use tonic::Request;

use std::{error, io};
use std::collections::BTreeMap;

pub fn process(matches: &ArgMatches, data_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match data_matches.subcommand() {
        ("coalesce", Some(coalesce_matches)) =>
            coalesce(&matches, &data_matches, &coalesce_matches),
        ("fill", Some(fill_matches)) =>
            fill(&matches, &data_matches, &fill_matches),
        ("list", Some(list_matches)) =>
            list(&matches, &data_matches, &list_matches),
        ("search", Some(search_matches)) =>
            search(&matches, &data_matches, &search_matches),
        ("split", Some(split_matches)) =>
            split(&matches, &data_matches, &split_matches),
        ("store", Some(store_matches)) =>
            store(&matches, &data_matches, &store_matches),
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn coalesce(matches: &ArgMatches, _: &ArgMatches,
        coalesce_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ImageManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            coalesce_matches.value_of("end_timestamp"))?,
        geocode: crate::string_opt(
            coalesce_matches.value_of("geocode")),
        max_cloud_coverage: crate::f64_opt(
            coalesce_matches.value_of("max_cloud_coverage"))?,
        min_pixel_coverage: crate::f64_opt(
            coalesce_matches.value_of("min_pixel_coverage"))?,
        platform: crate::string_opt(
            coalesce_matches.value_of("platform")),
        recurse: coalesce_matches.is_present("recurse"),
        source: crate::string_opt(coalesce_matches.value_of("source")),
        start_timestamp: crate::i64_opt(
            coalesce_matches.value_of("start_timestamp"))?,
    };

    // initialize ImageCoalesceRequest
    let coalesce_request = ImageCoalesceRequest {
        album: coalesce_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
        platform: coalesce_matches.value_of("PLATFORM").unwrap().to_string(),
        task_id: crate::u64_opt(coalesce_matches.value_of("task_id"))?,
        thread_count: coalesce_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
        window_seconds: coalesce_matches.value_of("window_seconds")
            .unwrap().parse::<i64>()?,
    };

    // initialize request
    let request = Request::new(ImageBroadcastRequest {
        message_type: ImageBroadcastType::Coalesce as i32,
        coalesce_request: Some(coalesce_request),
        fill_request: None,
        split_request: None,
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, coalesce_reply) in reply.coalesce_replies.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, coalesce_reply.task_id);
    }

    Ok(())
}

#[tokio::main]
async fn fill(matches: &ArgMatches, _: &ArgMatches,
        fill_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ImageManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            fill_matches.value_of("end_timestamp"))?,
        geocode: crate::string_opt(fill_matches.value_of("geocode")),
        max_cloud_coverage: None,
        min_pixel_coverage: None,
        platform: crate::string_opt(fill_matches.value_of("platform")),
        recurse: fill_matches.is_present("recurse"),
        source: None,
        start_timestamp: crate::i64_opt(
            fill_matches.value_of("start_timestamp"))?,
    };

    // initialize ImageFillRequest
    let fill_request = ImageFillRequest {
        album: fill_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
        task_id: crate::u64_opt(fill_matches.value_of("task_id"))?,
        thread_count: fill_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
        window_seconds: fill_matches.value_of("window_seconds")
            .unwrap().parse::<i64>()?,
    };

    // initialize request
    let request = Request::new(ImageBroadcastRequest {
        message_type: ImageBroadcastType::Fill as i32,
        coalesce_request: None,
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
    }

    Ok(())
}

#[tokio::main]
async fn list(matches: &ArgMatches, _: &ArgMatches,
        list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize NodeManagement grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = NodeManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeListRequest
    let node_list_request = Request::new(NodeListRequest {});

    // retrieve NodeListReply
    let node_list_reply = client.list(node_list_request).await?;
    let node_list_reply = node_list_reply.get_ref();

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            list_matches.value_of("end_timestamp"))?,
        geocode: crate::string_opt(list_matches.value_of("geocode")),
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

    // initialize ImageListRequest
    let request = ImageListRequest {
        album: list_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
    };

    // iterate over each available node
    println!("{:<8}{:<12}{:<10}{:<8}{:<12}{:<16}{:<16}{:<12}{:<80}",
        "node", "platform", "geocode", "source", "timestamp",
        "pixel_coverage", "cloud_coverage", "subdataset", "path");
    println!("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
    for node in node_list_reply.nodes.iter() {
        // initialize ImageManagement grpc client
        let mut client = ImageManagementClient::connect(
            format!("http://{}", node.rpc_addr)).await?;

        // iterate over image stream
        let mut stream = client.list(Request::new(request.clone()))
            .await?.into_inner();
        while let Some(image) = stream.message().await? {
            for file in image.files.iter() {
                println!("{:<8}{:<12}{:<10}{:<8}{:<12}{:<16.5}{:<16.5}{:<12}{:<80}",
                    node.id, image.platform, image.geocode,
                    image.source, image.timestamp, file.pixel_coverage,
                    image.cloud_coverage.unwrap_or(-1.0),
                    file.subdataset, file.path);
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn store(matches: &ArgMatches, _: &ArgMatches,
        store_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = ImageManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // parse load format
    let format = match store_matches.value_of("FORMAT") {
        Some("generic") => ImageFormat::Generic as i32,
        Some("gridmet") => ImageFormat::Gridmet as i32,
        Some("landsat8c1l1") => ImageFormat::Landsat8c1l1 as i32,
        Some("mcd43a4") => ImageFormat::Mcd43a4 as i32,
        Some("mod11a1") => ImageFormat::Mod11a1 as i32,
        Some("mod11a2") => ImageFormat::Mod11a2 as i32,
        Some("naip") => ImageFormat::Naip as i32,
        Some("nlcd") => ImageFormat::Nlcd as i32,
        Some("sentinel2") => ImageFormat::Sentinel2 as i32,
        Some("vnp21v001") => ImageFormat::Vnp21v001 as i32,
        _ => unimplemented!(),
    };

    // initialize ImageStoreRequest
    let request = Request::new(ImageStoreRequest {
        album: store_matches.value_of("ALBUM").unwrap().to_string(),
        format: format,
        glob: store_matches.value_of("GLOB").unwrap().to_string(),
        precision: store_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        task_id: crate::u64_opt(store_matches.value_of("task_id"))?,
        thread_count: store_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    });

    // retrieve reply
    let reply = client.store(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("task starting with id '{}'", reply.task_id);

    Ok(())
}

#[tokio::main]
async fn search(matches: &ArgMatches, _: &ArgMatches,
        search_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize NodeManagement grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = NodeManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize NodeListRequest
    let node_list_request = Request::new(NodeListRequest {});

    // retrieve NodeListReply
    let node_list_reply = client.list(node_list_request).await?;
    let node_list_reply = node_list_reply.get_ref();

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            search_matches.value_of("end_timestamp"))?,
        geocode: crate::string_opt(search_matches.value_of("geocode")),
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

    // initialize ImageSearchRequest
    let request = ImageSearchRequest {
        album: search_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
    };

    // maintain streams vector
    let mut clients = Vec::new();
    for node in node_list_reply.nodes.iter() {
        // initialize ImageManagement grpc client
        let client = ImageManagementClient::connect(
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
            let geocode_map = platform_map.entry(
                extent.platform.clone()).or_insert(BTreeMap::new());

            let source_map = geocode_map.entry(
                extent.geocode.clone()).or_insert(BTreeMap::new());

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
        "geocode", "source", "precision", "count");
    println!("--------------------------------------------------------------");
    for (platform, geocode_map) in platform_map.iter() {
        for (geocode, source_map) in geocode_map.iter() {
            for (source, count_map) in source_map.iter() {
                for (precision, count) in count_map.iter() {
                    println!("{:<16}{:<10}{:<12}{:<12}{:<12}",
                        platform, geocode, source, precision, count);
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
    let mut client = ImageManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize Filter
    let filter = Filter {
        end_timestamp: crate::i64_opt(
            split_matches.value_of("end_timestamp"))?,
        geocode: crate::string_opt(split_matches.value_of("geocode")),
        max_cloud_coverage: None,
        min_pixel_coverage: None,
        platform: crate::string_opt(split_matches.value_of("platform")),
        recurse: split_matches.is_present("recurse"),
        source: None,
        start_timestamp: crate::i64_opt(
            split_matches.value_of("start_timestamp"))?,
    };

    // initialize ImageSplitRequest
    let split_request = ImageSplitRequest {
        album: split_matches.value_of("ALBUM").unwrap().to_string(),
        filter: filter,
        geocode_bound: crate::string_opt(
            split_matches.value_of("geocode_bound")),
        precision: split_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        task_id: crate::u64_opt(split_matches.value_of("task_id"))?,
        thread_count: split_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    };

    // initialize request
    let request = Request::new(ImageBroadcastRequest {
        message_type: ImageBroadcastType::Split as i32,
        coalesce_request: None,
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
