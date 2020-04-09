use clap::ArgMatches;
use protobuf::{DataManagementClient, FillAllRequest, ImageFormat, SearchAllRequest, LoadFormat, LoadRequest, SplitAllRequest};
use tonic::Request;

use std::{error, io};
use std::collections::BTreeMap;

pub fn process(matches: &ArgMatches, data_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match data_matches.subcommand() {
        ("fill", Some(fill_matches)) => {
            fill(&matches, &data_matches, &fill_matches)
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

    // initialize request
    let request = Request::new(FillAllRequest {
        geohash: fill_matches.value_of("geohash").unwrap().to_string(),
        platform: fill_matches.value_of("platform").unwrap().to_string(),
        thread_count: fill_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
        window_seconds: fill_matches.value_of("window_seconds")
            .unwrap().parse::<i64>()?,
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
        dataset: search_matches.value_of("dataset").unwrap().to_string(),
        geohash: search_matches.value_of("geohash").unwrap().to_string(),
        platform: search_matches.value_of("platform").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.search_all(request).await?;
    let reply = reply.get_ref();

    // print information
    if search_matches.is_present("summary") {
        let precision = match search_matches.value_of("geohash") {
            None => 1,
            Some(x) => x.len() + 1,
        };
 
        // compile agglomerate view of data
        let mut platform_map = BTreeMap::new();
        for (_, search_reply) in reply.nodes.iter() {
            for image in search_reply.images.iter() {
                let dataset_map = platform_map.entry(
                    image.platform.clone()).or_insert(BTreeMap::new());

                let geohash_map = dataset_map.entry(
                    image.dataset.clone()).or_insert(BTreeMap::new());

                let geohash = &image.geohash[..precision];
                let count_map = geohash_map.entry(
                    geohash.clone()).or_insert(BTreeMap::new());

                let count = count_map.entry(image.geohash.len())
                    .or_insert(0);
                *count += 1;
            }
        }

        // print summarized data
        println!("{:<16}{:<12}{:<12}{:<12}{:<12}", "platform",
            "dataset", "geohash", "precision", "count");
        println!("----------------------------------------------------------------");
        for (platform, dataset_map) in platform_map.iter() {
            for (dataset, geohash_map) in dataset_map.iter() {
                for (geohash, count_map) in geohash_map.iter() {
                    for (precision, count) in count_map.iter() {
                        //println!("{} {} {} {} {}", platform, dataset,
                        //    geohash, precision, count);
                        println!("{:<16}{:<12}{:<12}{:<12}{:<12}",
                            platform, dataset, geohash,
                            precision, count);
                    }
                }
            }
        }
    } else {
        println!("{:<12}{:<80}{:<16}{:<12}{:<12}{:<8}", "node_id",
            "path", "platform", "dataset", "geohash", "coverage");
        println!("--------------------------------------------------------------------------------------------------------------------------------");
        for (node_id, search_reply) in reply.nodes.iter() {
            for image in search_reply.images.iter() {
                println!("{:<12}{:<80}{:<16}{:<12}{:<12}{:<8}", 
                    node_id, image.path, image.platform, image.dataset,
                    image.geohash, image.coverage);
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

    // initialize request
    let request = Request::new(SplitAllRequest {
        dataset: split_matches.value_of("dataset").unwrap().to_string(),
        geohash: split_matches.value_of("geohash").unwrap().to_string(),
        platform: split_matches.value_of("platform").unwrap().to_string(),
        precision: split_matches.value_of("precision")
            .unwrap().parse::<u32>()?,
        thread_count: split_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    });

    // retrieve reply
    let reply = client.split_all(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, split_reply) in reply.nodes.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, split_reply.task_id);
    }

    Ok(())
}
