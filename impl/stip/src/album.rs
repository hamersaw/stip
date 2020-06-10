use clap::ArgMatches;
use protobuf::{AlbumBroadcastRequest, AlbumBroadcastType, AlbumCloseRequest, AlbumCreateRequest, AlbumListRequest, AlbumManagementClient, AlbumOpenRequest, AlbumStatus, Geocode};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, album_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match album_matches.subcommand() {
        ("close", Some(close_matches)) =>
            close(&matches, &album_matches, &close_matches),
        ("create", Some(create_matches)) =>
            create(&matches, &album_matches, &create_matches),
        ("list", Some(list_matches)) =>
            list(&matches, &album_matches, &list_matches),
        ("open", Some(open_matches)) =>
            open(&matches, &album_matches, &open_matches),
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn close(matches: &ArgMatches, _: &ArgMatches,
        close_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = AlbumManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let close_request = AlbumCloseRequest {
        id: close_matches.value_of("ID").unwrap().to_string(),
    };

    let request = Request::new(AlbumBroadcastRequest {
        message_type: AlbumBroadcastType::AlbumClose as i32,
        create_request: None,
        close_request: Some(close_request),
        open_request: None,
    });

    // retrieve reply
    let _ = client.broadcast(request).await?;

    Ok(())
}

#[tokio::main]
async fn create(matches: &ArgMatches, _: &ArgMatches,
        create_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = AlbumManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // parse arguments
    let geocode = match create_matches.value_of("GEOCODE") {
        Some("geohash") => Geocode::Geohash as i32,
        Some("quadtile") => Geocode::Quadtile as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let create_request = AlbumCreateRequest {
        dht_key_length: create_matches.value_of("dht_key_length")
            .unwrap().parse::<i32>()?,
        geocode: geocode,
        id: create_matches.value_of("ID").unwrap().to_string(),
    };

    let request = Request::new(AlbumBroadcastRequest {
        message_type: AlbumBroadcastType::AlbumCreate as i32,
        create_request: Some(create_request),
        close_request: None,
        open_request: None,
    });

    // retrieve reply
    let _ = client.broadcast(request).await?;

    Ok(())
}

#[tokio::main]
async fn list(matches: &ArgMatches, _: &ArgMatches,
        _list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = AlbumManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(AlbumListRequest {});

    // retrieve reply
    let reply = client.list(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<24}{:<12}{:<16}{:<8}", "id",
        "geocode", "dht_key_length", "status");
    println!("------------------------------------------------------------");
    for album in reply.albums.iter() {
        let geocode = match Geocode::from_i32(album.geocode).unwrap() {
            Geocode::Geohash => "geohash",
            Geocode::Quadtile => "quadtile",
        };

        let status = match AlbumStatus::from_i32(album.status).unwrap() {
            AlbumStatus::Closed => "closed",
            AlbumStatus::Open => "open",
        };

        println!("{:<24}{:<12}{:<16}{:<8}", album.id, geocode,
            album.dht_key_length, status);
    }

    Ok(())
}

#[tokio::main]
async fn open(matches: &ArgMatches, _: &ArgMatches,
        open_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = AlbumManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let open_request = AlbumOpenRequest {
        id: open_matches.value_of("ID").unwrap().to_string(),
        task_id: crate::u64_opt(open_matches.value_of("task_id"))?,
        thread_count: open_matches.value_of("thread_count")
            .unwrap().parse::<u32>()?,
    };

    let request = Request::new(AlbumBroadcastRequest {
        message_type: AlbumBroadcastType::AlbumOpen as i32,
        create_request: None,
        close_request: None,
        open_request: Some(open_request),
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // print information
    for (node_id, open_reply) in reply.open_replies.iter() {
        println!("task starting on node '{}' with id '{}'",
            node_id, open_reply.task_id);
    }

    Ok(())
}
