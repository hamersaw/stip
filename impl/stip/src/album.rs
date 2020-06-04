use clap::ArgMatches;
use protobuf::{AlbumCloseRequest, AlbumCreateRequest, AlbumIndex, AlbumListRequest, AlbumManagementClient, AlbumOpenRequest, AlbumStatus, Geocode};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, album_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match album_matches.subcommand() {
        ("close", Some(close_matches)) =>
            create(&matches, &album_matches, &close_matches),
        ("create", Some(create_matches)) =>
            create(&matches, &album_matches, &create_matches),
        ("list", Some(list_matches)) =>
            list(&matches, &album_matches, &list_matches),
        ("open", Some(open_matches)) =>
            list(&matches, &album_matches, &open_matches),
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
    let request = Request::new(AlbumCloseRequest {
        id: close_matches.value_of("ID").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.close(request).await?;
    let reply = reply.get_ref();

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
    let dht_key_length = 
            match create_matches.value_of("dht_key_length") {
        Some(value) => Some(value.parse::<u32>()?),
        None => None,
    };

    let geocode = match create_matches.value_of("GEOCODE") {
        Some("geohash") => Geocode::Geohash as i32,
        Some("quadtile") => Geocode::Quadtile as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let request = Request::new(AlbumCreateRequest {
        dht_key_length: dht_key_length,
        geocode: geocode,
        id: create_matches.value_of("ID").unwrap().to_string(),
    });

    // retrieve reply
    let reply = client.create(request).await?;
    let reply = reply.get_ref();

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
    println!("{:<24}{:<12}{:<16}{:<8}{:<12}", "id",
        "geocode", "dht_key_length", "status", "index");
    println!("--------------------------------------------------------------------");
    for album in reply.albums.iter() {
        let geocode = match Geocode::from_i32(album.geocode).unwrap() {
            Geocode::Geohash => "geohash",
            Geocode::Quadtile => "quadtile",
        };

        let index = match album.index {
            Some(index) => match AlbumIndex::from_i32(index).unwrap() {
                AlbumIndex::Sqlite => "sqlite",
            },
            None => "none",
        };

        let status = match AlbumStatus::from_i32(album.status).unwrap() {
            AlbumStatus::Closed => "closed",
            AlbumStatus::Open => "open",
        };

        println!("{:<24}{:<12}{:<16}{:<8}{:<12}", album.id, geocode,
            album.dht_key_length.unwrap_or(0), status, index);
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

    // parse arguments
    let index = match open_matches.value_of("INDEX") {
        Some("sqlite") => AlbumIndex::Sqlite as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let request = Request::new(AlbumOpenRequest {
        id: open_matches.value_of("ID").unwrap().to_string(),
        index: index,
    });

    // retrieve reply
    let reply = client.open(request).await?;
    let reply = reply.get_ref();

    Ok(())
}
