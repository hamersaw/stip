use clap::ArgMatches;
use protobuf::{AlbumCreateRequest, AlbumListRequest, AlbumManagementClient, AlbumStatus, SpatialHashAlgorithm};
use tonic::Request;

use std::{error, io};

pub fn process(matches: &ArgMatches, album_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match album_matches.subcommand() {
        ("create", Some(create_matches)) =>
            create(&matches, &album_matches, &create_matches),
        ("list", Some(list_matches)) =>
            list(&matches, &album_matches, &list_matches),
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn create(matches: &ArgMatches, _: &ArgMatches,
        create_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = AlbumManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // parse dht hash characters
    let dht_hash_characters = 
            match create_matches.value_of("dht_hash_characters") {
        Some(value) => Some(value.parse::<u32>()?),
        None => None,
    };

    // parse load format
    let spatial_hash_algorithm =
            match create_matches.value_of("SPATIAL_HASH_ALGORITHM") {
        Some("geohash") => SpatialHashAlgorithm::Geohash as i32,
        Some("quadtile") => SpatialHashAlgorithm::Quadtile as i32,
        _ => unimplemented!(),
    };

    // initialize request
    let request = Request::new(AlbumCreateRequest {
        dht_hash_characters: dht_hash_characters,
        id: create_matches.value_of("ID").unwrap().to_string(),
        spatial_hash_algorithm: spatial_hash_algorithm,
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
    println!("{:<24}{:<12}{:<12}{:<8}", "id",
        "algorithm", "dht_chars", "status");
    println!("--------------------------------------------------------");
    for album in reply.albums.iter() {
        let spatial_hash_algorithm = match SpatialHashAlgorithm
                ::from_i32(album.spatial_hash_algorithm).unwrap() {
            SpatialHashAlgorithm::Geohash => "geohash",
            SpatialHashAlgorithm::Quadtile => "quadtile",
        };

        let status = match AlbumStatus::from_i32(album.status).unwrap() {
            AlbumStatus::Closed => "closed",
            AlbumStatus::Open => "open",
        };

        println!("{:<24}{:<12}{:<12}{:<8}",
            album.id, spatial_hash_algorithm,
            album.dht_hash_characters.unwrap_or(0), status);
    }

    Ok(())
}
