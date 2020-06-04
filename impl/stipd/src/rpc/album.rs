use protobuf::{Album, AlbumCreateReply, AlbumCreateRequest, AlbumListReply, AlbumListRequest, AlbumManagement};
use tonic::{Request, Response, Status};

use crate::album::{AlbumManager, AlbumStatus, Geocode};

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct AlbumManagementImpl {
    album_manager: Arc<RwLock<AlbumManager>>,
}

impl AlbumManagementImpl {
    pub fn new(album_manager: Arc<RwLock<AlbumManager>>)
            -> AlbumManagementImpl {
        AlbumManagementImpl {
            album_manager: album_manager,
        }
    }
}

#[tonic::async_trait]
impl AlbumManagement for AlbumManagementImpl {
    async fn create(&self, request: Request<AlbumCreateRequest>)
            -> Result<Response<AlbumCreateReply>, Status> {
        trace!("AlbumCreateRequest: {:?}", request);
        let request = request.get_ref();

        let dht_key_length = match request.dht_key_length {
            Some(value) => Some(value as u8),
            None => None,
        };

        let geocode = match protobuf::Geocode
                ::from_i32(request.geocode).unwrap() {
            protobuf::Geocode::Geohash => Geocode::Geohash,
            protobuf::Geocode::Quadtile => Geocode::QuadTile,
        };

        // initialize album - TODO error
        {
            let mut album_manager =
                self.album_manager.write().unwrap();
            album_manager.create(dht_key_length, geocode,
                &request.id).unwrap()
        }

        // initialize reply
        let reply = AlbumCreateReply {};

        Ok(Response::new(reply))
    }

    async fn list(&self, request: Request<AlbumListRequest>)
            -> Result<Response<AlbumListReply>, Status> {
        trace!("AlbumListRequest: {:?}", request);

        // populate albums from AlbumManager
        let mut albums = Vec::new();
        {
            let album_manager = self.album_manager.read().unwrap();
            for (id, album) in album_manager.iter() {
                let (dht_key_length, geocode,
                    status) = album.get_metadata();

                // parse album metadata
                let dht_key_length = match dht_key_length {
                    Some(value) => Some(value as u32),
                    None => None,
                };

                let geocode = match geocode {
                    Geocode::Geohash => protobuf::Geocode::Geohash,
                    Geocode::QuadTile => protobuf::Geocode::Quadtile,
                };

                let status = match status {
                    AlbumStatus::Closed =>
                        protobuf::AlbumStatus::Closed,
                    AlbumStatus::Open => protobuf::AlbumStatus::Open,
                };

                // add Album protobuf
                albums.push(Album {
                    dht_key_length: dht_key_length,
                    geocode: geocode as i32,
                    id: id.to_string(),
                    status: status as i32,
                });
            }
        }

        // initialize reply
        let reply = AlbumListReply {
            albums: albums,
        };

        Ok(Response::new(reply))
    }
}
