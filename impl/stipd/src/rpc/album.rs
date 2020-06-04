use protobuf::{Album, AlbumCreateReply, AlbumCreateRequest, AlbumListReply, AlbumListRequest, AlbumManagement};
use tonic::{Request, Response, Status};

use crate::album::{AlbumManager, AlbumStatus, SpatialHashAlgorithm};

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

        let dht_hash_characters = match request.dht_hash_characters {
            Some(value) => Some(value as u8),
            None => None,
        };

        let spatial_hash_algorithm = match protobuf::SpatialHashAlgorithm
                ::from_i32(request.spatial_hash_algorithm).unwrap() {
            protobuf::SpatialHashAlgorithm::Geohash =>
                SpatialHashAlgorithm::Geohash,
            protobuf::SpatialHashAlgorithm::Quadtile =>
                SpatialHashAlgorithm::QuadTile,
        };

        // initialize album - TODO error
        {
            let mut album_manager =
                self.album_manager.write().unwrap();
            album_manager.create(dht_hash_characters, &request.id,
                spatial_hash_algorithm).unwrap();
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
            let album_manager =
                self.album_manager.read().unwrap();
            for (id, album) in album_manager.iter() {
                let (dht_hash_characters, spatial_hash_algorithm,
                    status) = album.get_metadata();

                let dht_hash_characters = match dht_hash_characters {
                    Some(value) => Some(value as u32),
                    None => None,
                };

                let spatial_hash_algorithm =
                        match spatial_hash_algorithm {
                    SpatialHashAlgorithm::Geohash =>
                        protobuf::SpatialHashAlgorithm::Geohash,
                    SpatialHashAlgorithm::QuadTile =>
                        protobuf::SpatialHashAlgorithm::Quadtile,
                };

                let status = match status {
                    AlbumStatus::Closed =>
                        protobuf::AlbumStatus::Closed,
                    AlbumStatus::Open =>
                        protobuf::AlbumStatus::Open,
                };

                albums.push(Album {
                    id: id.to_string(),
                    dht_hash_characters: dht_hash_characters,
                    spatial_hash_algorithm:
                        spatial_hash_algorithm as i32,
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
