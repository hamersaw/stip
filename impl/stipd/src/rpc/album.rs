use protobuf::{Album, AlbumBroadcastReply, AlbumBroadcastRequest, AlbumBroadcastType, AlbumCloseReply, AlbumCloseRequest, AlbumCreateReply, AlbumCreateRequest, AlbumListReply, AlbumListRequest, AlbumManagement, AlbumManagementClient, AlbumOpenReply, AlbumOpenRequest};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use crate::album::{AlbumManager, AlbumIndex, Geocode};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct AlbumManagementImpl {
    album_manager: Arc<RwLock<AlbumManager>>,
    dht: Arc<RwLock<Dht>>,
}

impl AlbumManagementImpl {
    pub fn new(album_manager: Arc<RwLock<AlbumManager>>,
            dht: Arc<RwLock<Dht>>) -> AlbumManagementImpl {
        AlbumManagementImpl {
            album_manager: album_manager,
            dht: dht,
        }
    }
}

#[tonic::async_trait]
impl AlbumManagement for AlbumManagementImpl {
    async fn broadcast(&self, request: Request<AlbumBroadcastRequest>)
            -> Result<Response<AlbumBroadcastReply>, Status> {
        trace!("AlbumBroadcastRequest: {:?}", request);
        let request = request.get_ref();

        // copy valid dht nodes
        let mut dht_nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // check if rpc address is populated
                if let None = addrs.1 {
                    continue;
                }

                dht_nodes.push((*node_id, addrs.1.unwrap().clone()));
            }
        }

        // send broadcast message to each dht node
        let mut create_replies = HashMap::new();
        let mut close_replies = HashMap::new();
        let mut open_replies = HashMap::new();

        for (node_id, addr) in dht_nodes {
            // initialize grpc client - TODO error
            let mut client = AlbumManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // execute message at dht node
            match AlbumBroadcastType::from_i32(request.message_type).unwrap() {
                AlbumBroadcastType::AlbumCreate => {
                    let reply = client.create(request
                        .create_request.clone().unwrap()).await.unwrap();
                    create_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                AlbumBroadcastType::AlbumClose => {
                    let reply = client.close(request
                        .close_request.clone().unwrap()).await.unwrap();
                    close_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                AlbumBroadcastType::AlbumOpen => {
                    let reply = client.open(request
                        .open_request.clone().unwrap()).await.unwrap();
                    open_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
            };
        }

        // initialize reply
        let reply = AlbumBroadcastReply {
            message_type: request.message_type,
            create_replies: create_replies,
            close_replies: close_replies,
            open_replies: open_replies,
        };

        Ok(Response::new(reply))
    }

    async fn close(&self, request: Request<AlbumCloseRequest>)
            -> Result<Response<AlbumCloseReply>, Status> {
        trace!("AlbumCloseRequest: {:?}", request);
        let request = request.get_ref();

        // TODO - close the album
        /*{
            let mut album_manager =
                self.album_manager.write().unwrap();
            album_manager.create(dht_key_length, geocode,
                &request.id).unwrap()
        }*/

        // initialize reply
        let reply = AlbumCloseReply {};

        Ok(Response::new(reply))
    }

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
                let album = album.read().unwrap();

                // parse album metadata
                let dht_key_length = match album.get_dht_key_length() {
                    Some(value) => Some(value as u32),
                    None => None,
                };

                let geocode = match album.get_geocode() {
                    Geocode::Geohash => protobuf::Geocode::Geohash,
                    Geocode::QuadTile => protobuf::Geocode::Quadtile,
                };

                let (index, status) = match album.get_index() {
                    Some(AlbumIndex::Sqlite) =>
                        (Some(protobuf::AlbumIndex::Sqlite as i32),
                            protobuf::AlbumStatus::Open),
                    None => (None, protobuf::AlbumStatus::Closed),
                };

                // add Album protobuf
                albums.push(Album {
                    dht_key_length: dht_key_length,
                    geocode: geocode as i32,
                    id: id.to_string(),
                    index: index,
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

    async fn open(&self, request: Request<AlbumOpenRequest>)
            -> Result<Response<AlbumOpenReply>, Status> {
        trace!("AlbumOpenRequest: {:?}", request);
        let request = request.get_ref();

        // TODO - open the album
        /*{
            let mut album_manager =
                self.album_manager.write().unwrap();
            album_manager.create(dht_key_length, geocode,
                &request.id).unwrap()
        }*/

        // initialize reply
        let reply = AlbumOpenReply {};

        Ok(Response::new(reply))
    }
}
