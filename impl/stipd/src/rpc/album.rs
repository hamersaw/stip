use protobuf::{Album, AlbumBroadcastReply, AlbumBroadcastRequest, AlbumBroadcastType, AlbumCloseReply, AlbumCloseRequest, AlbumCreateReply, AlbumCreateRequest, AlbumDeleteReply, AlbumDeleteRequest, AlbumListReply, AlbumListRequest, AlbumManagement, AlbumManagementClient, AlbumOpenReply, AlbumOpenRequest};
use st_image::prelude::Geocode;
use swarm::prelude::Dht;
use tonic::{Code, Request, Response, Status};

use crate::album::AlbumManager;
use crate::task::{Task, TaskManager};
use crate::task::open::OpenTask;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct AlbumManagementImpl {
    album_manager: Arc<RwLock<AlbumManager>>,
    dht: Arc<RwLock<Dht>>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl AlbumManagementImpl {
    pub fn new(album_manager: Arc<RwLock<AlbumManager>>,
            dht: Arc<RwLock<Dht>>, task_manager: Arc<RwLock<TaskManager>>)
            -> AlbumManagementImpl {
        AlbumManagementImpl {
            album_manager: album_manager,
            dht: dht,
            task_manager: task_manager,
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

                dht_nodes.push((*node_id, addrs.1.unwrap()));
            }
        }

        // send broadcast message to each dht node
        let mut create_replies = HashMap::new();
        let mut close_replies = HashMap::new();
        let mut delete_replies = HashMap::new();
        let mut open_replies = HashMap::new();

        let mut task_id = None;
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            let mut client = match AlbumManagementClient::connect(
                    format!("http://{}", addr)).await {
                Ok(client) => client,
                Err(e) => return Err(Status::new(Code::Unavailable,
                    format!("connection to {} failed: {}", addr, e))),
            };

            // execute message at dht node
            match AlbumBroadcastType::from_i32(request.message_type).unwrap() {
                AlbumBroadcastType::AlbumCreate => {
                    let reply = match client.create(request
                            .create_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("create broadcast failed: {}", e))),
                    };
                    create_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                AlbumBroadcastType::AlbumClose => {
                    let reply = match client.close(request
                            .close_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("close broadcast failed: {}", e))),
                    };
                    close_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                AlbumBroadcastType::AlbumDelete => {
                    let reply = match client.delete(request
                            .delete_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("delete broadcast failed: {}", e))),
                    };
                    delete_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                AlbumBroadcastType::AlbumOpen => {
                    // compile new AlbumOpenRequest
                    let mut open_request =
                        request.open_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        open_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = match client.open(open_request).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("open broadcast failed: {}", e))),
                    };
                    open_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
            };
        }

        // initialize reply
        let reply = AlbumBroadcastReply {
            message_type: request.message_type,
            create_replies: create_replies,
            close_replies: close_replies,
            delete_replies: delete_replies,
            open_replies: open_replies,
        };

        Ok(Response::new(reply))
    }

    async fn close(&self, request: Request<AlbumCloseRequest>)
            -> Result<Response<AlbumCloseReply>, Status> {
        trace!("AlbumCloseRequest: {:?}", request);
        let request = request.get_ref();

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.id)?;

        // close album
        {
            let mut album = album.write().unwrap();
            album.close();
        }

        // initialize reply
        let reply = AlbumCloseReply {};

        Ok(Response::new(reply))
    }

    async fn create(&self, request: Request<AlbumCreateRequest>)
            -> Result<Response<AlbumCreateReply>, Status> {
        trace!("AlbumCreateRequest: {:?}", request);
        let request = request.get_ref();

        // check if album already exists
        let _ = crate::rpc::assert_album_not_exists(
            &self.album_manager, &request.id)?;

        // parse arguments
        let geocode = match protobuf::Geocode
                ::from_i32(request.geocode).unwrap() {
            protobuf::Geocode::Geohash => Geocode::Geohash,
            protobuf::Geocode::Quadtile => Geocode::QuadTile,
        };

        // create album
        {
            let mut album_manager = self.album_manager.write().unwrap();
            if let Err(e) = album_manager.create(
                    request.dht_key_length as i8, geocode, &request.id) {
                return Err(Status::new(Code::Unknown,
                    format!("failed to create album: {}", e)));
            }
        }

        // initialize reply
        let reply = AlbumCreateReply {};

        Ok(Response::new(reply))
    }

    async fn delete(&self, request: Request<AlbumDeleteRequest>)
            -> Result<Response<AlbumDeleteReply>, Status> {
        trace!("AlbumDeleteRequest: {:?}", request);
        let request = request.get_ref();

        // ensure album exists
        let _ = crate::rpc::assert_album_exists(
            &self.album_manager, &request.id)?;

        // delete album
        {
            let mut album_manager = self.album_manager.write().unwrap();
            if let Err(e) = album_manager.delete(&request.id) {
                return Err(Status::new(Code::Unknown,
                    format!("failed to delete album: {}", e)));
            }
        }

        // initialize reply
        let reply = AlbumDeleteReply {};

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
                let geocode = match album.get_geocode() {
                    Geocode::Geohash => protobuf::Geocode::Geohash,
                    Geocode::QuadTile => protobuf::Geocode::Quadtile,
                };

                let status = match album.get_index() {
                    Some(_) => protobuf::AlbumStatus::Open,
                    None => protobuf::AlbumStatus::Closed,
                };

                // add Album protobuf
                albums.push(Album {
                    dht_key_length: album.get_dht_key_length() as i32,
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

    async fn open(&self, request: Request<AlbumOpenRequest>)
            -> Result<Response<AlbumOpenReply>, Status> {
        trace!("AlbumOpenRequest: {:?}", request);
        let request = request.get_ref();

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.id)?;

        // open album
        {
            let mut album = album.write().unwrap();
            if let Err(e) = album.open() {
                return Err(Status::new(Code::Unknown,
                    format!("failed to open album: {}", e)))
            }
        }

        // initialize task
        let task = Arc::new(OpenTask::new(album));

        // start task
        let task_handle = match task.start(request.thread_count as u8) {
            Ok(task_handle) => task_handle,
            Err(e) => return Err(Status::new(Code::Unknown,
                format!("failed to start OpenTask: {}", e))),
        };

        // register task with TaskHandler
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            match task_manager.register(task_handle, request.task_id) {
                Ok(task_id) => task_id,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to register OpenTask: {}", e))),
            }
        };

        // initialize reply
        let reply = AlbumOpenReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }
}
